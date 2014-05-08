# -*- test-case-name: vumi.persist.tests.test_txriak_manager -*-

"""A manager implementation on top of txriak."""

from riakasaurus.riak import RiakClient, RiakObject, RiakMapReduce
from riakasaurus import transport
from twisted.internet.defer import (
    inlineCallbacks, gatherResults, maybeDeferred, succeed, returnValue)

from vumi.persist.model import Manager


class TxRiakManager(Manager):
    """A persistence manager for txriak."""

    call_decorator = staticmethod(inlineCallbacks)

    @classmethod
    def from_config(cls, config):
        config = config.copy()
        bucket_prefix = config.pop('bucket_prefix')
        load_bunch_size = config.pop('load_bunch_size',
                                     cls.DEFAULT_LOAD_BUNCH_SIZE)
        mapreduce_timeout = config.pop('mapreduce_timeout',
                                       cls.DEFAULT_MAPREDUCE_TIMEOUT)
        transport_type = config.pop('transport_type', 'http')
        http_transport_class = transport.HTTPTransport
        if cls.USE_STREAMING_MAPREDUCE:
            http_transport_class = StreamingMapReduceHttpTransport
        transport_class = {
            'http': http_transport_class,
            'protocol_buffer': transport.PBCTransport,
        }.get(transport_type, http_transport_class)

        host = config.get('host', '127.0.0.1')
        port = config.get('port', 8098)
        prefix = config.get('prefix', 'riak')
        mapred_prefix = config.get('mapred_prefix', 'mapred')
        client_id = config.get('client_id')
        # NOTE: the current riakasaurus RiakClient doesn't accept
        #       transport_options or solr_transport_class like the sync
        #       RiakManager client.
        client = RiakClient(host=host, port=port, prefix=prefix,
            mapred_prefix=mapred_prefix, client_id=client_id,
            transport=transport_class)
        return cls(client, bucket_prefix, load_bunch_size=load_bunch_size,
                   mapreduce_timeout=mapreduce_timeout)

    def _encode_indexes(self, iterable, encoding='utf-8'):
        """
        From Basho's docs:

            When using the HTTP interface, multi-valued indexes are specified
            by separating the values with a comma (,). For that reason,
            your application should avoid using a comma as part of an
            index value.

        The index values we get can either be a single string value or can
        be a tuple of multiple values that need to be set. If we get a tuple
        then convert it to a comma separated string.
        """
        encoded = []
        for key, value in iterable:
            if not isinstance(value, (list, tuple)):
                value = [value]

            value = ", ".join([v.encode(encoding) for v in value])
            key = key.encode(encoding)
            encoded.append((key, value))

        return encoded

    def riak_object(self, modelcls, key, result=None):
        bucket = self.bucket_for_modelcls(modelcls)
        riak_object = RiakObject(self.client, bucket, key)
        if result:
            metadata = result['metadata']
            indexes = metadata['index']
            if hasattr(indexes, 'items'):
                # TODO: I think this is a Riak bug. In some cases
                #       (maybe when there are no indexes?) the index
                #       comes back as a list, in others (maybe when
                #       there are indexes?) it comes back as a dict.
                indexes = indexes.items()

            content_type = metadata['content-type'].encode('utf-8')
            indexes = self._encode_indexes(indexes, 'utf-8')
            data = result['data'].encode('utf-8')

            riak_object.set_content_type(content_type)
            riak_object.set_indexes(indexes)
            riak_object.set_encoded_data(data)
        else:
            riak_object.set_data({'$VERSION': modelcls.VERSION})
            riak_object.set_content_type("application/json")
        return riak_object

    def store(self, modelobj):
        d = modelobj._riak_object.store()
        d.addCallback(lambda result: modelobj)
        return d

    def delete(self, modelobj):
        return modelobj._riak_object.delete()

    def load(self, modelcls, key, result=None):
        riak_object = self.riak_object(modelcls, key, result)
        d = succeed(riak_object) if result else riak_object.reload()

        def build_model_object(riak_object):
            if riak_object.get_data() is None:
                return None

            data_version = riak_object.get_data().get('$VERSION', None)
            if data_version == modelcls.VERSION:
                return modelcls(self, key, _riak_object=riak_object)

            migrator = modelcls.MIGRATOR(modelcls, self, data_version)
            md = maybeDeferred(migrator, riak_object)
            md.addCallback(lambda mdata: mdata.get_riak_object())
            return md.addCallback(build_model_object)

        return d.addCallback(build_model_object)

    def _load_multiple(self, modelcls, keys):
        d = gatherResults([self.load(modelcls, key) for key in keys])
        d.addCallback(lambda objs: [obj for obj in objs if obj is not None])
        return d

    def riak_map_reduce(self):
        return RiakMapReduce(self.client)

    def riak_enable_search(self, modelcls):
        bucket_name = self.bucket_name(modelcls)
        bucket = self.client.bucket(bucket_name)
        return bucket.enable_search()

    def run_map_reduce(self, mapreduce, mapper_func=None, reducer_func=None):
        def map_results(raw_results):
            deferreds = []
            for row in raw_results:
                deferreds.append(maybeDeferred(mapper_func, self, row))
            return gatherResults(deferreds)

        mapreduce_done = mapreduce.run(timeout=self.mapreduce_timeout)
        if mapper_func is not None:
            mapreduce_done.addCallback(map_results)
        if reducer_func is not None:
            mapreduce_done.addCallback(lambda r: reducer_func(self, r))
        return mapreduce_done

    def should_quote_index_values(self):
        return not isinstance(self.client, transport.PBCTransport)

    @inlineCallbacks
    def purge_all(self):
        buckets = yield self.client.list_buckets()
        deferreds = []
        for bucket_name in buckets:
            if bucket_name.startswith(self.bucket_prefix):
                bucket = self.client.bucket(bucket_name)
                deferreds.append(bucket.purge_keys())
        yield gatherResults(deferreds)


class StreamingMapReduceHttpTransport(transport.HTTPTransport):

    @inlineCallbacks
    def mapred(self, inputs, query, timeout=None):
        """
        Run a MapReduce query.

        Pilfered from riakasaurus and modified to handle multipart data.
        """
        plm = yield self.phaseless_mapred()
        if not plm and (query is None or len(query) is 0):
            raise Exception('Phase-less MapReduce is not supported '
                            'by this Riak node')

        # Construct the job, optionally set the timeout...
        job = {'inputs': inputs, 'query': query}
        if timeout is not None:
            job['timeout'] = timeout

        content = self.encodeJson(job)

        # Do the request...
        url = "/%s?chunked=true" % (self.client._mapred_prefix,)
        headers = {'Content-Type': 'application/json'}
        response = yield self.http_request('POST', url, headers, content)

        # Make sure the expected status code came back...
        status = response[0]['http_code']
        if status != 200:
            self.raise_mapred_error(response[0], response[1])
        returnValue(self.decode_chunked_response(response[0], response[1]))

    def raise_mapred_error(self, headers, body):
        raise Exception(
            'Error running MapReduce operation. Headers: %s Body: %s' % (
                repr(headers), repr(body)))

    def decode_chunked_response(self, headers, body):
        fake_email = "Content-Type: %s\n\n%s" % (headers['content-type'], body)
        from email import message_from_string
        msg = message_from_string(fake_email)
        if msg.is_multipart():
            return self.decode_chunks(msg.get_payload())

        payload = msg.get_payload()
        if not payload.strip():
            # No content means no results.
            payload = "[]"
        result = self.decodeJson(payload)
        if isinstance(result, dict) and 'error' in result:
            self.raise_mapred_error(headers, body)

        return result

    def decode_chunks(self, chunks):
        phase_results = {}
        for chunk in chunks:
            part = self.decodeJson(chunk.get_payload())
            phase_results.setdefault(part['phase'], []).extend(part['data'])
        # NOTE: We discard all but the last phase received.
        return phase_results[max(phase_results.keys())]
