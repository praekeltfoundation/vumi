# -*- test-case-name: vumi.persist.tests.test_txriak_manager -*-

"""An async manager implementation on top of the riak Python package."""

import json

from riak import RiakClient, RiakObject, RiakMapReduce, RiakError
from twisted.internet.threads import deferToThread
from twisted.internet.defer import (
    inlineCallbacks, returnValue, gatherResults, maybeDeferred, succeed)

from vumi.persist.model import Manager, VumiRiakError


def to_unicode(text, encoding='utf-8'):
    if text is None:
        return text
    if isinstance(text, tuple):
        return tuple(to_unicode(item, encoding) for item in text)
    if not isinstance(text, unicode):
        return text.decode(encoding)
    return text


def riakErrorHandler(failure):
    e = failure.trap(RiakError)
    raise VumiRiakError(e)


class VumiTxIndexPage(object):
    """
    Wrapper around a page of index query results.

    Iterating over this object will return the results for the current page.
    """

    def __init__(self, index_page):
        self._index_page = index_page

    def __iter__(self):
        if self._index_page.stream:
            raise NotImplementedError("Streaming is not currently supported.")
        return (to_unicode(item) for item in self._index_page)

    def __eq__(self, other):
        return self._index_page.__eq__(other)

    def has_next_page(self):
        """
        Indicate whether there are more results to follow.

        :returns:
            ``True`` if there are more results, ``False`` if this is the last
            page.
        """
        return self._index_page.has_next_page()

    @property
    def continuation(self):
        return to_unicode(self._index_page.continuation)

    # Methods that touch the network.

    def next_page(self):
        """
        Fetch the next page of results.

        :returns:
            A new :class:`VumiTxIndexPage` object containing the next page of
            results.
        """
        if not self.has_next_page():
            return succeed(None)
        d = deferToThread(self._index_page.next_page)
        d.addCallback(type(self))
        d.addErrback(riakErrorHandler)
        return d


class VumiTxRiakBucket(object):
    def __init__(self, riak_bucket):
        self._riak_bucket = riak_bucket

    def get_name(self):
        return self._riak_bucket.name

    # Methods that touch the network.

    def get_index(self, index_name, start_value, end_value=None,
                  return_terms=None):
        d = self.get_index_page(
            index_name, start_value, end_value, return_terms=return_terms)
        d.addCallback(list)
        return d

    def get_index_page(self, index_name, start_value, end_value=None,
                       return_terms=None, max_results=None, continuation=None):
        d = deferToThread(
            self._riak_bucket.get_index, index_name, start_value, end_value,
            return_terms=return_terms, max_results=max_results,
            continuation=continuation)
        d.addCallback(VumiTxIndexPage)
        d.addErrback(riakErrorHandler)
        return d


class VumiTxRiakObject(object):
    def __init__(self, riak_obj):
        self._riak_obj = riak_obj

    @property
    def key(self):
        return self._riak_obj.key

    def get_key(self):
        return self.key

    def get_content_type(self):
        return self._riak_obj.content_type

    def set_content_type(self, content_type):
        self._riak_obj.content_type = content_type

    def get_data(self):
        return self._riak_obj.data

    def set_data(self, data):
        self._riak_obj.data = data

    def set_encoded_data(self, encoded_data):
        self._riak_obj.encoded_data = encoded_data

    def set_data_field(self, key, value):
        self._riak_obj.data[key] = value

    def delete_data_field(self, key):
        del self._riak_obj.data[key]

    def get_indexes(self):
        return self._riak_obj.indexes

    def set_indexes(self, indexes):
        self._riak_obj.indexes = indexes

    def add_index(self, index_name, index_value):
        self._riak_obj.add_index(index_name, index_value)

    def remove_index(self, index_name, index_value=None):
        self._riak_obj.remove_index(index_name, index_value)

    def get_user_metadata(self):
        return self._riak_obj.usermeta

    def set_user_metadata(self, usermeta):
        self._riak_obj.usermeta = usermeta

    def get_bucket(self):
        return VumiTxRiakBucket(self._riak_obj.bucket)

    # Methods that touch the network.

    def store(self):
        d = deferToThread(self._riak_obj.store)
        d.addCallback(type(self))
        return d

    def reload(self):
        d = deferToThread(self._riak_obj.reload)
        d.addCallback(type(self))
        return d

    def delete(self):
        d = deferToThread(self._riak_obj.delete)
        d.addCallback(type(self))
        return d


class TxRiakManager(Manager):
    """An async persistence manager for the riak Python package."""

    call_decorator = staticmethod(inlineCallbacks)

    @classmethod
    def from_config(cls, config):
        config = config.copy()
        bucket_prefix = config.pop('bucket_prefix')
        load_bunch_size = config.pop(
            'load_bunch_size', cls.DEFAULT_LOAD_BUNCH_SIZE)
        mapreduce_timeout = config.pop(
            'mapreduce_timeout', cls.DEFAULT_MAPREDUCE_TIMEOUT)
        transport_type = config.pop('transport_type', 'http')
        store_versions = config.pop('store_versions', None)

        host = config.get('host', '127.0.0.1')
        port = config.get('port')
        prefix = config.get('prefix', 'riak')
        mapred_prefix = config.get('mapred_prefix', 'mapred')
        client_id = config.get('client_id')
        transport_options = config.get('transport_options', {})

        client_args = dict(
            host=host, prefix=prefix, mapred_prefix=mapred_prefix,
            protocol=transport_type, client_id=client_id,
            transport_options=transport_options)

        if port is not None:
            client_args['port'] = port

        client = RiakClient(**client_args)
        # Some versions of the riak client library use simplejson by
        # preference, which breaks some of our unicode assumptions. This makes
        # sure we're using stdlib json which doesn't sometimes return
        # bytestrings instead of unicode.
        client.set_encoder('application/json', json.dumps)
        client.set_encoder('text/json', json.dumps)
        client.set_decoder('application/json', json.loads)
        client.set_decoder('text/json', json.loads)
        return cls(
            client, bucket_prefix, load_bunch_size=load_bunch_size,
            mapreduce_timeout=mapreduce_timeout, store_versions=store_versions)

    def close_manager(self):
        return deferToThread(self.client.close)

    def riak_bucket(self, bucket_name):
        bucket = self.client.bucket(bucket_name)
        if bucket is not None:
            bucket = VumiTxRiakBucket(bucket)
        return bucket

    def riak_object(self, modelcls, key, result=None):
        bucket = self.bucket_for_modelcls(modelcls)._riak_bucket
        riak_object = VumiTxRiakObject(RiakObject(self.client, bucket, key))
        if result:
            metadata = result['metadata']
            indexes = metadata['index']
            if hasattr(indexes, 'items'):
                # TODO: I think this is a Riak bug. In some cases
                #       (maybe when there are no indexes?) the index
                #       comes back as a list, in others (maybe when
                #       there are indexes?) it comes back as a dict.
                indexes = indexes.items()
            data = result['data']
            riak_object.set_content_type(metadata['content-type'])
            riak_object.set_indexes(indexes)
            riak_object.set_encoded_data(data)
        else:
            riak_object.set_content_type("application/json")
            riak_object.set_data({'$VERSION': modelcls.VERSION})
        return riak_object

    def store(self, modelobj):
        riak_object = modelobj._riak_object
        modelcls = type(modelobj)
        model_name = "%s.%s" % (modelcls.__module__, modelcls.__name__)
        store_version = self.store_versions.get(model_name, modelcls.VERSION)
        # Run reverse migrators until we have the correct version of the data.
        data_version = riak_object.get_data().get('$VERSION', None)
        while data_version != store_version:
            migrator = modelcls.MIGRATOR(
                modelcls, self, data_version, reverse=True)
            riak_object = migrator(riak_object).get_riak_object()
            data_version = riak_object.get_data().get('$VERSION', None)
        d = riak_object.store()
        d.addCallback(lambda _: modelobj)
        return d

    def delete(self, modelobj):
        d = modelobj._riak_object.delete()
        d.addCallback(lambda _: None)
        return d

    @inlineCallbacks
    def load(self, modelcls, key, result=None):
        riak_object = self.riak_object(modelcls, key, result)
        if not result:
            yield riak_object.reload()
        was_migrated = False

        # Run migrators until we have the correct version of the data.
        while riak_object.get_data() is not None:
            data_version = riak_object.get_data().get('$VERSION', None)
            if data_version == modelcls.VERSION:
                obj = modelcls(self, key, _riak_object=riak_object)
                obj.was_migrated = was_migrated
                returnValue(obj)
            migrator = modelcls.MIGRATOR(modelcls, self, data_version)
            riak_object = migrator(riak_object).get_riak_object()
            was_migrated = True
        returnValue(None)

    def _load_multiple(self, modelcls, keys):
        d = gatherResults([self.load(modelcls, key) for key in keys])
        d.addCallback(lambda objs: [obj for obj in objs if obj is not None])
        return d

    def riak_map_reduce(self):
        mapreduce = RiakMapReduce(self.client)
        # Hack: We replace the two methods that hit the network with
        #       deferToThread wrappers to prevent accidental sync calls in
        #       other code.
        run = mapreduce.run
        stream = mapreduce.stream
        mapreduce.run = lambda *a, **kw: deferToThread(run, *a, **kw)
        mapreduce.stream = lambda *a, **kw: deferToThread(stream, *a, **kw)
        return mapreduce

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

    def _search_iteration(self, bucket, query, rows, start):
        d = deferToThread(bucket.search, query, rows=rows, start=start)
        d.addCallback(lambda r: [doc["id"] for doc in r["docs"]])
        return d

    @inlineCallbacks
    def real_search(self, modelcls, query, rows=None, start=None):
        rows = 1000 if rows is None else rows
        bucket_name = self.bucket_name(modelcls)
        bucket = self.client.bucket(bucket_name)
        if start is not None:
            keys = yield self._search_iteration(bucket, query, rows, start)
            returnValue(keys)
        keys = []
        new_keys = yield self._search_iteration(bucket, query, rows, 0)
        while new_keys:
            keys.extend(new_keys)
            new_keys = yield self._search_iteration(
                bucket, query, rows, len(keys))
        returnValue(keys)

    def riak_enable_search(self, modelcls):
        bucket_name = self.bucket_name(modelcls)
        bucket = self.client.bucket(bucket_name)
        return deferToThread(bucket.enable_search)

    def riak_search_enabled(self, modelcls):
        bucket_name = self.bucket_name(modelcls)
        bucket = self.client.bucket(bucket_name)
        return deferToThread(bucket.search_enabled)

    def should_quote_index_values(self):
        return False

    @inlineCallbacks
    def purge_all(self):
        def delete_obj(bucket, key):
            # These are sync operations
            obj = bucket.get(key)
            obj.delete()

        def purge_bucket(bucket):
            key_deletes = []
            for key in bucket.get_keys():
                key_deletes.append(deferToThread(delete_obj, bucket, key))
            d = gatherResults(key_deletes)
            d.addCallback(lambda _: deferToThread(bucket.clear_properties))
            return d

        bucket_deletes = []
        buckets = yield deferToThread(self.client.get_buckets)
        for bucket in buckets:
            if bucket.name.startswith(self.bucket_prefix):
                bucket_deletes.append(purge_bucket(bucket))
        yield gatherResults(bucket_deletes)
