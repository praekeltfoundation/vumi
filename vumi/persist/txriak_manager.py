# -*- test-case-name: vumi.persist.tests.test_txriak_manager -*-

"""A manager implementation on top of txriak."""

from riakasaurus.riak import RiakClient, RiakObject, RiakMapReduce, RiakLink
import riakasaurus
from twisted.internet.defer import (
    inlineCallbacks, gatherResults, maybeDeferred, succeed)

from vumi.persist.model import Manager


class TxRiakManager(Manager):
    """A persistence manager for txriak."""

    call_decorator = staticmethod(inlineCallbacks)

    @classmethod
    def from_config(cls, config):
        config = config.copy()
        bucket_prefix = config.pop('bucket_prefix')
        client = RiakClient(**config)
        return cls(client, bucket_prefix)

    def riak_object(self, cls, key, result=None):
        bucket_name = self.bucket_name(cls)
        bucket = self.client.bucket(bucket_name)
        riak_object = RiakObject(self.client, bucket, key)
        if result:
            metadata = result['metadata']
            data = result['data']
            riak_object.set_content_type(metadata['content-type'])
            riak_object.set_indexes(metadata['index'].items())
            riak_object.set_encoded_data(data)
        else:
            riak_object.set_data({})
            riak_object.set_content_type("application/json")
        return riak_object

    def store(self, modelobj):
        d = modelobj._riak_object.store()
        d.addCallback(lambda result: modelobj)
        return d

    def delete(self, modelobj):
        return modelobj._riak_object.delete()

    def load(self, cls, key, result=None):
        riak_object = self.riak_object(cls, key, result)
        if result:
            return succeed(cls(self, key, _riak_object=riak_object))
        else:
            d = riak_object.reload()
            d.addCallback(lambda result: cls(self, key, _riak_object=result)
                            if result.get_data() is not None else None)
            return d

    def load_list(self, cls, keys):
        deferreds = []
        for key in keys:
            deferreds.append(self.load(cls, key))
        return gatherResults(deferreds)

    def riak_map_reduce(self):
        return RiakMapReduce(self.client)

    def riak_search(self, cls, query, return_keys=False):
        bucket_name = self.bucket_name(cls)

        def map_result_to_objects(result):
            docs = result['response']['docs']
            keys = [doc['id'] for doc in docs]
            if return_keys:
                return keys
            return self.load_list(cls, keys)

        d = self.client.solr().search(bucket_name, query)
        d.addCallback(map_result_to_objects)
        return d

    def riak_enable_search(self, cls):
        bucket_name = self.bucket_name(cls)
        bucket = self.client.bucket(bucket_name)
        return bucket.enable_search()

    def run_map_reduce(self, mapreduce, mapper_func):
        mapreduce_done = mapreduce.run()

        def map_results(raw_results):
            deferreds = []
            for row in raw_results:
                if riakasaurus.VERSION>="1.0.6":
                    link = row
                else:
                    link = RiakLink(row[0], row[1])

                deferreds.append(maybeDeferred(mapper_func, self, link))
            return gatherResults(deferreds)

        mapreduce_done.addCallback(map_results)
        return mapreduce_done

    @inlineCallbacks
    def purge_all(self):
        buckets = yield self.client.list_buckets()
        deferreds = []
        for bucket_name in buckets:
            if bucket_name.startswith(self.bucket_prefix):
                bucket = self.client.bucket(bucket_name)
                deferreds.append(bucket.purge_keys())
        yield gatherResults(deferreds)
