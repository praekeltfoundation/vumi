# -*- test-case-name: vumi.persist.tests.test_riak_manager -*-

"""A manager implementation on top of the riak Python package."""

from riak import RiakClient, RiakObject, RiakMapReduce

from vumi.persist.model import Manager
from vumi.utils import flatten_generator


class RiakManager(Manager):
    """A persistence manager for the riak Python package."""

    call_decorator = staticmethod(flatten_generator)

    @classmethod
    def from_config(cls, config):
        config = config.copy()
        bucket_prefix = config.pop('bucket_prefix')
        client = RiakClient(**config)
        return cls(client, bucket_prefix)

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
            data = result['data']
            riak_object.set_content_type(metadata['content-type'])
            riak_object.set_indexes(indexes)
            riak_object.set_encoded_data(data)
        else:
            riak_object.set_data({})
            riak_object.set_content_type("application/json")
        return riak_object

    def store(self, modelobj):
        modelobj._riak_object.store()
        return modelobj

    def delete(self, modelobj):
        modelobj._riak_object.delete()

    def load(self, modelcls, key, result=None):
        riak_object = self.riak_object(modelcls, key, result)
        if not result:
            riak_object.reload()
        return (modelcls(self, key, _riak_object=riak_object)
                if riak_object.get_data() is not None else None)

    def riak_map_reduce(self):
        return RiakMapReduce(self.client)

    def run_map_reduce(self, mapreduce, mapper_func=None, reducer_func=None):
        results = mapreduce.run()
        if mapper_func is not None:
            results = [mapper_func(self, row) for row in results]
        if reducer_func is not None:
            results = reducer_func(self, results)
        return results

    def riak_enable_search(self, modelcls):
        bucket_name = self.bucket_name(modelcls)
        bucket = self.client.bucket(bucket_name)
        return bucket.enable_search()

    def purge_all(self):
        buckets = self.client.get_buckets()
        for bucket_name in buckets:
            if bucket_name.startswith(self.bucket_prefix):
                bucket = self.client.bucket(bucket_name)
                for key in bucket.get_keys():
                    obj = bucket.get(key)
                    obj.delete()
