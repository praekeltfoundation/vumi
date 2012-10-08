# -*- test-case-name: vumi.persist.tests.test_riak_manager -*-

"""A manager implementation on top of the riak Python package."""

from riak import RiakClient, RiakObject, RiakMapReduce

from vumi.persist.model import Manager
from vumi.utils import flatten_generator


class RiakManager(Manager):
    """A persistence manager for the riak Python package."""

    call_decorator = staticmethod(flatten_generator)
    # Since this is a synchronous manager we want to fetch objects
    # as part of the mapreduce call. Async managers might prefer
    # to request the objects in parallel as this could be more efficient.
    fetch_objects = True

    @classmethod
    def from_config(cls, config):
        config = config.copy()
        bucket_prefix = config.pop('bucket_prefix')
        client = RiakClient(**config)
        return cls(client, bucket_prefix)

    def riak_object(self, cls, key, result=None):
        bucket = self.bucket_for_cls(cls)
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

    def load(self, cls, key, result=None):
        riak_object = self.riak_object(cls, key, result)
        if not result:
            riak_object.reload()
        return (cls(self, key, _riak_object=riak_object)
                if riak_object.get_data() is not None else None)

    def load_list(self, cls, keys):
        return [self.load(cls, key) for key in keys]

    def riak_map_reduce(self):
        return RiakMapReduce(self.client)

    def run_map_reduce(self, mapreduce, mapper_func):
        raw_results = mapreduce.run()
        results = [mapper_func(self, row) for row in raw_results]
        return results

    def riak_search(self, cls, query, return_keys=False):
        bucket_name = self.bucket_name(cls)
        mr = self.riak_map_reduce().search(bucket_name, query)
        if not return_keys:
            mr = mr.map(function="""
                function (v) {
                    return [[v.key, v.values[0]]]
                }
                """)

        def map_handler(manager, key_and_result):
            if return_keys:
                return key_and_result.get_key()
            else:
                key, result = key_and_result
                return cls.load(manager, key, result)

        return self.run_map_reduce(mr, map_handler)

    def riak_enable_search(self, cls):
        bucket_name = self.bucket_name(cls)
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
