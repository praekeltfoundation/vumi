# -*- test-case-name: vumi.persist.tests.test_riak_manager -*-

"""A manager implementation on top of the riak Python package."""

from riak import RiakClient, RiakObject, RiakMapReduce

from vumi.persist.model import Manager


class RiakManager(Manager):
    """A persistence manager for the riak Python package."""

    @classmethod
    def from_config(cls, config):
        bucket_prefix = config.pop('bucket_prefix')
        client = RiakClient(**config)
        return cls(client, bucket_prefix)

    def riak_object(self, modelobj):
        bucket_name = self.bucket_prefix + modelobj.bucket
        bucket = self.client.bucket(bucket_name)
        riak_object = RiakObject(self.client, bucket, modelobj.key)
        riak_object.set_data({})
        riak_object.set_content_type("application/json")
        return riak_object

    def store(self, modelobj):
        modelobj._riak_object.store()
        return modelobj

    def load(self, modelobj):
        result = modelobj._riak_object.reload()
        return modelobj if result.get_data() is not None else None

    def load_list(self, modelobjs):
        return [self.load(modelobj) for modelobj in modelobjs]

    def riak_map_reduce(self):
        return RiakMapReduce(self.client)

    def run_map_reduce(self, mapreduce, mapper_func):
        raw_results = mapreduce.run()
        results = [mapper_func(self, row) for row in raw_results]
        return results

    def purge_all(self):
        buckets = self.client.get_buckets()
        for bucket_name in buckets:
            if bucket_name.startswith(self.bucket_prefix):
                bucket = self.client.bucket(bucket_name)
                for key in bucket.get_keys():
                    obj = bucket.get(key)
                    obj.delete()
