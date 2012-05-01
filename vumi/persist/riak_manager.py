# -*- test-case-name: vumi.persist.tests.test_riak_manager -*-

"""A manager implementation on top of the riak Python package."""

from functools import wraps

from riak import RiakClient, RiakObject, RiakMapReduce
from twisted.internet.defer import _DefGen_Return

from vumi.persist.model import Manager


def flatten_generator(generator_func):
    """
    This is a synchronous version of @inlineCallbacks.

    NOTE: It doesn't correctly handle returnValue() being called in a
    non-decorated function called from the function we're decorating. We could
    copy the Twisted code to do that, but it's messy.
    """
    @wraps(generator_func)
    def wrapped(*args, **kw):
        gen = generator_func(*args, **kw)
        result = None
        while True:
            try:
                result = gen.send(result)
            except StopIteration:
                # Fell off the end, or "return" statement.
                return None
            except _DefGen_Return, e:
                # returnValue() called.
                return e.value

    return wrapped


class RiakManager(Manager):
    """A persistence manager for the riak Python package."""

    call_decorator = staticmethod(flatten_generator)

    @classmethod
    def from_config(cls, config):
        config = config.copy()
        bucket_prefix = config.pop('bucket_prefix')
        client = RiakClient(**config)
        return cls(client, bucket_prefix)

    def riak_object(self, cls, key):
        bucket_name = self.bucket_name(cls)
        bucket = self.client.bucket(bucket_name)
        riak_object = RiakObject(self.client, bucket, key)
        riak_object.set_data({})
        riak_object.set_content_type("application/json")
        return riak_object

    def store(self, modelobj):
        modelobj._riak_object.store()
        return modelobj

    def delete(self, modelobj):
        modelobj._riak_object.delete()

    def load(self, cls, key):
        riak_object = self.riak_object(cls, key)
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
        result = self.client.solr().search(bucket_name, query)
        docs = result['response']['docs']
        keys = [doc['id'] for doc in docs]
        if return_keys:
            return keys
        return self.load_list(cls, keys)

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
