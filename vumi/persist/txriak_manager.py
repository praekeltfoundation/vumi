# -*- test-case-name: vumi.persist.tests.test_txriak_manager -*-

"""A manager implementation on top of txriak."""

from txriak.riak import RiakClient, RiakObject
from twisted.internet.defer import inlineCallbacks

from vumi.persist.model import Manager


class TxRiakManager(Manager):
    """A persistence manager for txriak."""

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
        d = modelobj._riak_object.store()
        d.addCallback(lambda result: modelobj)
        return d

    def load(self, modelobj):
        d = modelobj._riak_object.reload()
        d.addCallback(lambda result: modelobj)
        return d

    @inlineCallbacks
    def purge_all(self):
        buckets = yield self.client.list_buckets()
        for bucket_name in buckets:
            if bucket_name.startswith(self.bucket_prefix):
                bucket = self.client.bucket(bucket_name)
                yield bucket.purge_keys()
