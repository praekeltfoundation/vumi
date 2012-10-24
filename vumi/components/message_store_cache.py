# -*- test-case-name: vumi.components.tests.test_message_store_cache -*-
# -*- coding: utf-8 -*-
import time

from twisted.internet.defer import returnValue

from vumi.persist.redis_base import Manager


class MessageStoreCache(object):
    """
    A helper class to provide a view on information in the message store
    that is difficult to query straight from riak.
    """
    BATCH_KEY = 'batch_id'
    OUTBOUND_KEY = 'outbound'

    def __init__(self, redis):
        # Also store as manager this the @Manager.calls_manager decorator
        # expects it to be called as such.
        self.redis = self.manager = redis

    def reconcile(self, message_store, batch_id):
        """
        Reconcile the cache with the data in Riak. This is a heavy process.
        """
        pass

    def key(self, *args):
        return ':'.join([unicode(a) for a in args])

    def batch_key(self, batch_id, *args):
        return self.key(self.BATCH_KEY, batch_id, *args)

    def outbound_key(self, batch_id):
        return self.batch_key(batch_id, self.OUTBOUND_KEY)

    def list_batch_ids(self):
        """
        Return a list of known batch_ids
        """

    def get_timestamp(self, datetime):
        """
        Return a timestamp value for a datetime value.
        """
        return time.mktime(datetime.timetuple())

    def add_outbound_message(self, batch_id, msg):
        """
        Add an outbound message to the cache for the given batch_id
        """
        return self.add_outbound_message_key(batch_id, msg['message_id'],
            self.get_timestamp(msg['timestamp']))

    @Manager.calls_manager
    def add_outbound_message_key(self, batch_id, message_key, timestamp):
        """
        Add a message key, weighted with the timestamp to the batch_id.
        """
        yield self.redis.sadd(self.batch_key(batch_id))
        yield self.redis.zadd(self.outbound_key(batch_id), **{
            message_key: timestamp,
        })

    def add_event(self, batch_id, event):
        """
        Add an event to the cache for the given batch_id
        """
        pass

    def add_inbound_message(self, batch_id, msg):
        """
        Add an inbound message to the cache for the given batch_id
        """
        pass

    def add_inbound_message_key(self, batch_id, message_key, timestamp):
        pass

    def get_to_addrs(self, batch_id):
        """
        Return a set of unique to_addrs addressed in this batch ordered
        by the most recent timestamp.
        """
        pass

    def get_to_addrs_count(self, batch_id):
        """
        Return count of the unique to_addrs in this batch.
        """
        pass

    def get_inbound_message_keys(self, batch_id):
        """
        Return a list of keys ordered according to their timestamps
        """
        pass

    @Manager.calls_manager
    def get_outbound_message_keys(self, batch_id):
        """
        Return a list of keys ordered according to their timestamps.
        """
        keys = yield self.redis.zrange(self.outbound_key(batch_id), 0, -1)
        returnValue(keys)
