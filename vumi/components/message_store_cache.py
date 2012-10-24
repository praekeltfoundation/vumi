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
    INBOUND_KEY = 'inbound'
    TO_ADDR_KEY = 'to_addr'
    FROM_ADDR_KEY = 'from_addr'

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

    def batch_key(self, *args):
        return self.key(self.BATCH_KEY, *args)

    def outbound_key(self, batch_id):
        return self.batch_key(batch_id, self.OUTBOUND_KEY)

    def inbound_key(self, batch_id):
        return self.batch_key(batch_id, self.INBOUND_KEY)

    def to_addr_key(self, batch_id):
        return self.batch_key(batch_id, self.TO_ADDR_KEY)

    def from_addr_key(self, batch_id):
        return self.batch_key(batch_id, self.FROM_ADDR_KEY)

    @Manager.calls_manager
    def get_batch_ids(self):
        """
        Return a list of known batch_ids
        """
        batch_ids = yield self.redis.smembers(self.batch_key())
        returnValue(batch_ids)

    def get_timestamp(self, datetime):
        """
        Return a timestamp value for a datetime value.
        """
        return time.mktime(datetime.timetuple())

    @Manager.calls_manager
    def add_outbound_message(self, batch_id, msg):
        """
        Add an outbound message to the cache for the given batch_id
        """
        timestamp = self.get_timestamp(msg['timestamp'])
        yield self.add_outbound_message_key(batch_id, msg['message_id'],
            timestamp)
        yield self.add_to_addr(batch_id, msg['to_addr'], timestamp)

    @Manager.calls_manager
    def add_outbound_message_key(self, batch_id, message_key, timestamp):
        """
        Add a message key, weighted with the timestamp to the batch_id.
        """
        yield self.redis.sadd(self.batch_key(), batch_id)
        yield self.redis.zadd(self.outbound_key(batch_id), **{
            message_key: timestamp,
            })

    def add_event(self, batch_id, event):
        """
        Add an event to the cache for the given batch_id
        """
        pass

    @Manager.calls_manager
    def add_inbound_message(self, batch_id, msg):
        """
        Add an inbound message to the cache for the given batch_id
        """
        timestamp = self.get_timestamp(msg['timestamp'])
        yield self.add_inbound_message_key(batch_id, msg['message_id'],
            timestamp)
        yield self.add_from_addr(batch_id, msg['from_addr'], timestamp)

    @Manager.calls_manager
    def add_inbound_message_key(self, batch_id, message_key, timestamp):
        """
        Add a message key, weighted with the timestamp to the batch_id
        """
        yield self.redis.sadd(self.batch_key(), batch_id)
        yield self.redis.zadd(self.inbound_key(batch_id), **{
            message_key: timestamp,
            })

    def add_from_addr(self, batch_id, from_addr, timestamp):
        """
        Add a from_addr to this batch_id, weighted by timestamp. Generally
        this information is retrieved when `add_inbound_message()` is called.
        """
        return self.redis.zadd(self.from_addr_key(batch_id), **{
            from_addr: timestamp,
            })

    def get_from_addrs(self, batch_id):
        """
        Return a set of all known from_addrs sorted by timestamp.
        """
        return self.redis.zrange(self.from_addr_key(batch_id), 0, -1)

    def get_from_addrs_count(self, batch_id):
        """
        Return the number of from_addrs for this batch_id
        """
        return self.redis.zcard(self.from_addr_key(batch_id))

    def add_to_addr(self, batch_id, to_addr, timestamp):
        """
        Add a to-addr to this batch_id, weighted by timestamp. Generally
        this information is retrieved when `add_outbound_message()` is called.
        """
        return self.redis.zadd(self.to_addr_key(batch_id), **{
            to_addr: timestamp,
            })

    def get_to_addrs(self, batch_id):
        """
        Return a set of unique to_addrs addressed in this batch ordered
        by the most recent timestamp.
        """
        return self.redis.zrange(self.to_addr_key(batch_id), 0, -1)

    def get_to_addrs_count(self, batch_id):
        """
        Return count of the unique to_addrs in this batch.
        """
        return self.redis.zcard(self.to_addr_key(batch_id))

    @Manager.calls_manager
    def get_inbound_message_keys(self, batch_id):
        """
        Return a list of keys ordered according to their timestamps
        """
        keys = yield self.redis.zrange(self.inbound_key(batch_id), 0, -1)
        returnValue(keys)

    @Manager.calls_manager
    def get_outbound_message_keys(self, batch_id):
        """
        Return a list of keys ordered according to their timestamps.
        """
        keys = yield self.redis.zrange(self.outbound_key(batch_id), 0, -1)
        returnValue(keys)
