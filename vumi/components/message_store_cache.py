# -*- test-case-name: vumi.components.tests.test_message_store_cache -*-
# -*- coding: utf-8 -*-
import time

from twisted.internet.defer import returnValue

from vumi.persist.redis_base import Manager
from vumi.message import TransportEvent


class MessageStoreCache(object):
    """
    A helper class to provide a view on information in the message store
    that is difficult to query straight from riak.
    """
    BATCH_KEY = 'batches'
    OUTBOUND_KEY = 'outbound'
    INBOUND_KEY = 'inbound'
    TO_ADDR_KEY = 'to_addr'
    FROM_ADDR_KEY = 'from_addr'
    STATUS_KEY = 'status'

    def __init__(self, redis):
        # Store redis as `manager` as well since @Manager.calls_manager
        # requires it to be named as such.
        self.redis = self.manager = redis

    @Manager.calls_manager
    def calculate_offsets(self, message_store, batch_id):
        """
        Check if the cache needs to be reconciled with the data in Riak.
        This is a heavy process since we're doing index based counts
        in Riak and comparing them to the cached counts.

        Returns a tuple (inbound_offset, outbound_offset) of the
        message_store count minus the cached_count.
        """
        inbound_offset = yield self.calculate_inbound_offset(message_store,
            batch_id)
        outbound_offset = yield self.calculate_outbound_offset(message_store,
            batch_id)
        returnValue((inbound_offset, outbound_offset))

    @Manager.calls_manager
    def calculate_inbound_offset(self, message_store, batch_id):
        ms_count = yield message_store.batch_inbound_count(batch_id)
        cache_count = yield self.count_inbound_message_keys(batch_id)
        returnValue(ms_count - cache_count)

    @Manager.calls_manager
    def calculate_outbound_offset(self, message_store, batch_id):
        ms_count = yield message_store.batch_outbound_count(batch_id)
        cache_count = yield self.count_outbound_message_keys(batch_id)
        returnValue(ms_count - cache_count)

    def key(self, *args):
        return ':'.join([unicode(a) for a in args])

    def batch_key(self, *args):
        return self.key(self.BATCH_KEY, *args)

    def outbound_key(self, batch_id):
        return self.batch_key(self.OUTBOUND_KEY, batch_id)

    def inbound_key(self, batch_id):
        return self.batch_key(self.INBOUND_KEY, batch_id)

    def to_addr_key(self, batch_id):
        return self.batch_key(self.TO_ADDR_KEY, batch_id)

    def from_addr_key(self, batch_id):
        return self.batch_key(self.FROM_ADDR_KEY, batch_id)

    def status_key(self, batch_id):
        return self.batch_key(self.STATUS_KEY, batch_id)

    @Manager.calls_manager
    def batch_start(self, batch_id):
        """
        Does various setup work in order to be able to accurately
        store cached data for a batch_id.

        A call to this isn't necessary but good for general house keeping.

        This operation idempotent.
        """
        yield self.redis.sadd(self.batch_key(), batch_id)
        yield self.init_status(batch_id)

    @Manager.calls_manager
    def init_status(self, batch_id):
        """"
        Setup the hash for event tracking on this batch, it primes the
        hash to have the bare minimum of expected keys and their values
        all set to 0. If there's already an existing value then it is
        left untouched.
        """
        events = (TransportEvent.EVENT_TYPES.keys() +
                  ['delivery_report.%s' % status
                   for status in TransportEvent.DELIVERY_STATUSES] +
                  ['sent'])
        for event in events:
            yield self.redis.hsetnx(self.status_key(batch_id), event, 0)

    @Manager.calls_manager
    def get_batch_ids(self):
        """
        Return a list of known batch_ids
        """
        batch_ids = yield self.redis.smembers(self.batch_key())
        returnValue(batch_ids)

    def batch_exists(self, batch_id):
        return self.redis.sismember(self.batch_key(), batch_id)

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
        yield self.increment_event_status(batch_id, 'sent')
        yield self.redis.zadd(self.outbound_key(batch_id), **{
            message_key: timestamp,
            })

    @Manager.calls_manager
    def add_event(self, batch_id, event):
        """
        Add an event to the cache for the given batch_id
        """
        event_type = event['event_type']
        yield self.increment_event_status(batch_id, event_type)
        if event_type == 'delivery_report':
            yield self.increment_event_status(batch_id,
                '%s.%s' % (event_type, event['delivery_status']))

    def increment_event_status(self, batch_id, event_type):
        """
        Increment the status for the given event_type by 1 for the given
        batch_id
        """
        return self.redis.hincrby(self.status_key(batch_id), event_type,  1)

    def get_event_status(self, batch_id):
        """
        Return a dictionary containing the latest event stats for the given
        batch_id.
        """
        return self.redis.hgetall(self.status_key(batch_id))

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

    def count_from_addrs(self, batch_id):
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

    def count_to_addrs(self, batch_id):
        """
        Return count of the unique to_addrs in this batch.
        """
        return self.redis.zcard(self.to_addr_key(batch_id))

    @Manager.calls_manager
    def get_inbound_message_keys(self, batch_id, start=0, stop=-1):
        """
        Return a list of keys ordered according to their timestamps
        """
        keys = yield self.redis.zrange(self.inbound_key(batch_id),
                                        start, stop)
        returnValue(keys)

    def count_inbound_message_keys(self, batch_id):
        """
        Return the count of the unique inbound message keys for this batch_id
        """
        return self.redis.zcard(self.inbound_key(batch_id))

    @Manager.calls_manager
    def get_outbound_message_keys(self, batch_id, start=0, stop=-1):
        """
        Return a list of keys ordered according to their timestamps.
        """
        keys = yield self.redis.zrange(self.outbound_key(batch_id),
                                        start, stop)
        returnValue(keys)

    def count_outbound_message_keys(self, batch_id):
        """
        Return the count of the unique outbound message keys for this batch_id
        """
        return self.redis.zcard(self.outbound_key(batch_id))
