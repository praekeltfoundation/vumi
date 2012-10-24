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

    def __init__(self, message_store):
        self.message_store = message_store
        # Need to define a `manager` for @Manager.calls_manager to work.
        self.manager = self.message_store.manager
        self.redis = self.message_store.redis

    def reconcile(self, message_store, batch_id):
        """
        Reconcile the cache with the data in Riak. This is a heavy process.
        """
        # TODO: idea here is to compare the counts in Redis with the counts
        #       that the shiny new by_index_count stuff gives us. If it's
        #       wildly off then we need to reconcile these two. We need to
        #       do this from Twisted (not in Celery) since doing this
        #       synchronous is going to be unacceptably slow.
        pass

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
