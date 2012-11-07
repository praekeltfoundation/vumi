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
    EVENT_KEY = 'event'
    STATUS_KEY = 'status'

    def __init__(self, redis):
        # Store redis as `manager` as well since @Manager.calls_manager
        # requires it to be named as such.
        self.redis = self.manager = redis

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

    def event_key(self, batch_id):
        return self.batch_key(self.EVENT_KEY, batch_id)

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

    def get_batch_ids(self):
        """
        Return a list of known batch_ids
        """
        return self.redis.smembers(self.batch_key())

    def batch_exists(self, batch_id):
        return self.redis.sismember(self.batch_key(), batch_id)

    @Manager.calls_manager
    def clear_batch(self, batch_id):
        """
        Removes all cached values for the given batch_id, useful before
        a reconciliation happens to ensure that we start from scratch.

        NOTE:   This will reset all counters back to zero and will increment
                them as messages are received. If your UI depends on your
                cached values your UI values might be off while the
                reconciliation is taking place.
        """
        yield self.redis.delete(self.inbound_key(batch_id))
        yield self.redis.delete(self.outbound_key(batch_id))
        yield self.redis.delete(self.event_key(batch_id))
        yield self.redis.delete(self.status_key(batch_id))
        yield self.redis.delete(self.to_addr_key(batch_id))
        yield self.redis.delete(self.from_addr_key(batch_id))
        yield self.redis.srem(self.batch_key(), batch_id)

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
        new_entry = yield self.redis.zadd(self.outbound_key(batch_id), **{
            message_key.encode('utf-8'): timestamp,
            })
        if new_entry:
            yield self.increment_event_status(batch_id, 'sent')

    @Manager.calls_manager
    def add_event(self, batch_id, event):
        """
        Add an event to the cache for the given batch_id
        """

        event_id = event['event_id']
        new_entry = yield self.add_event_key(batch_id, event_id)
        if new_entry:
            event_type = event['event_type']
            yield self.increment_event_status(batch_id, event_type)
            if event_type == 'delivery_report':
                yield self.increment_event_status(batch_id,
                    '%s.%s' % (event_type, event['delivery_status']))

    def add_event_key(self, batch_id, event_key):
        """
        Add the event key to the set of known event keys.
        Returns 0 if the key already exists in the set, 1 if it doesn't.
        """
        return self.redis.sadd(self.event_key(batch_id), event_key)

    def increment_event_status(self, batch_id, event_type):
        """
        Increment the status for the given event_type by 1 for the given
        batch_id
        """
        return self.redis.hincrby(self.status_key(batch_id), event_type,  1)

    @Manager.calls_manager
    def get_event_status(self, batch_id):
        """
        Return a dictionary containing the latest event stats for the given
        batch_id.
        """
        stats = yield self.redis.hgetall(self.status_key(batch_id))
        returnValue(dict([(k, int(v)) for k, v in stats.iteritems()]))

    @Manager.calls_manager
    def add_inbound_message(self, batch_id, msg):
        """
        Add an inbound message to the cache for the given batch_id
        """
        timestamp = self.get_timestamp(msg['timestamp'])
        yield self.add_inbound_message_key(batch_id, msg['message_id'],
            timestamp)
        yield self.add_from_addr(batch_id, msg['from_addr'], timestamp)

    def add_inbound_message_key(self, batch_id, message_key, timestamp):
        """
        Add a message key, weighted with the timestamp to the batch_id
        """
        return self.redis.zadd(self.inbound_key(batch_id), **{
            message_key.encode('utf-8'): timestamp,
            })

    def add_from_addr(self, batch_id, from_addr, timestamp):
        """
        Add a from_addr to this batch_id, weighted by timestamp. Generally
        this information is retrieved when `add_inbound_message()` is called.
        """
        return self.redis.zadd(self.from_addr_key(batch_id), **{
            from_addr.encode('utf-8'): timestamp,
            })

    def get_from_addrs(self, batch_id, asc=False):
        """
        Return a set of all known from_addrs sorted by timestamp.
        """
        return self.redis.zrange(self.from_addr_key(batch_id), 0, -1,
            desc=not asc)

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
            to_addr.encode('utf-8'): timestamp,
            })

    def get_to_addrs(self, batch_id, asc=False):
        """
        Return a set of unique to_addrs addressed in this batch ordered
        by the most recent timestamp.
        """
        return self.redis.zrange(self.to_addr_key(batch_id), 0, -1,
            desc=not asc)

    def count_to_addrs(self, batch_id):
        """
        Return count of the unique to_addrs in this batch.
        """
        return self.redis.zcard(self.to_addr_key(batch_id))

    def get_inbound_message_keys(self, batch_id, start=0, stop=-1, asc=False):
        """
        Return a list of keys ordered according to their timestamps
        """
        return self.redis.zrange(self.inbound_key(batch_id),
                                        start, stop, desc=not asc)

    def count_inbound_message_keys(self, batch_id):
        """
        Return the count of the unique inbound message keys for this batch_id
        """
        return self.redis.zcard(self.inbound_key(batch_id))

    def get_outbound_message_keys(self, batch_id, start=0, stop=-1,
        asc=False):
        """
        Return a list of keys ordered according to their timestamps.
        """
        return self.redis.zrange(self.outbound_key(batch_id),
                                        start, stop, desc=not asc)

    def count_outbound_message_keys(self, batch_id):
        """
        Return the count of the unique outbound message keys for this batch_id
        """
        return self.redis.zcard(self.outbound_key(batch_id))

    @Manager.calls_manager
    def count_inbound_throughput(self, batch_id, sample_time=300):
        """
        Calculate the number of messages seen in the last `sample_time` amount
        of seconds.

        :param int sample_time:
            How far to look back to calculate the throughput.
            Defaults to 300 seconds (5 minutes)
        """
        [(latest, timestamp)] = yield self.redis.zrange(
                            self.inbound_key(batch_id), 0, 0, desc=True,
                            withscores=True)
        count = yield self.redis.zcount(self.inbound_key(batch_id),
            timestamp, timestamp + sample_time)
        returnValue(int(count))

    @Manager.calls_manager
    def count_outbound_throughput(self, batch_id, sample_time=300):
        """
        Calculate the number of messages seen in the last `sample_time` amount
        of seconds.

        :param int sample_time:
            How far to look back to calculate the throughput.
            Defaults to 300 seconds (5 minutes)
        """
        [(latest, timestamp)] = yield self.redis.zrange(
                            self.outbound_key(batch_id), 0, 0, desc=True,
                            withscores=True)
        count = yield self.redis.zcount(self.outbound_key(batch_id),
            timestamp, timestamp + sample_time)
        returnValue(int(count))
