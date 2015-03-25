# -*- test-case-name: vumi.components.tests.test_message_store_cache -*-
# -*- coding: utf-8 -*-

from datetime import datetime
import hashlib
import json
import time

from twisted.internet.defer import returnValue

from vumi.persist.redis_base import Manager
from vumi.message import TransportEvent, parse_vumi_date
from vumi.errors import VumiError


class MessageStoreCacheException(VumiError):
    pass


class MessageStoreCache(object):
    """
    A helper class to provide a view on information in the message store
    that is difficult to query straight from riak.
    """
    BATCH_KEY = 'batches'
    OUTBOUND_KEY = 'outbound'
    OUTBOUND_COUNT_KEY = 'outbound_count'
    INBOUND_KEY = 'inbound'
    INBOUND_COUNT_KEY = 'inbound_count'
    TO_ADDR_KEY = 'to_addr_hll'
    FROM_ADDR_KEY = 'from_addr_hll'
    EVENT_KEY = 'event'
    EVENT_COUNT_KEY = 'event_count'
    STATUS_KEY = 'status'
    SEARCH_TOKEN_KEY = 'search_token'
    SEARCH_RESULT_KEY = 'search_result'
    TRUNCATE_MESSAGE_KEY_COUNT_AT = 2000

    # Cache search results for 24 hrs
    DEFAULT_SEARCH_RESULT_TTL = 60 * 60 * 24

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

    def outbound_count_key(self, batch_id):
        return self.batch_key(self.OUTBOUND_COUNT_KEY, batch_id)

    def inbound_key(self, batch_id):
        return self.batch_key(self.INBOUND_KEY, batch_id)

    def inbound_count_key(self, batch_id):
        return self.batch_key(self.INBOUND_COUNT_KEY, batch_id)

    def to_addr_key(self, batch_id):
        return self.batch_key(self.TO_ADDR_KEY, batch_id)

    def from_addr_key(self, batch_id):
        return self.batch_key(self.FROM_ADDR_KEY, batch_id)

    def status_key(self, batch_id):
        return self.batch_key(self.STATUS_KEY, batch_id)

    def event_key(self, batch_id):
        return self.batch_key(self.EVENT_KEY, batch_id)

    def event_count_key(self, batch_id):
        return self.batch_key(self.EVENT_COUNT_KEY, batch_id)

    def search_token_key(self, batch_id):
        return self.batch_key(self.SEARCH_TOKEN_KEY, batch_id)

    def search_result_key(self, batch_id, token):
        return self.batch_key(self.SEARCH_RESULT_KEY, batch_id, token)

    def uses_counters(self, batch_id):
        """
        Returns ``True`` if ``batch_id`` has moved to the new system
        of using counters instead of assuming all keys are in Redis
        and doing a `zcard` on that.

        The test for this is to see if `inbound_count_key(batch_id)`
        exists. If it is then we've moved to the new system and are
        using counters.
        """
        return self.redis.exists(self.inbound_count_key(batch_id))

    def uses_event_counters(self, batch_id):
        """
        Returns ``True`` if ``batch_id`` has moved to the new system of using
        counters for events instead of assuming all keys are in Redis and doing
        a `zcard` on that.

        The test for this is to see if `inbound_count_key(batch_id)` exists. If
        it is then we've moved to the new system and are using counters.
        """
        return self.redis.exists(self.event_count_key(batch_id))

    @Manager.calls_manager
    def switch_to_counters(self, batch_id):
        """
        Actively switch a batch from the old ``zcard()`` based approach
        to the new ``redis.incr()`` counter based approach.
        """
        uses_counters = yield self.uses_counters(batch_id)
        if uses_counters:
            return

        # NOTE:     Under high load this may result in the counter being off
        #           by a few. Considering this is a cache that is to be
        #           reconciled we're happy for that to be the case.
        inbound_count = yield self.count_inbound_message_keys(batch_id)
        outbound_count = yield self.count_outbound_message_keys(batch_id)

        # We do `*_count or None` because there's a chance of getting back
        # a None if this is a new batch that's not received any traffic yet.
        yield self.redis.set(self.inbound_count_key(batch_id),
                             inbound_count or 0)
        yield self.redis.set(self.outbound_count_key(batch_id),
                             outbound_count or 0)

        yield self.truncate_inbound_message_keys(batch_id)
        yield self.truncate_outbound_message_keys(batch_id)

    @Manager.calls_manager
    def _truncate_keys(self, redis_key, truncate_at):
        # Indexes are zero based
        truncate_at = (truncate_at or self.TRUNCATE_MESSAGE_KEY_COUNT_AT) + 1
        # NOTE: Doing this because ZCARD is O(1) where ZREMRANGEBYRANK is
        #       O(log(N)+M)
        current_size = yield self.redis.zcard(redis_key)
        if current_size <= truncate_at:
            returnValue(0)

        keys_removed = yield self.redis.zremrangebyrank(
            redis_key, 0, truncate_at * -1)
        returnValue(keys_removed)

    def truncate_inbound_message_keys(self, batch_id, truncate_at=None):
        return self._truncate_keys(self.inbound_key(batch_id), truncate_at)

    def truncate_outbound_message_keys(self, batch_id, truncate_at=None):
        return self._truncate_keys(self.outbound_key(batch_id), truncate_at)

    def truncate_event_keys(self, batch_id, truncate_at=None):
        return self._truncate_keys(self.event_key(batch_id), truncate_at)

    @Manager.calls_manager
    def batch_start(self, batch_id, use_counters=True):
        """
        Does various setup work in order to be able to accurately
        store cached data for a batch_id.

        A call to this isn't necessary but good for general house keeping.

        :param bool use_counters:
            If ``True`` this batch is started and will use counters
            rather than Redis zsets() to keep track of message counts.

            Defaults to ``True``.


        This operation idempotent.
        """
        yield self.redis.sadd(self.batch_key(), batch_id)
        yield self.init_status(batch_id)
        if use_counters:
            yield self.redis.set(self.inbound_count_key(batch_id), 0)
            yield self.redis.set(self.outbound_count_key(batch_id), 0)
            yield self.redis.set(self.event_count_key(batch_id), 0)

    @Manager.calls_manager
    def init_status(self, batch_id):
        """
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
        yield self.redis.delete(self.inbound_count_key(batch_id))
        yield self.redis.delete(self.outbound_key(batch_id))
        yield self.redis.delete(self.outbound_count_key(batch_id))
        yield self.redis.delete(self.event_key(batch_id))
        yield self.redis.delete(self.event_count_key(batch_id))
        yield self.redis.delete(self.status_key(batch_id))
        yield self.redis.delete(self.to_addr_key(batch_id))
        yield self.redis.delete(self.from_addr_key(batch_id))
        yield self.redis.srem(self.batch_key(), batch_id)

    def get_timestamp(self, timestamp):
        """
        Return a timestamp value for a datetime value.
        """
        if isinstance(timestamp, basestring):
            timestamp = parse_vumi_date(timestamp)
        return time.mktime(timestamp.timetuple())

    @Manager.calls_manager
    def add_outbound_message(self, batch_id, msg):
        """
        Add an outbound message to the cache for the given batch_id
        """
        timestamp = self.get_timestamp(msg['timestamp'])
        yield self.add_outbound_message_key(
            batch_id, msg['message_id'], timestamp)
        yield self.add_to_addr(batch_id, msg['to_addr'])

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

            uses_counters = yield self.uses_counters(batch_id)
            if uses_counters:
                yield self.redis.incr(self.outbound_count_key(batch_id))
                yield self.truncate_outbound_message_keys(batch_id)

    @Manager.calls_manager
    def add_outbound_message_count(self, batch_id, count):
        """
        Add a count to all outbound message counters. (Used for recon.)
        """
        yield self.increment_event_status(batch_id, 'sent', count)
        yield self.redis.incr(self.outbound_count_key(batch_id), count)

    @Manager.calls_manager
    def add_event_count(self, batch_id, status, count):
        """
        Add a count to all relevant event counters. (Used for recon.)
        """
        yield self.increment_event_status(batch_id, status, count)
        yield self.redis.incr(self.event_count_key(batch_id), count)

    @Manager.calls_manager
    def add_event(self, batch_id, event):
        """
        Add an event to the cache for the given batch_id
        """
        event_id = event['event_id']
        timestamp = self.get_timestamp(event['timestamp'])
        new_entry = yield self.add_event_key(batch_id, event_id, timestamp)
        if new_entry:
            event_type = event['event_type']
            yield self.increment_event_status(batch_id, event_type)
            if event_type == 'delivery_report':
                yield self.increment_event_status(
                    batch_id, '%s.%s' % (event_type, event['delivery_status']))

    @Manager.calls_manager
    def add_event_key(self, batch_id, event_key, timestamp):
        """
        Add the event key to the set of known event keys.
        Returns 0 if the key already exists in the set, 1 if it doesn't.
        """
        uses_event_counters = yield self.uses_event_counters(batch_id)
        if uses_event_counters:
            new_entry = yield self.redis.zadd(self.event_key(batch_id), **{
                event_key.encode('utf-8'): timestamp,
            })
            if new_entry:
                yield self.redis.incr(self.event_count_key(batch_id))
                yield self.truncate_event_keys(batch_id)
            returnValue(new_entry)
        else:
            # HACK: Disabling this because of unbounded growth.
            #       Please perform reconciliation on all batches that still use
            #       SET-based event tracking.
            # NOTE: Cheaper recon is coming Real Soon Now.
            returnValue(False)
            # This uses a set, not a sorted set.
            new_entry = yield self.redis.sadd(
                self.event_key(batch_id), event_key)
            returnValue(new_entry)

    def increment_event_status(self, batch_id, event_type, count=1):
        """
        Increment the status for the given event_type for the given batch_id.
        """
        return self.redis.hincrby(self.status_key(batch_id), event_type, count)

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
        yield self.add_inbound_message_key(
            batch_id, msg['message_id'], timestamp)
        yield self.add_from_addr(batch_id, msg['from_addr'])

    @Manager.calls_manager
    def add_inbound_message_key(self, batch_id, message_key, timestamp):
        """
        Add a message key, weighted with the timestamp to the batch_id
        """
        new_entry = yield self.redis.zadd(self.inbound_key(batch_id), **{
            message_key.encode('utf-8'): timestamp,
        })

        if new_entry:
            uses_counters = yield self.uses_counters(batch_id)
            if uses_counters:
                yield self.redis.incr(self.inbound_count_key(batch_id))
                yield self.truncate_inbound_message_keys(batch_id)

    @Manager.calls_manager
    def add_inbound_message_count(self, batch_id, count):
        """
        Add a count to all inbound message counters. (Used for recon.)
        """
        yield self.redis.incr(self.inbound_count_key(batch_id), count)

    def add_from_addr(self, batch_id, from_addr):
        """
        Add a from_addr to this batch_id using Redis's HyperLogLog
        functionality. Generally this information is set when
        `add_inbound_message()` is called.
        """
        return self.redis.pfadd(
            self.from_addr_key(batch_id), from_addr.encode('utf-8'))

    def get_from_addrs(self, batch_id, asc=False):
        """
        Return a set of all known from_addrs sorted by timestamp.
        """
        # NOTE: Disabled because this doesn't scale to large batches.
        #       See https://github.com/praekelt/vumi/issues/877

        # return self.redis.zrange(self.from_addr_key(batch_id), 0, -1,
        #                          desc=not asc)
        return []

    def count_from_addrs(self, batch_id):
        """
        Return count of the unique from_addrs in this batch. Note that the
        returned count is not exact. We use Redis's HyperLogLog functionality,
        so the count is subject to a standard error of 0.81%:
        http://redis.io/commands/pfcount
        """
        return self.redis.pfcount(self.from_addr_key(batch_id))

    def add_to_addr(self, batch_id, to_addr):
        """
        Add a to_addr to this batch_id using Redis's HyperLogLog
        functionality. Generally this information is set when
        `add_outbound_message()` is called.
        """
        return self.redis.pfadd(
            self.to_addr_key(batch_id), to_addr.encode('utf-8'))

    def get_to_addrs(self, batch_id, asc=False):
        """
        Return a set of unique to_addrs addressed in this batch ordered
        by the most recent timestamp.
        """
        # NOTE: Disabled because this doesn't scale to large batches.
        #       See https://github.com/praekelt/vumi/issues/877

        # return self.redis.zrange(self.to_addr_key(batch_id), 0, -1,
        #                          desc=not asc)
        return []

    def count_to_addrs(self, batch_id):
        """
        Return count of the unique to_addrs in this batch. Note that the
        returned count is not exact. We use Redis's HyperLogLog functionality,
        so the count is subject to a standard error of 0.81%:
        http://redis.io/commands/pfcount
        """
        return self.redis.pfcount(self.to_addr_key(batch_id))

    def get_inbound_message_keys(self, batch_id, start=0, stop=-1, asc=False,
                                 with_timestamp=False):
        """
        Return a list of keys ordered according to their timestamps
        """
        return self.redis.zrange(self.inbound_key(batch_id),
                                 start, stop, desc=not asc,
                                 withscores=with_timestamp)

    @Manager.calls_manager
    def inbound_message_count(self, batch_id):
        count = yield self.redis.get(self.inbound_count_key(batch_id))
        returnValue(0 if count is None else int(count))

    def inbound_message_keys_size(self, batch_id):
        return self.redis.zcard(self.inbound_key(batch_id))

    @Manager.calls_manager
    def count_inbound_message_keys(self, batch_id):
        """
        Return the count of the unique inbound message keys for this batch_id
        """
        if not (yield self.uses_counters(batch_id)):
            returnValue((yield self.inbound_message_keys_size(batch_id)))

        count = yield self.inbound_message_count(batch_id)
        returnValue(count)

    def get_outbound_message_keys(self, batch_id, start=0, stop=-1, asc=False,
                                  with_timestamp=False):
        """
        Return a list of keys ordered according to their timestamps.
        """
        return self.redis.zrange(self.outbound_key(batch_id),
                                 start, stop, desc=not asc,
                                 withscores=with_timestamp)

    @Manager.calls_manager
    def outbound_message_count(self, batch_id):
        count = yield self.redis.get(self.outbound_count_key(batch_id))
        returnValue(0 if count is None else int(count))

    def outbound_message_keys_size(self, batch_id):
        return self.redis.zcard(self.outbound_key(batch_id))

    @Manager.calls_manager
    def count_outbound_message_keys(self, batch_id):
        """
        Return the count of the unique outbound message keys for this batch_id
        """
        if not (yield self.uses_counters(batch_id)):
            returnValue((yield self.outbound_message_keys_size(batch_id)))

        count = yield self.outbound_message_count(batch_id)
        returnValue(count)

    @Manager.calls_manager
    def event_count(self, batch_id):
        count = yield self.redis.get(self.event_count_key(batch_id))
        returnValue(0 if count is None else int(count))

    @Manager.calls_manager
    def count_event_keys(self, batch_id):
        """
        Return the count of the unique event keys for this batch_id
        """
        uses_event_counters = yield self.uses_event_counters(batch_id)
        if uses_event_counters:
            count = yield self.event_count(batch_id)
            returnValue(count)
        else:
            count = yield self.redis.scard(self.event_key(batch_id))
            returnValue(count)

    @Manager.calls_manager
    def count_inbound_throughput(self, batch_id, sample_time=300):
        """
        Calculate the number of messages seen in the last `sample_time` amount
        of seconds.

        :param int sample_time:
            How far to look back to calculate the throughput.
            Defaults to 300 seconds (5 minutes)
        """
        last_seen = yield self.redis.zrange(
            self.inbound_key(batch_id), 0, 0, desc=True,
            withscores=True)
        if not last_seen:
            returnValue(0)

        [(latest, timestamp)] = last_seen
        count = yield self.redis.zcount(
            self.inbound_key(batch_id), timestamp - sample_time, timestamp)
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
        last_seen = yield self.redis.zrange(
            self.outbound_key(batch_id), 0, 0, desc=True, withscores=True)
        if not last_seen:
            returnValue(0)

        [(latest, timestamp)] = last_seen
        count = yield self.redis.zcount(
            self.outbound_key(batch_id), timestamp - sample_time, timestamp)
        returnValue(int(count))

    def get_query_token(self, direction, query):
        """
        Return a token for the query.

        The query is a list of dictionaries, to ensure consistent keys
        we want to make sure the input is always ordered the same before
        creating a cache key.

        :param str direction:
            Namespace to store this query under.
            Generally 'inbound' or 'outbound'.
        :param list query:
            A list of dictionaries with query information.

        """
        ordered_query = sorted([sorted(part.items()) for part in query])
        # TODO: figure out if JSON is necessary here or if something like str()
        #       will work just as well.
        return '%s-%s' % (
            direction, hashlib.md5(json.dumps(ordered_query)).hexdigest())

    @Manager.calls_manager
    def start_query(self, batch_id, direction, query):
        """
        Start a query operation on the inbound messages for the given batch_id.
        Returns a token with which the results of the query can be fetched
        as soon as they arrive.
        """
        token = self.get_query_token(direction, query)
        yield self.redis.sadd(self.search_token_key(batch_id), token)
        returnValue(token)

    @Manager.calls_manager
    def store_query_results(self, batch_id, token, keys, direction,
                            ttl=None):
        """
        Store the inbound query results for a query that was started with
        `start_inbound_query`. Internally this grabs the timestamps from
        the cache (there is an assumption that it has already been reconciled)
        and orders the results accordingly.

        :param str token:
            The token to store the results under.
        :param list keys:
            The list of keys to store.
        :param str direction:
            Which messages to search, either inbound or outbound.
        :param int ttl:
            How long to store the results for.
            Defaults to DEFAULT_SEARCH_RESULT_TTL.
        """
        ttl = ttl or self.DEFAULT_SEARCH_RESULT_TTL
        result_key = self.search_result_key(batch_id, token)
        if direction == 'inbound':
            score_set_key = self.inbound_key(batch_id)
        elif direction == 'outbound':
            score_set_key = self.outbound_key(batch_id)
        else:
            raise MessageStoreCacheException('Invalid direction')

        # populate the results set weighted according to the timestamps
        # that are already known in the cache.
        for key in keys:
            timestamp = yield self.redis.zscore(score_set_key, key)
            yield self.redis.zadd(result_key, **{
                key.encode('utf-8'): timestamp,
            })

        # Auto expire after TTL
        yield self.redis.expire(result_key, ttl)
        # Remove from the list of in progress search operations.
        yield self.redis.srem(self.search_token_key(batch_id), token)

    def is_query_in_progress(self, batch_id, token):
        """
        Check whether a search is still in progress for the given token.
        """
        return self.redis.sismember(self.search_token_key(batch_id), token)

    def get_query_results(self, batch_id, token, start=0, stop=-1,
                          asc=False):
        """
        Return the results for the query token. Will return an empty list
        of no results are available.
        """
        result_key = self.search_result_key(batch_id, token)
        return self.redis.zrange(result_key, start, stop, desc=not asc)

    def count_query_results(self, batch_id, token):
        """
        Return the number of results for the query token.
        """
        result_key = self.search_result_key(batch_id, token)
        return self.redis.zcard(result_key)
