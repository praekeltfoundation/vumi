# -*- test-case-name: vumi.components.tests.test_message_store_cache -*-
# -*- coding: utf-8 -*-
import time
import hashlib
import json

from twisted.internet.defer import returnValue, inlineCallbacks

from vumi.persist.redis_base import Manager
from vumi.message import TransportEvent
from vumi.errors import VumiError


class MessageStoreCacheException(VumiError):
    pass


class MessageStoreCacheMigration(object):

    def __init__(self, cache):
        self.cache = cache

    def migrate_from_unversioned(self, batch_id):
        return self.cache.set_cache_version(batch_id, 0)


class MessageStoreCacheMigrator(object):
    """on the fly migrations for vumi#685"""

    MIGRATION_CLASS = MessageStoreCacheMigration

    @inlineCallbacks
    def migrate(self, cache, batch_id):
        """
        When given a batch_id, find what cache version it is on.
        If it doesn't equal `cache.MIGRATION_VERSION` then start the
        migration class and migrate until the latest version is reached.
        """
        while True:
            current_version = yield cache.get_cache_version(batch_id)
            if current_version is None:
                migrate_function = 'migrate_from_unversioned'
            elif int(current_version) != cache.MIGRATION_VERSION:
                migrate_function = 'migrate_from_%s' % (current_version,)
            else:
                returnValue(cache)

            migration = self.MIGRATION_CLASS(cache)
            handler = getattr(migration, migrate_function, None)
            if handler is None:
                raise MessageStoreCacheException(
                    'Needing to migration from %s but %s is not defined' % (
                        current_version, migrate_function))
            yield handler(batch_id)


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
    SEARCH_TOKEN_KEY = 'search_token'
    SEARCH_RESULT_KEY = 'search_result'
    CACHE_VERSION_KEY = 'cache_version'

    # Cache search results for 24 hrs
    DEFAULT_SEARCH_RESULT_TTL = 60 * 60 * 24

    # Migration counter
    MIGRATION_VERSION = 0

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

    def search_token_key(self, batch_id):
        return self.batch_key(self.SEARCH_TOKEN_KEY, batch_id)

    def search_result_key(self, batch_id, token):
        return self.batch_key(self.SEARCH_RESULT_KEY, batch_id, token)

    def cache_version_key(self, batch_id):
        return self.batch_key(self.CACHE_VERSION_KEY, batch_id)

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
        return self.redis.hincrby(self.status_key(batch_id), event_type, 1)

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

    def get_inbound_message_keys(self, batch_id, start=0, stop=-1, asc=False,
                                    with_timestamp=False):
        """
        Return a list of keys ordered according to their timestamps
        """
        return self.redis.zrange(self.inbound_key(batch_id),
                                    start, stop, desc=not asc,
                                    withscores=with_timestamp)

    def count_inbound_message_keys(self, batch_id):
        """
        Return the count of the unique inbound message keys for this batch_id
        """
        return self.redis.zcard(self.inbound_key(batch_id))

    def get_outbound_message_keys(self, batch_id, start=0, stop=-1, asc=False,
                                    with_timestamp=False):
        """
        Return a list of keys ordered according to their timestamps.
        """
        return self.redis.zrange(self.outbound_key(batch_id),
                                        start, stop, desc=not asc,
                                        withscores=with_timestamp)

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
        last_seen = yield self.redis.zrange(
                            self.inbound_key(batch_id), 0, 0, desc=True,
                            withscores=True)
        if not last_seen:
            returnValue(0)

        [(latest, timestamp)] = last_seen
        count = yield self.redis.zcount(self.inbound_key(batch_id),
            timestamp - sample_time, timestamp)
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
                            self.outbound_key(batch_id), 0, 0, desc=True,
                            withscores=True)
        if not last_seen:
            returnValue(0)

        [(latest, timestamp)] = last_seen
        count = yield self.redis.zcount(self.outbound_key(batch_id),
            timestamp - sample_time, timestamp)
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
        return '%s-%s' % (direction,
            hashlib.md5(json.dumps(ordered_query)).hexdigest())

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

    def get_cache_version(self, batch_id):
        return self.redis.get(self.cache_version_key(batch_id))

    def set_cache_version(self, batch_id, version):
        return self.redis.set(self.cache_version_key(batch_id), version)
