import time
import json
import fnmatch
from datetime import datetime, timedelta

from twisted.trial import unittest

from vumi.tests.utils import get_stubbed_worker
from vumi.workers.failures.workers import FailureWorker


RETRY_TIMESTAMPS_KEY = "failures:vas2nets#retry_timestamps"


class FakeRedis(object):
    def __init__(self):
        self._data = {}

    # Global operations

    def exists(self, key):
        return key in self._data

    def keys(self, pattern='*'):
        return fnmatch.filter(self._data.keys(), pattern)

    def flushdb(self):
        self._data = {}

    # String operations

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value):
        self._data[key] = value

    # Hash operations

    def hmset(self, key, mapping):
        hval = self._data.setdefault(key, {})
        hval.update(mapping)

    def hgetall(self, key):
        return self._data.get(key, {})

    # Set operations

    def sadd(self, key, value):
        sval = self._data.setdefault(key, set())
        sval.add(value)

    def smembers(self, key):
        return self._data.get(key, set())

    def spop(self, key):
        sval = self._data.get(key, set())
        if not sval:
            return None
        return sval.pop()

    def scard(self, key):
        return len(self._data.get(key, set()))

    # Sorted set operations

    def zadd(self, key, **valscores):
        zval = self._data.setdefault(key, [])
        new_zval = [val for val in zval if val[1] not in valscores]
        for value, score in valscores.items():
            new_zval.append((score, value))
        new_zval.sort()
        self._data[key] = new_zval

    def zrem(self, key, value):
        zval = self._data.setdefault(key, [])
        new_zval = [val for val in zval if val[1] != value]
        self._data[key] = new_zval

    def zcard(self, key):
        return len(self._data.get(key, []))

    def zrange(self, key, start, stop):
        zval = self._data.get(key, [])
        stop += 1  # redis start/stop are element indexes
        if stop == 0:
            stop = None
        return [val[1] for val in zval[start:stop]]


def mktimestamp(delta=0):
    timestamp = datetime.utcnow() + timedelta(seconds=delta)
    return timestamp.isoformat().split('.')[0]


class FailureWorkerTestCase(unittest.TestCase):

    def setUp(self):
        self.config = {
            'transport_name': 'vas2nets'
        }
        self.worker = get_stubbed_worker(FailureWorker, self.config)
        self.worker.startWorker()
        self.worker.r_server = FakeRedis()
        self.worker.r_server.flushdb()
        self.redis = self.worker.r_server

    def assert_write_timestamp(self, expected, delta, now):
        self.assertEqual(expected,
                         self.worker.get_next_write_timestamp(delta, now=now))

    def assert_zcard(self, expected, key):
        self.assertEqual(expected, self.redis.zcard(key))

    def assert_stored_timestamps(self, *expected):
        self.assertEqual(list(expected),
                         self.redis.zrange(RETRY_TIMESTAMPS_KEY, 0, -1))

    def store_failure(self, reason=None, message_json=None):
        if not reason:
            reason = "bad stuff happened"
        if not message_json:
            message_json = json.dumps({'message': 'foo', 'reason': reason})
        return self.worker.store_failure(message_json, reason)

    def test_redis_access(self):
        """
        Sanity check that we can put stuff in redis (or our fake) and
        get it out again.
        """
        self.assertEqual(None, self.worker.r_get("foo"))
        self.assertEqual([], self.redis.keys())
        self.worker.r_set("foo", "bar")
        self.assertEqual("bar", self.worker.r_get("foo"))
        self.assertEqual(['failures:vas2nets#foo'], self.redis.keys())

    def test_store_failure(self):
        """
        Store a failure in redis and make sure we can get at it again.
        """
        key = self.store_failure(reason="reason")
        self.assertEqual(set([key]), self.worker.get_failure_keys())
        message_json = json.dumps({"message": "foo", "reason": "reason"})
        self.assertEqual({
                "message": message_json,
                "retry_delay": "0",
                "reason": "reason",
                }, self.redis.hgetall(key))

    def test_write_timestamp(self):
        """
        We need granular timestamps.
        """
        start = datetime.utcnow().isoformat()
        timestamp = self.worker.get_next_write_timestamp(0)
        end = (datetime.utcnow() + timedelta(seconds=6)).isoformat()
        self.assertTrue(start < timestamp < end)

        self.assert_write_timestamp("1970-01-01T00:00:05", 0, 0)
        self.assert_write_timestamp("1970-01-01T00:00:05", 0, 4)
        self.assert_write_timestamp("1970-01-01T00:00:10", 0, 5)
        self.assert_write_timestamp("1970-01-01T00:00:10", 0, 9)

        self.assert_write_timestamp("1970-01-01T00:00:05", 2, 0)
        self.assert_write_timestamp("1970-01-01T00:00:10", 2, 4)
        self.assert_write_timestamp("1970-01-01T00:00:10", 2, 5)
        self.assert_write_timestamp("1970-01-01T00:00:15", 2, 9)

        self.assert_write_timestamp("1970-01-01T00:03:25", 101, 100)
        self.assert_write_timestamp("1970-01-01T00:03:30", 101, 104)
        self.assert_write_timestamp("1970-01-01T00:03:30", 101, 105)
        self.assert_write_timestamp("1970-01-01T00:03:35", 101, 109)

    def test_write_timestamp_granularity(self):
        """
        We need granular timestamps with assorted granularity.
        """
        self.worker.GRANULARITY = 10
        self.assert_write_timestamp("1970-01-01T00:00:10", 0, 0)
        self.assert_write_timestamp("1970-01-01T00:00:10", 0, 4)
        self.assert_write_timestamp("1970-01-01T00:00:10", 0, 5)
        self.assert_write_timestamp("1970-01-01T00:00:10", 0, 9)
        self.assert_write_timestamp("1970-01-01T00:00:20", 0, 11)

        self.assert_write_timestamp("1970-01-01T00:00:20", 12, 0)
        self.assert_write_timestamp("1970-01-01T00:00:20", 12, 4)
        self.assert_write_timestamp("1970-01-01T00:00:20", 12, 5)
        self.assert_write_timestamp("1970-01-01T00:00:30", 12, 9)
        self.assert_write_timestamp("1970-01-01T00:00:30", 12, 11)

        self.worker.GRANULARITY = 3
        self.assert_write_timestamp("1970-01-01T00:00:03", 0, 0)
        self.assert_write_timestamp("1970-01-01T00:00:06", 0, 4)
        self.assert_write_timestamp("1970-01-01T00:00:06", 0, 5)
        self.assert_write_timestamp("1970-01-01T00:00:12", 0, 9)
        self.assert_write_timestamp("1970-01-01T00:00:12", 0, 11)

        self.assert_write_timestamp("1970-01-01T00:00:15", 12, 0)
        self.assert_write_timestamp("1970-01-01T00:00:18", 12, 4)
        self.assert_write_timestamp("1970-01-01T00:00:18", 12, 5)
        self.assert_write_timestamp("1970-01-01T00:00:24", 12, 9)
        self.assert_write_timestamp("1970-01-01T00:00:24", 12, 11)

    def test_store_read_timestamp(self):
        """
        We need to store granular timestamps.
        """
        timestamp1 = "1977-07-28T12:34:56"
        timestamp2 = "1980-07-30T12:34:56"
        timestamp3 = "1980-09-02T12:34:56"
        self.assert_stored_timestamps()
        self.worker.store_read_timestamp(timestamp2)
        self.assert_stored_timestamps(timestamp2)
        self.worker.store_read_timestamp(timestamp1)
        self.assert_stored_timestamps(timestamp1, timestamp2)
        self.worker.store_read_timestamp(timestamp3)
        self.assert_stored_timestamps(timestamp1, timestamp2, timestamp3)

    def test_read_timestamp(self):
        """
        We need to read the next timestamp.
        """
        past = mktimestamp(-10)
        future = mktimestamp(10)
        self.assertEqual(None, self.worker.get_next_read_timestamp())
        self.worker.store_read_timestamp(future)
        self.assertEqual(None, self.worker.get_next_read_timestamp())
        self.worker.store_read_timestamp(past)
        self.assertEqual(past, self.worker.get_next_read_timestamp())

    def test_store_retry(self):
        """
        Store a retry in redis and make sure we can get at it again.
        """
        timestamp = "1970-01-01T00:00:05"
        retry_key = "failures:vas2nets#retry_keys." + timestamp
        key = self.store_failure()
        self.assert_zcard(0, RETRY_TIMESTAMPS_KEY)

        self.worker.store_retry(key, 0, now=0)
        self.assert_zcard(1, RETRY_TIMESTAMPS_KEY)
        self.assertEqual([timestamp],
                         self.redis.zrange(RETRY_TIMESTAMPS_KEY, 0, 0))
        self.assertEqual(set([key]), self.redis.smembers(retry_key))

    def test_get_retry_key_none(self):
        """
        If there are no stored retries, get None.
        """
        self.assertEqual(None, self.worker.get_next_retry_key())

    def test_get_retry_key_future(self):
        """
        If there are no retries due, get None.
        """
        self.worker.store_retry(self.store_failure(), 10)
        self.assert_zcard(1, RETRY_TIMESTAMPS_KEY)
        self.assertEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(1, RETRY_TIMESTAMPS_KEY)

    def test_get_retry_key_one_due(self):
        """
        Get a retry from redis when we have one due.
        """
        self.worker.store_retry(self.store_failure(), 0, now=time.time() - 5)
        self.assert_zcard(1, RETRY_TIMESTAMPS_KEY)
        self.assertNotEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(0, RETRY_TIMESTAMPS_KEY)
        self.assertEqual(None, self.worker.get_next_retry_key())

    def test_get_retry_key_two_due(self):
        """
        Get a retry from redis when we have two due.
        """
        self.worker.store_retry(self.store_failure(), 0, now=time.time() - 5)
        self.worker.store_retry(self.store_failure(), 0, now=time.time() - 5)
        self.assert_zcard(1, RETRY_TIMESTAMPS_KEY)
        self.assertNotEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(1, RETRY_TIMESTAMPS_KEY)

    def test_get_retry_key_two_due_different_times(self):
        """
        Get a retry from redis when we have two due at different times.
        """
        self.worker.store_retry(self.store_failure(), 0, now=time.time() - 5)
        self.worker.store_retry(self.store_failure(), 0, now=time.time() - 15)
        self.assert_zcard(2, RETRY_TIMESTAMPS_KEY)
        self.assertNotEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(1, RETRY_TIMESTAMPS_KEY)
        self.assertNotEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(0, RETRY_TIMESTAMPS_KEY)

    def test_get_retry_key_one_due_one_future(self):
        """
        Get a retry from redis when we have one due and one in the future.
        """
        self.worker.store_retry(self.store_failure(), 0, now=time.time() - 5)
        self.worker.store_retry(self.store_failure(), 0)
        self.assert_zcard(2, RETRY_TIMESTAMPS_KEY)
        self.assertNotEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(1, RETRY_TIMESTAMPS_KEY)
        self.assertEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(1, RETRY_TIMESTAMPS_KEY)
