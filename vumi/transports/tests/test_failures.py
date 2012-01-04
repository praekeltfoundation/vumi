import time
import json
from datetime import datetime, timedelta

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks

from vumi.tests.utils import get_stubbed_worker, FakeRedis
from vumi.transports.failures import FailureWorker


def mktimestamp(delta=0):
    timestamp = datetime.utcnow() + timedelta(seconds=delta)
    return timestamp.isoformat().split('.')[0]


class FailureWorkerTestCase(unittest.TestCase):

    def setUp(self):
        return self.make_worker()

    def tearDown(self):
        return self.worker.stopWorker()

    @inlineCallbacks
    def make_worker(self, retry_delivery_period=0):
        self.config = {
            'transport_name': 'sphex',
            'retry_routing_key': 'sms.outbound.%(transport_name)s',
            'failures_routing_key': 'sms.failures.%(transport_name)s',
            'retry_delivery_period': retry_delivery_period,
        }
        self.worker = get_stubbed_worker(FailureWorker, self.config)
        self.worker.r_server = FakeRedis()
        self.worker.r_server.flushdb()
        self.redis = self.worker.r_server
        yield self.worker.startWorker()
        self.broker = self.worker._amqp_client.broker
        self.retry_timestamps_key = self.worker._retry_timestamps_key

    def assert_write_timestamp(self, expected, delta, now):
        self.assertEqual(expected,
                         self.worker.get_next_write_timestamp(delta, now=now))

    def assert_zcard(self, expected, key):
        self.assertEqual(expected, self.redis.zcard(key))

    def assert_stored_timestamps(self, *expected):
        self.assertEqual(list(expected),
                         self.redis.zrange(self.retry_timestamps_key, 0, -1))

    def assert_published_retries(self, expected):
        msgs = self.broker.get_dispatched('vumi', 'sms.outbound.sphex')
        self.assertEqual(expected, [json.loads(m.body) for m in msgs])

    def store_failure(self, reason=None, message=None):
        if not reason:
            reason = "bad stuff happened"
        if not message:
            message = {'message': 'foo', 'reason': reason}
        return self.worker.store_failure(message, reason)

    def store_retry(self, retry_delay=0, now_delta=0, reason=None,
                    message_json=None):
        key = self.store_failure(reason, message_json)
        now = time.time() + now_delta
        self.worker.store_retry(key, retry_delay, now=now)

    def test_redis_access(self):
        """
        Sanity check that we can put stuff in redis (or our fake) and
        get it out again.
        """

        def r_get(key):
            return self.worker.r_server.get(self.worker.r_key(key))

        self.assertEqual(None, r_get("foo"))
        self.assertEqual([], self.redis.keys())
        self.worker.r_server.set(self.worker.r_key("foo"), "bar")
        self.assertEqual("bar", r_get("foo"))
        self.assertEqual(['failures:sphex#foo'], self.redis.keys())

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
        # Test a second one, this time with a JSON-encoded message
        key2 = self.store_failure(message=json.dumps({"foo": "bar"}),
                                  reason="reason")
        self.assertEqual(set([key, key2]), self.worker.get_failure_keys())
        message_json = json.dumps({"foo": "bar"})
        self.assertEqual({
                "message": message_json,
                "retry_delay": "0",
                "reason": "reason",
                }, self.redis.hgetall(key2))

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
        retry_key = "failures:sphex#retry_keys." + timestamp
        key = self.store_failure()
        self.assert_zcard(0, self.retry_timestamps_key)

        self.worker.store_retry(key, 0, now=0)
        self.assert_zcard(1, self.retry_timestamps_key)
        self.assertEqual([timestamp],
                         self.redis.zrange(self.retry_timestamps_key, 0, 0))
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
        self.store_retry(10)
        self.assert_zcard(1, self.retry_timestamps_key)
        self.assertEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(1, self.retry_timestamps_key)

    def test_get_retry_key_one_due(self):
        """
        Get a retry from redis when we have one due.
        """
        self.store_retry(0, -5)
        self.assert_zcard(1, self.retry_timestamps_key)
        self.assertNotEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(0, self.retry_timestamps_key)
        self.assertEqual(None, self.worker.get_next_retry_key())

    def test_get_retry_key_two_due(self):
        """
        Get a retry from redis when we have two due.
        """
        self.store_retry(0, -5)
        self.store_retry(0, -5)
        self.assert_zcard(1, self.retry_timestamps_key)
        self.assertNotEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(1, self.retry_timestamps_key)

    def test_get_retry_key_two_due_different_times(self):
        """
        Get a retry from redis when we have two due at different times.
        """
        self.store_retry(0, -5)
        self.store_retry(0, -15)
        self.assert_zcard(2, self.retry_timestamps_key)
        self.assertNotEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(1, self.retry_timestamps_key)
        self.assertNotEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(0, self.retry_timestamps_key)

    def test_get_retry_key_one_due_one_future(self):
        """
        Get a retry from redis when we have one due and one in the future.
        """
        self.store_retry(0, -5)
        self.worker.store_retry(self.store_failure(), 0)
        self.assert_zcard(2, self.retry_timestamps_key)
        self.assertNotEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(1, self.retry_timestamps_key)
        self.assertEqual(None, self.worker.get_next_retry_key())
        self.assert_zcard(1, self.retry_timestamps_key)

    def test_deliver_retries_none(self):
        """
        Delivering no retries should do nothing.
        """
        self.worker.deliver_retries()
        self.assert_published_retries([])

    def test_deliver_retries_future(self):
        """
        Delivering no current retries should do nothing.
        """
        self.worker.store_retry(self.store_failure(), 0)
        self.worker.deliver_retries()
        self.assert_published_retries([])

    def test_deliver_retries_one_due(self):
        """
        Delivering a current retry should deliver one message.
        """
        self.store_retry(0, -5)
        self.worker.deliver_retries()
        self.assert_published_retries([{
                    'message': 'foo',
                    'reason': 'bad stuff happened',
                    }])

    def test_deliver_retries_many_due(self):
        """
        Delivering current retries should deliver all messages.
        """
        self.store_retry(0, -5)
        self.store_retry(0, -15)
        self.store_retry(0, -5)
        self.worker.deliver_retries()
        self.assert_published_retries([{
                    'message': 'foo',
                    'reason': 'bad stuff happened',
                    }] * 3)

    def test_update_retry_metadata(self):
        """
        Retry metadata should be updated as appropriate.
        """
        def mkmsg(retries, delay):
            return {'retry_metadata': {'retries': retries, 'delay': delay}}

        def assert_update_rmd(retries, delay, msg):
            msg = self.worker.update_retry_metadata(msg)
            self.assertEqual({'retries': retries, 'delay': delay},
                             msg['retry_metadata'])

        assert_update_rmd(1, 1, {})
        assert_update_rmd(2, 3, mkmsg(1, 1))
        assert_update_rmd(3, 9, mkmsg(2, 3))

    @inlineCallbacks
    def test_start_retrying(self):
        """
        The retry publisher should start when configured appropriately.
        """
        self.assertEqual(None, self.worker.delivery_loop)
        yield self.make_worker(1)
        self.assertEqual(self.worker.deliver_retries,
                         self.worker.delivery_loop.f)
        self.assertTrue(self.worker.delivery_loop.running)
        yield self.worker.stopWorker()
