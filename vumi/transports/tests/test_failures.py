import time
import json
from datetime import datetime, timedelta

from twisted.internet.defer import inlineCallbacks

from vumi.message import Message
from vumi.transports.failures import FailureWorker
from vumi.tests.helpers import VumiTestCase, PersistenceHelper, WorkerHelper


def mktimestamp(delta=0):
    timestamp = datetime.utcnow() + timedelta(seconds=delta)
    return timestamp.isoformat().split('.')[0]


class TestFailureWorker(VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        return self.make_worker()

    @inlineCallbacks
    def make_worker(self, retry_delivery_period=0):
        self.worker_helper = self.add_helper(WorkerHelper('sphex'))
        config = self.persistence_helper.mk_config({
            'transport_name': 'sphex',
            'retry_routing_key': 'sms.outbound.%(transport_name)s',
            'failures_routing_key': 'sms.failures.%(transport_name)s',
            'retry_delivery_period': retry_delivery_period,
        })
        self.worker = yield self.worker_helper.get_worker(
            FailureWorker, config)
        self.redis = self.worker.redis
        yield self.redis._purge_all()  # Just in case

    def assert_write_timestamp(self, expected, delta, now):
        self.assertEqual(expected,
                         self.worker.get_next_write_timestamp(delta, now=now))

    @inlineCallbacks
    def assert_zcard(self, expected, key):
        self.assertEqual(expected, (yield self.redis.zcard(key)))

    @inlineCallbacks
    def assert_equal_d(self, expected, value):
        self.assertEqual((yield expected), (yield value))

    @inlineCallbacks
    def assert_not_equal_d(self, expected, value):
        self.assertNotEqual((yield expected), (yield value))

    @inlineCallbacks
    def assert_get_retry_key(self, exists=True):
        retry_key = yield self.worker.get_next_retry_key()
        if exists:
            self.assertNotEqual(None, retry_key)
        else:
            self.assertEqual(None, retry_key)

    @inlineCallbacks
    def assert_stored_timestamps(self, *expected):
        timestamps = yield self.redis.zrange('retry_timestamps', 0, -1)
        self.assertEqual(list(expected), timestamps)

    def assert_published_retries(self, expected):
        msgs = self.worker_helper.get_dispatched(
            'sms.outbound', 'sphex', Message)
        self.assertEqual(expected, [m.payload for m in msgs])

    def store_failure(self, reason=None, message=None):
        if not reason:
            reason = "bad stuff happened"
        if not message:
            message = {'message': 'foo', 'reason': reason}
        return self.worker.store_failure(message, reason)

    @inlineCallbacks
    def store_retry(self, retry_delay=0, now_delta=0, reason=None,
                    message_json=None):
        key = yield self.store_failure(reason, message_json)
        now = time.time() + now_delta
        yield self.worker.store_retry(key, retry_delay, now=now)

    @inlineCallbacks
    def test_redis_access(self):
        """
        Sanity check that we can put stuff in redis (or our fake) and
        get it out again.
        """

        def r_get(key):
            return self.worker.redis.get(key)

        yield self.assert_equal_d(None, r_get("foo"))
        yield self.assert_equal_d([], self.redis.keys())
        yield self.worker.redis.set("foo", "bar")
        yield self.assert_equal_d("bar", r_get("foo"))
        yield self.assert_equal_d(['foo'], self.redis.keys())

    @inlineCallbacks
    def test_store_failure(self):
        """
        Store a failure in redis and make sure we can get at it again.
        """
        key = yield self.store_failure(reason="reason")
        yield self.assert_equal_d(set([key]), self.worker.get_failure_keys())
        message_json = json.dumps({"message": "foo", "reason": "reason"})
        yield self.assert_equal_d({
                "message": message_json,
                "retry_delay": "0",
                "reason": "reason",
                }, self.redis.hgetall(key))
        # Test a second one, this time with a JSON-encoded message
        key2 = yield self.store_failure(
            message=json.dumps({"foo": "bar"}), reason="reason")
        yield self.assert_equal_d(
            set([key, key2]), self.worker.get_failure_keys())
        message_json = json.dumps({"foo": "bar"})
        yield self.assert_equal_d({
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

    @inlineCallbacks
    def test_store_read_timestamp(self):
        """
        We need to store granular timestamps.
        """
        timestamp1 = "1977-07-28T12:34:56"
        timestamp2 = "1980-07-30T12:34:56"
        timestamp3 = "1980-09-02T12:34:56"
        yield self.assert_stored_timestamps()
        yield self.worker.store_read_timestamp(timestamp2)
        yield self.assert_stored_timestamps(timestamp2)
        yield self.worker.store_read_timestamp(timestamp1)
        yield self.assert_stored_timestamps(timestamp1, timestamp2)
        yield self.worker.store_read_timestamp(timestamp3)
        yield self.assert_stored_timestamps(timestamp1, timestamp2, timestamp3)

    @inlineCallbacks
    def test_read_timestamp(self):
        """
        We need to read the next timestamp.
        """
        past = mktimestamp(-10)
        future = mktimestamp(10)
        yield self.assert_equal_d(None, self.worker.get_next_read_timestamp())
        yield self.worker.store_read_timestamp(future)
        yield self.assert_equal_d(None, self.worker.get_next_read_timestamp())
        yield self.worker.store_read_timestamp(past)
        yield self.assert_equal_d(past, self.worker.get_next_read_timestamp())

    @inlineCallbacks
    def test_store_retry(self):
        """
        Store a retry in redis and make sure we can get at it again.
        """
        timestamp = "1970-01-01T00:00:05"
        retry_key = "retry_keys." + timestamp
        key = yield self.store_failure()
        yield self.assert_zcard(0, 'retry_timestamps')

        yield self.worker.store_retry(key, 0, now=0)
        yield self.assert_zcard(1, 'retry_timestamps')
        yield self.assert_equal_d([timestamp],
                                  self.redis.zrange('retry_timestamps', 0, 0))
        yield self.assert_equal_d(set([key]), self.redis.smembers(retry_key))

    def test_get_retry_key_none(self):
        """
        If there are no stored retries, get None.
        """
        return self.assert_get_retry_key(False)

    @inlineCallbacks
    def test_get_retry_key_future(self):
        """
        If there are no retries due, get None.
        """
        yield self.store_retry(10)
        yield self.assert_zcard(1, 'retry_timestamps')
        yield self.assert_get_retry_key(False)
        yield self.assert_zcard(1, 'retry_timestamps')

    @inlineCallbacks
    def test_get_retry_key_one_due(self):
        """
        Get a retry from redis when we have one due.
        """
        yield self.store_retry(0, -5)
        yield self.assert_zcard(1, 'retry_timestamps')
        yield self.assert_get_retry_key()
        yield self.assert_zcard(0, 'retry_timestamps')
        yield self.assert_get_retry_key(False)

    @inlineCallbacks
    def test_get_retry_key_two_due(self):
        """
        Get a retry from redis when we have two due.
        """
        yield self.store_retry(0, -5)
        yield self.store_retry(0, -5)
        yield self.assert_zcard(1, 'retry_timestamps')
        yield self.assert_get_retry_key()
        yield self.assert_zcard(1, 'retry_timestamps')

    @inlineCallbacks
    def test_get_retry_key_two_due_different_times(self):
        """
        Get a retry from redis when we have two due at different times.
        """
        yield self.store_retry(0, -5)
        yield self.store_retry(0, -15)
        yield self.assert_zcard(2, 'retry_timestamps')
        yield self.assert_get_retry_key()
        yield self.assert_zcard(1, 'retry_timestamps')
        yield self.assert_get_retry_key()
        yield self.assert_zcard(0, 'retry_timestamps')

    @inlineCallbacks
    def test_get_retry_key_one_due_one_future(self):
        """
        Get a retry from redis when we have one due and one in the future.
        """
        yield self.store_retry(0, -5)
        yield self.worker.store_retry(self.store_failure(), 0)
        yield self.assert_zcard(2, 'retry_timestamps')
        yield self.assert_get_retry_key()
        yield self.assert_zcard(1, 'retry_timestamps')
        yield self.assert_get_retry_key(False)
        yield self.assert_zcard(1, 'retry_timestamps')

    @inlineCallbacks
    def test_deliver_retries_none(self):
        """
        Delivering no retries should do nothing.
        """
        yield self.worker.deliver_retries()
        self.assert_published_retries([])

    @inlineCallbacks
    def test_deliver_retries_future(self):
        """
        Delivering no current retries should do nothing.
        """
        yield self.worker.store_retry(self.store_failure(), 0)
        yield self.worker.deliver_retries()
        self.assert_published_retries([])

    @inlineCallbacks
    def test_deliver_retries_one_due(self):
        """
        Delivering a current retry should deliver one message.
        """
        yield self.store_retry(0, -5)
        yield self.worker.deliver_retries()
        self.assert_published_retries([{
                    'message': 'foo',
                    'reason': 'bad stuff happened',
                    }])

    @inlineCallbacks
    def test_deliver_retries_many_due(self):
        """
        Delivering current retries should deliver all messages.
        """
        yield self.store_retry(0, -5)
        yield self.store_retry(0, -15)
        yield self.store_retry(0, -5)
        yield self.worker.deliver_retries()
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
        yield self.worker.stopWorker()
        yield self.make_worker(1)
        self.assertEqual(self.worker.deliver_retries,
                         self.worker.delivery_loop.f)
        self.assertTrue(self.worker.delivery_loop.running)
