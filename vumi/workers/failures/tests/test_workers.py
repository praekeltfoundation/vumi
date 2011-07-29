import fnmatch
from twisted.trial import unittest

from vumi.tests.utils import get_stubbed_worker
from vumi.workers.failures.workers import Vas2NetsFailureWorker


class FakeRedis(object):
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def exists(self, key):
        return key in self.data

    def keys(self, pattern='*'):
        return fnmatch.filter(self.data.keys(), pattern)

    def flushdb(self):
        self.data = {}


class Vas2NetsFailureWorkerTestCase(unittest.TestCase):

    def setUp(self):
        self.config = {
            'transport_name': 'vas2nets'
        }
        self.worker = get_stubbed_worker(Vas2NetsFailureWorker, self.config)
        self.worker.startWorker()
        # self.worker.r_server = FakeRedis()
        self.worker.r_server.flushdb()

    def test_redis_access(self):
        """
        Sanity check that we can put stuff in redis (or our fake) and
        get it out again.
        """
        self.assertEqual(None, self.worker.r_get("foo"))
        self.assertEqual([], self.worker.r_server.keys())
        self.worker.r_set("foo", "bar")
        self.assertEqual("bar", self.worker.r_get("foo"))
        self.assertEqual(['failures:vas2nets#foo'], self.worker.r_server.keys())

    def test_store_failure(self):
        """
        Store a failure in redis and make sure we can get at it again.
        """
        key = self.worker.store_failure({'message': 'foo'}, "bad stuff happened")
        self.assertEqual(set([key]), self.worker.get_failure_keys())
        self.assertEqual({
                "message": str({"message": "foo"}),
                "retry": "False",
                "reason": "bad stuff happened",
                }, self.worker.r_server.hgetall(key))

    def test_write_timestamp(self):
        def get_write_timestamp(delta, timestamp):
            return self.worker.get_next_write_timestamp(delta, timestamp)

        self.assertEqual("1970-01-01T00:00:05", get_write_timestamp(0, 0))
        self.assertEqual("1970-01-01T00:00:05", get_write_timestamp(0, 4))
        self.assertEqual("1970-01-01T00:00:10", get_write_timestamp(0, 5))
        self.assertEqual("1970-01-01T00:00:10", get_write_timestamp(0, 9))

        self.assertEqual("1970-01-01T00:00:05", get_write_timestamp(2, 0))
        self.assertEqual("1970-01-01T00:00:10", get_write_timestamp(2, 4))
        self.assertEqual("1970-01-01T00:00:10", get_write_timestamp(2, 5))
        self.assertEqual("1970-01-01T00:00:15", get_write_timestamp(2, 9))

        self.assertEqual("1970-01-01T00:03:25", get_write_timestamp(101, 100))
        self.assertEqual("1970-01-01T00:03:30", get_write_timestamp(101, 104))
        self.assertEqual("1970-01-01T00:03:30", get_write_timestamp(101, 105))
        self.assertEqual("1970-01-01T00:03:35", get_write_timestamp(101, 109))

    def test_write_timestamp_granularity(self):
        def get_write_timestamp(delta, timestamp):
            return self.worker.get_next_write_timestamp(delta, timestamp)

        self.worker.GRANULARITY = 10
        self.assertEqual("1970-01-01T00:00:10", get_write_timestamp(0, 0))
        self.assertEqual("1970-01-01T00:00:10", get_write_timestamp(0, 4))
        self.assertEqual("1970-01-01T00:00:10", get_write_timestamp(0, 5))
        self.assertEqual("1970-01-01T00:00:10", get_write_timestamp(0, 9))
        self.assertEqual("1970-01-01T00:00:20", get_write_timestamp(0, 11))

        self.assertEqual("1970-01-01T00:00:20", get_write_timestamp(12, 0))
        self.assertEqual("1970-01-01T00:00:20", get_write_timestamp(12, 4))
        self.assertEqual("1970-01-01T00:00:20", get_write_timestamp(12, 5))
        self.assertEqual("1970-01-01T00:00:30", get_write_timestamp(12, 9))
        self.assertEqual("1970-01-01T00:00:30", get_write_timestamp(12, 11))

        self.worker.GRANULARITY = 3
        self.assertEqual("1970-01-01T00:00:03", get_write_timestamp(0, 0))
        self.assertEqual("1970-01-01T00:00:06", get_write_timestamp(0, 4))
        self.assertEqual("1970-01-01T00:00:06", get_write_timestamp(0, 5))
        self.assertEqual("1970-01-01T00:00:12", get_write_timestamp(0, 9))
        self.assertEqual("1970-01-01T00:00:12", get_write_timestamp(0, 11))

        self.assertEqual("1970-01-01T00:00:15", get_write_timestamp(12, 0))
        self.assertEqual("1970-01-01T00:00:18", get_write_timestamp(12, 4))
        self.assertEqual("1970-01-01T00:00:18", get_write_timestamp(12, 5))
        self.assertEqual("1970-01-01T00:00:24", get_write_timestamp(12, 9))
        self.assertEqual("1970-01-01T00:00:24", get_write_timestamp(12, 11))
