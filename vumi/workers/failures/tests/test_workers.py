import fnmatch
from twisted.trial import unittest

from vumi.tests.utils import get_stubbed_worker
from vumi.workers.failures.workers import Vas2NetsFailureWorker

from vumi.utils import get_deploy_int


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
        self.worker.r_server = FakeRedis()
        self.worker.r_server.flushdb()

    def test_redis_access(self):
        self.assertEqual(None, self.worker.r_get("foo"))
        self.assertEqual([], self.worker.r_server.keys())
        self.worker.r_set("foo", "bar")
        self.assertEqual("bar", self.worker.r_get("foo"))
        self.assertEqual(['failures:vas2nets#foo'], self.worker.r_server.keys())

