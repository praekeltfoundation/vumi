from twisted.trial.unittest import TestCase

from vumi.service import Worker
from vumi.tests.utils import get_stubbed_worker, FakeRedis
from vumi.tests.fake_amqp import FakeAMQClient


class ToyWorker(Worker):
    def poke(self):
        return "poke"


class UtilsTestCase(TestCase):

    def test_get_stubbed_worker(self):
        worker = get_stubbed_worker(ToyWorker)
        self.assertEqual("poke", worker.poke())
        self.assertTrue(isinstance(worker._amqp_client, FakeAMQClient))

    def test_get_stubbed_worker_with_config(self):
        options = {'key': 'value'}
        worker = get_stubbed_worker(ToyWorker, options)
        self.assertEqual({}, worker._amqp_client.vumi_options)
        self.assertEqual(options, worker.config)


class FakeRedisIncrTestCase(TestCase):

    def test_incr(self):
        self.r_server = FakeRedis()
        self.r_server.set("inc", 1)
        self.assertEqual('1', self.r_server.get("inc"))
        self.assertEqual('2', self.r_server.incr("inc"))
        self.assertEqual('3', self.r_server.incr("inc"))


    def test_incrby(self):
        self.r_server = FakeRedis()
        self.r_server.set("inc", 1)
        self.assertEqual('1', self.r_server.get("inc"))
        self.assertEqual('2', self.r_server.incrby("inc", 1))
        self.assertEqual('4', self.r_server.incrby("inc", 2))
        self.assertEqual('7', self.r_server.incrby("inc", 3))
        self.assertEqual('11', self.r_server.incrby("inc", 4))
        self.assertEqual('111', self.r_server.incrby("inc", 100))


