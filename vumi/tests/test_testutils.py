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


class FakeRedisTestCase(TestCase):

    def test_delete(self):
        self.r_server = FakeRedis()
        self.r_server.set("delete_me", 1)
        self.assertEqual(True, self.r_server.delete("delete_me"))
        self.assertEqual(False, self.r_server.delete("delete_me"))

    def test_incr(self):
        self.r_server = FakeRedis()
        self.r_server.set("inc", 1)
        self.assertEqual('1', self.r_server.get("inc"))
        self.assertEqual(2, self.r_server.incr("inc"))
        self.assertEqual(3, self.r_server.incr("inc"))
        self.assertEqual('3', self.r_server.get("inc"))

    def test_incr_with_by_param(self):
        self.r_server = FakeRedis()
        self.r_server.set("inc", 1)
        self.assertEqual('1', self.r_server.get("inc"))
        self.assertEqual(2, self.r_server.incr("inc", 1))
        self.assertEqual(4, self.r_server.incr("inc", 2))
        self.assertEqual(7, self.r_server.incr("inc", 3))
        self.assertEqual(11, self.r_server.incr("inc", 4))
        self.assertEqual(111, self.r_server.incr("inc", 100))
        self.assertEqual('111', self.r_server.get("inc"))

    def test_zrange(self):
        self.r_server = FakeRedis()
        self.r_server.zadd('set', one=0.1, two=0.2, three=0.3)
        self.assertEqual(self.r_server.zrange('set', 0, 0), ['one'])
        self.assertEqual(self.r_server.zrange('set', 0, 1), ['one', 'two'])
        self.assertEqual(self.r_server.zrange('set', 0, 2),
            ['one', 'two', 'three'])
        self.assertEqual(self.r_server.zrange('set', 0, 3),
            ['one', 'two', 'three'])
        self.assertEqual(self.r_server.zrange('set', 0, -1),
                        ['one', 'two','three'])
        self.assertEqual(self.r_server.zrange('set', 0, -1, withscores=True),
                        [(0.1, 'one'), (0.2, 'two'), (0.3, 'three')])
        self.assertEqual(self.r_server.zrange('set', 0, -1, desc=True),
                        ['three', 'two','one'])
        self.assertEqual(self.r_server.zrange('set', 0, -1, desc=True,
            withscores=True), [(0.3, 'three'), (0.2, 'two'), (0.1, 'one')])
