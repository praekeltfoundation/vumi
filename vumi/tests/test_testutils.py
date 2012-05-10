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
                                                ['one', 'two', 'three'])
        self.assertEqual(self.r_server.zrange('set', 0, -1, withscores=True),
                        [(0.1, 'one'), (0.2, 'two'), (0.3, 'three')])
        self.assertEqual(self.r_server.zrange('set', 0, -1, desc=True),
                        ['three', 'two', 'one'])
        self.assertEqual(self.r_server.zrange('set', 0, -1, desc=True,
            withscores=True), [(0.3, 'three'), (0.2, 'two'), (0.1, 'one')])

    def test_hgetall_returns_copy(self):
        self.r_server = FakeRedis()
        self.r_server.hset("hash", "foo", "1")
        data = self.r_server.hgetall("hash")
        data["foo"] = "2"
        self.assertEqual(self.r_server.hgetall("hash"), {
            "foo": "1",
            })

    def test_hincrby(self):
        self.r_server = FakeRedis()
        hincrby = self.r_server.hincrby
        self.assertEqual(hincrby("inc", "field1"), 1)
        self.assertEqual(hincrby("inc", "field1"), 2)
        self.assertEqual(hincrby("inc", "field1", 3), 5)
        self.assertEqual(hincrby("inc", "field1", "2"), 7)
        self.assertRaises(Exception, hincrby, "inc", "field1", "1.5")
        self.r_server.hset("inc", "field2", "a")
        self.assertRaises(Exception, hincrby, "inc", "field2")
        self.r_server.set("key", "string")
        self.assertRaises(Exception, self.r_server.hincrby, "key", "field1")

    def test_hexists(self):
        self.r_server = FakeRedis()
        self.r_server.hset('key', 'field', 1)
        self.assertTrue(self.r_server.hexists('key', 'field'))
        self.r_server.hdel('key', 'field')
        self.assertFalse(self.r_server.hexists('key', 'field'))

    def test_sadd(self):
        self.r_server = FakeRedis()
        self.r_server.sadd('set', 1)
        self.r_server.sadd('set', 2, 3, 4)
        self.assertEqual(self.r_server.smembers('set'), set([
            '1', '2', '3', '4']))

    def test_smove(self):
        self.r_server = FakeRedis()
        self.r_server.sadd('set1', 1)
        self.r_server.sadd('set2', 2)
        self.assertEqual(self.r_server.smove('set1', 'set2', '1'), True)
        self.assertEqual(self.r_server.smembers('set1'), set())
        self.assertEqual(self.r_server.smembers('set2'), set(['1', '2']))

        self.assertEqual(self.r_server.smove('set1', 'set2', '1'), False)

        self.assertEqual(self.r_server.smove('set2', 'set3', '1'), True)
        self.assertEqual(self.r_server.smembers('set2'), set(['2']))
        self.assertEqual(self.r_server.smembers('set3'), set(['1']))

    def test_sunion(self):
        self.r_server = FakeRedis()
        self.r_server.sadd('set1', 1)
        self.r_server.sadd('set2', 2)
        self.assertEqual(self.r_server.sunion('set1'), set(['1']))
        self.assertEqual(self.r_server.sunion('set1', 'set2'), set(['1', '2']))
        self.assertEqual(self.r_server.sunion('other'), set())

    def test_lrem(self):
        self.r_server = FakeRedis()
        for i in range(5):
            self.r_server.rpush('list', 'v%d' % i)
            self.r_server.rpush('list', 1)
        self.assertEqual(self.r_server.lrem('list', 1), 5)
        self.assertEqual(self.r_server.lrange('list', 0, -1),
                         ['v0', 'v1', 'v2', 'v3', 'v4'])

    def test_lrem_positive_num(self):
        self.r_server = FakeRedis()
        for i in range(5):
            self.r_server.rpush('list', 'v%d' % i)
            self.r_server.rpush('list', 1)
        self.assertEqual(self.r_server.lrem('list', 1, 2), 2)
        self.assertEqual(self.r_server.lrange('list', 0, -1),
                         ['v0', 'v1', 'v2', 1, 'v3', 1, 'v4', 1])

    def test_lrem_negative_num(self):
        self.r_server = FakeRedis()
        for i in range(5):
            self.r_server.rpush('list', 'v%d' % i)
            self.r_server.rpush('list', 1)
        self.assertEqual(self.r_server.lrem('list', 1, -2), 2)
        self.assertEqual(self.r_server.lrange('list', 0, -1),
                         ['v0', 1, 'v1', 1, 'v2', 1, 'v3', 'v4'])
