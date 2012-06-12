from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.persist.tests.fake_redis import FakeRedis


class FakeRedisTestCase(TestCase):
    timeout = 1

    def setUp(self):
        self.redis = FakeRedis()

    def tearDown(self):
        self.redis.teardown()

    def assert_redis_op(self, expected, op, *args, **kw):
        self.assertEqual(expected, getattr(self.redis, op)(*args, **kw))

    @inlineCallbacks
    def test_delete(self):
        yield self.redis.set("delete_me", 1)
        yield self.assert_redis_op(True, 'delete', "delete_me")
        yield self.assert_redis_op(False, 'delete', "delete_me")

    @inlineCallbacks
    def test_incr(self):
        yield self.redis.set("inc", 1)
        yield self.assert_redis_op('1', 'get', "inc")
        yield self.assert_redis_op(2, 'incr', "inc")
        yield self.assert_redis_op(3, 'incr', "inc")
        yield self.assert_redis_op('3', 'get', "inc")

    @inlineCallbacks
    def test_setnx(self):
        yield self.assert_redis_op(False, 'exists', "mykey")
        yield self.assert_redis_op(True, 'setnx', "mykey", "value")
        yield self.assert_redis_op("value", 'get', "mykey")
        yield self.assert_redis_op(False, 'setnx', "mykey", "other")
        yield self.assert_redis_op("value", 'get', "mykey")

    @inlineCallbacks
    def test_incr_with_by_param(self):
        yield self.redis.set("inc", 1)
        yield self.assert_redis_op('1', 'get', "inc")
        yield self.assert_redis_op(2, 'incr', "inc", 1)
        yield self.assert_redis_op(4, 'incr', "inc", 2)
        yield self.assert_redis_op(7, 'incr', "inc", 3)
        yield self.assert_redis_op(11, 'incr', "inc", 4)
        yield self.assert_redis_op(111, 'incr', "inc", 100)
        yield self.assert_redis_op('111', 'get', "inc")

    @inlineCallbacks
    def test_zrange(self):
        yield self.redis.zadd('set', one=0.1, two=0.2, three=0.3)
        yield self.assert_redis_op(['one'], 'zrange', 'set', 0, 0)
        yield self.assert_redis_op(['one', 'two'], 'zrange', 'set', 0, 1)
        yield self.assert_redis_op(
            ['one', 'two', 'three'], 'zrange', 'set', 0, 2)
        yield self.assert_redis_op(
            ['one', 'two', 'three'], 'zrange', 'set', 0, 3)
        yield self.assert_redis_op(
            ['one', 'two', 'three'], 'zrange', 'set', 0, -1)
        yield self.assert_redis_op(
            [(0.1, 'one'), (0.2, 'two'), (0.3, 'three')],
            'zrange', 'set', 0, -1, withscores=True)
        yield self.assert_redis_op(
            ['three', 'two', 'one'], 'zrange', 'set', 0, -1, desc=True)
        yield self.assert_redis_op(
            [(0.3, 'three'), (0.2, 'two'), (0.1, 'one')],
            'zrange', 'set', 0, -1, withscores=True, desc=True)

    @inlineCallbacks
    def test_hgetall_returns_copy(self):
        yield self.redis.hset("hash", "foo", "1")
        data = yield self.redis.hgetall("hash")
        data["foo"] = "2"
        yield self.assert_redis_op({"foo": "1"}, 'hgetall', "hash")

    @inlineCallbacks
    def test_hincrby(self):
        yield self.assert_redis_op(1, 'hincrby', "inc", "field1")
        yield self.assert_redis_op(2, 'hincrby', "inc", "field1")
        yield self.assert_redis_op(5, 'hincrby', "inc", "field1", 3)
        yield self.assert_redis_op(7, 'hincrby', "inc", "field1", "2")
        self.assertRaises(
            Exception, self.redis.hincrby, "inc", "field1", "1.5")
        yield self.redis.hset("inc", "field2", "a")
        yield self.assertRaises(Exception, self.redis.hincrby, "inc", "field2")
        yield self.redis.set("key", "string")
        yield self.assertRaises(Exception, self.redis.hincrby, "key", "field1")

    @inlineCallbacks
    def test_hexists(self):
        yield self.redis.hset('key', 'field', 1)
        yield self.assert_redis_op(True, 'hexists', 'key', 'field')
        yield self.redis.hdel('key', 'field')
        yield self.assert_redis_op(False, 'hexists', 'key', 'field')

    @inlineCallbacks
    def test_sadd(self):
        yield self.assert_redis_op(None, 'sadd', 'set', 1)
        yield self.assert_redis_op(None, 'sadd', 'set', 2, 3, 4)
        yield self.assert_redis_op(
            set(['1', '2', '3', '4']), 'smembers', 'set')

    @inlineCallbacks
    def test_smove(self):
        yield self.assert_redis_op(None, 'sadd', 'set1', 1)
        yield self.assert_redis_op(None, 'sadd', 'set2', 2)
        yield self.assert_redis_op(True, 'smove', 'set1', 'set2', '1')
        yield self.assert_redis_op(set(), 'smembers', 'set1')
        yield self.assert_redis_op(set(['1', '2']), 'smembers', 'set2')

        yield self.assert_redis_op(False, 'smove', 'set1', 'set2', '1')
        yield self.assert_redis_op(True, 'smove', 'set2', 'set3', '1')
        yield self.assert_redis_op(set(['2']), 'smembers', 'set2')
        yield self.assert_redis_op(set(['1']), 'smembers', 'set3')

    @inlineCallbacks
    def test_sunion(self):
        yield self.assert_redis_op(None, 'sadd', 'set1', 1)
        yield self.assert_redis_op(None, 'sadd', 'set2', 2)
        yield self.assert_redis_op(set(['1']), 'sunion', 'set1')
        yield self.assert_redis_op(set(['1', '2']), 'sunion', 'set1', 'set2')
        yield self.assert_redis_op(set(), 'sunion', 'other')

    @inlineCallbacks
    def test_lrem(self):
        for i in range(5):
            yield self.assert_redis_op(2 * i, 'rpush', 'list', 'v%d' % i)
            yield self.assert_redis_op(2 * i + 1, 'rpush', 'list', 1)
        yield self.assert_redis_op(5, 'lrem', 'list', 1)
        yield self.assert_redis_op(
            ['v0', 'v1', 'v2', 'v3', 'v4'], 'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_lrem_positive_num(self):
        for i in range(5):
            yield self.assert_redis_op(2 * i, 'rpush', 'list', 'v%d' % i)
            yield self.assert_redis_op(2 * i + 1, 'rpush', 'list', 1)
        yield self.assert_redis_op(2, 'lrem', 'list', 1, 2)
        yield self.assert_redis_op(
            ['v0', 'v1', 'v2', 1, 'v3', 1, 'v4', 1], 'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_lrem_negative_num(self):
        for i in range(5):
            yield self.assert_redis_op(2 * i, 'rpush', 'list', 'v%d' % i)
            yield self.assert_redis_op(2 * i + 1, 'rpush', 'list', 1)
        yield self.assert_redis_op(2, 'lrem', 'list', 1, -2)
        yield self.assert_redis_op(
            ['v0', 1, 'v1', 1, 'v2', 1, 'v3', 'v4'], 'lrange', 'list', 0, -1)


class FakeTxRedisTestCase(FakeRedisTestCase):
    def setUp(self):
        self.redis = FakeRedis(async=True)

    def assert_redis_op(self, expected, op, *args, **kw):
        d = getattr(self.redis, op)(*args, **kw)
        return d.addCallback(lambda r: self.assertEqual(expected, r))
