# -*- coding: utf-8 -*-
from twisted.internet.defer import inlineCallbacks

from vumi.persist.fake_redis import FakeRedis
from vumi.tests.helpers import VumiTestCase


class TestFakeRedis(VumiTestCase):

    def setUp(self):
        self.redis = FakeRedis()
        self.add_cleanup(self.redis.teardown)

    def assert_redis_op(self, expected, op, *args, **kw):
        self.assertEqual(expected, getattr(self.redis, op)(*args, **kw))

    def assert_error(self, func, *args, **kw):
        self.assertRaises(Exception, func, *args, **kw)

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
    def test_incrby(self):
        yield self.redis.set("inc", 1)
        yield self.assert_redis_op('1', 'get', "inc")
        yield self.assert_redis_op(3, 'incr', "inc", 2)
        yield self.assert_redis_op('3', 'get', "inc")

    @inlineCallbacks
    def test_decr(self):
        yield self.redis.set("dec", 4)
        yield self.assert_redis_op('4', 'get', "dec")
        yield self.assert_redis_op(3, 'decr', "dec")
        yield self.assert_redis_op(2, 'decr', "dec")
        yield self.assert_redis_op('2', 'get', "dec")

    @inlineCallbacks
    def test_decrby(self):
        yield self.redis.set("dec", 4)
        yield self.assert_redis_op('4', 'get', "dec")
        yield self.assert_redis_op(2, 'decr', "dec", 2)
        yield self.assert_redis_op('2', 'get', "dec")

    @inlineCallbacks
    def test_setnx(self):
        yield self.assert_redis_op(False, 'exists', "mykey")
        yield self.assert_redis_op(True, 'setnx', "mykey", "value")
        yield self.assert_redis_op("value", 'get', "mykey")
        yield self.assert_redis_op(False, 'setnx', "mykey", "other")
        yield self.assert_redis_op("value", 'get', "mykey")

    @inlineCallbacks
    def test_setex(self):
        yield self.assert_redis_op(False, 'exists', "mykey")
        yield self.assert_redis_op(True, 'setex', "mykey", 10, "value")
        yield self.assert_redis_op("value", 'get', "mykey")
        yield self.assert_redis_op(9, 'ttl', "mykey")

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
    def test_zadd(self):
        yield self.assert_redis_op(1, 'zadd', 'set', one=1.0)
        yield self.assert_redis_op(0, 'zadd', 'set', one=2.0)
        yield self.assert_redis_op([('one', 2.0)], 'zrange', 'set', 0, -1,
                                   withscores=True)
        yield self.assert_error(self.redis.zadd, "set", one='foo')
        yield self.assert_error(self.redis.zadd, "set", one=None)

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
            [('one', 0.1), ('two', 0.2), ('three', 0.3)],
            'zrange', 'set', 0, -1, withscores=True)
        yield self.assert_redis_op(
            ['three', 'two', 'one'], 'zrange', 'set', 0, -1, desc=True)
        yield self.assert_redis_op(
            [('three', 0.3), ('two', 0.2), ('one', 0.1)],
            'zrange', 'set', 0, -1, withscores=True, desc=True)
        yield self.assert_redis_op([('three', 0.3)],
            'zrange', 'set', 0, 0, withscores=True, desc=True)

    @inlineCallbacks
    def test_zrangebyscore(self):
        yield self.redis.zadd('set', one=0.1, two=0.2, three=0.3, four=0.4,
            five=0.5)
        yield self.assert_redis_op(['two', 'three', 'four'], 'zrangebyscore',
            'set', 0.2, 0.4)
        yield self.assert_redis_op(['two', 'three'], 'zrangebyscore',
            'set', 0.2, 0.4, 0, 2)
        yield self.assert_redis_op(['three'], 'zrangebyscore',
            'set', '(0.2', '(0.4')
        yield self.assert_redis_op(['two', 'three', 'four', 'five'],
            'zrangebyscore', 'set', '0.2', '+inf')
        yield self.assert_redis_op(['one', 'two'],
            'zrangebyscore', 'set', '-inf', '0.2')

    @inlineCallbacks
    def test_zcount(self):
        yield self.redis.zadd('set', one=0.1, two=0.2, three=0.3, four=0.4,
            five=0.5)
        yield self.assert_redis_op('3', 'zcount',
            'set', 0.2, 0.4)

    @inlineCallbacks
    def test_zrangebyscore_with_scores(self):
        yield self.redis.zadd('set', one=0.1, two=0.2, three=0.3, four=0.4,
            five=0.5)
        yield self.assert_redis_op(
            [('two', 0.2), ('three', 0.3), ('four', 0.4)],
            'zrangebyscore', 'set', 0.2, 0.4, withscores=True)

    @inlineCallbacks
    def test_zcard(self):
        yield self.assert_redis_op(0, 'zcard', 'set')
        yield self.redis.zadd('set', one=0.1, two=0.2)
        yield self.assert_redis_op(2, 'zcard', 'set')
        yield self.redis.zadd('set', three=0.3)
        yield self.assert_redis_op(3, 'zcard', 'set')

    @inlineCallbacks
    def test_zrem(self):
        yield self.redis.zadd('set', one=0.1, two=0.2)
        yield self.assert_redis_op(True, 'zrem', 'set', 'one')
        yield self.assert_redis_op(False, 'zrem', 'set', 'one')
        yield self.assert_redis_op(
            [('two', 0.2)], 'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank(self):
        yield self.redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(2, 'zremrangebyrank', 'set', 0, 1)
        yield self.assert_redis_op(
            [('three', 3)], 'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank_empty_range(self):
        yield self.redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(0, 'zremrangebyrank', 'set', 10, 11)
        yield self.assert_redis_op(
            [('one', 1), ('two', 2), ('three', 3)],
            'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank_negative_empty_range(self):
        yield self.redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(0, 'zremrangebyrank', 'set', -11, -10)
        yield self.assert_redis_op(
            [('one', 1), ('two', 2), ('three', 3)],
            'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank_negative_start(self):
        yield self.redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(2, 'zremrangebyrank', 'set', -2, 2)
        yield self.assert_redis_op(
            [('one', 1)], 'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank_negative_start_empty_range(self):
        yield self.redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(0, 'zremrangebyrank', 'set', -1, 1)
        yield self.assert_redis_op(
            [('one', 1), ('two', 2), ('three', 3)],
            'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank_negative_stop(self):
        yield self.redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(2, 'zremrangebyrank', 'set', 1, -1)
        yield self.assert_redis_op(
            [('one', 1)], 'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank_negative_stop_empty_range(self):
        yield self.redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(0, 'zremrangebyrank', 'set', 0, -5)
        yield self.assert_redis_op(
            [('one', 1), ('two', 2), ('three', 3)],
            'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zscore(self):
        yield self.redis.zadd('set', one=0.1, two=0.2)
        yield self.assert_redis_op(0.1, 'zscore', 'set', 'one')
        yield self.assert_redis_op(0.2, 'zscore', 'set', 'two')

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
        yield self.assert_error(self.redis.hincrby, "inc", "field1", "1.5")
        yield self.redis.hset("inc", "field2", "a")
        yield self.assert_error(self.redis.hincrby, "inc", "field2")
        yield self.redis.set("key", "string")
        yield self.assert_error(self.redis.hincrby, "key", "field1")

    @inlineCallbacks
    def test_hexists(self):
        yield self.redis.hset('key', 'field', 1)
        yield self.assert_redis_op(True, 'hexists', 'key', 'field')
        yield self.redis.hdel('key', 'field')
        yield self.assert_redis_op(False, 'hexists', 'key', 'field')

    @inlineCallbacks
    def test_hsetnx(self):
        yield self.redis.hset('key', 'field', 1)
        self.assert_redis_op(0, 'hsetnx', 'key', 'field', 2)
        self.assertEqual((yield self.redis.hget('key', 'field')), '1')
        self.assert_redis_op(1, 'hsetnx', 'key', 'other-field', 2)
        self.assertEqual((yield self.redis.hget('key', 'other-field')), '2')

    @inlineCallbacks
    def test_sadd(self):
        yield self.assert_redis_op(1, 'sadd', 'set', 1)
        yield self.assert_redis_op(3, 'sadd', 'set', 2, 3, 4)
        yield self.assert_redis_op(
            set(['1', '2', '3', '4']), 'smembers', 'set')

    @inlineCallbacks
    def test_smove(self):
        yield self.assert_redis_op(1, 'sadd', 'set1', 1)
        yield self.assert_redis_op(1, 'sadd', 'set2', 2)
        yield self.assert_redis_op(True, 'smove', 'set1', 'set2', '1')
        yield self.assert_redis_op(set(), 'smembers', 'set1')
        yield self.assert_redis_op(set(['1', '2']), 'smembers', 'set2')

        yield self.assert_redis_op(False, 'smove', 'set1', 'set2', '1')
        yield self.assert_redis_op(True, 'smove', 'set2', 'set3', '1')
        yield self.assert_redis_op(set(['2']), 'smembers', 'set2')
        yield self.assert_redis_op(set(['1']), 'smembers', 'set3')

    @inlineCallbacks
    def test_sunion(self):
        yield self.assert_redis_op(1, 'sadd', 'set1', 1)
        yield self.assert_redis_op(1, 'sadd', 'set2', 2)
        yield self.assert_redis_op(set(['1']), 'sunion', 'set1')
        yield self.assert_redis_op(set(['1', '2']), 'sunion', 'set1', 'set2')
        yield self.assert_redis_op(set(), 'sunion', 'other')

    @inlineCallbacks
    def test_rpop(self):
        yield self.redis.lpush('key', 1)
        yield self.redis.lpush('key', 2)
        yield self.redis.lpush('key', 3)
        yield self.assert_redis_op(1, 'rpop', 'key')
        yield self.assert_redis_op(2, 'rpop', 'key')
        yield self.assert_redis_op(3, 'rpop', 'key')
        yield self.assert_redis_op(None, 'rpop', 'key')

    @inlineCallbacks
    def test_rpoplpush(self):
        yield self.redis.lpush('source', 1)
        yield self.redis.lpush('source', 2)
        yield self.redis.lpush('source', 3)
        yield self.assert_redis_op(1, 'rpoplpush', 'source', 'destination')
        yield self.assert_redis_op(2, 'rpoplpush', 'source', 'destination')
        yield self.assert_redis_op(3, 'rpoplpush', 'source', 'destination')
        yield self.assert_redis_op(None, 'rpop', 'source')
        yield self.assert_redis_op(1, 'rpop', 'destination')
        yield self.assert_redis_op(2, 'rpop', 'destination')
        yield self.assert_redis_op(3, 'rpop', 'destination')
        yield self.assert_redis_op(None, 'rpop', 'destination')

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
    def test_ltrim(self):
        for i in range(1, 5):
            yield self.assert_redis_op(i - 1, 'rpush', 'list', str(i))
        yield self.assert_redis_op(
            ['1', '2', '3', '4'], 'lrange', 'list', 0, -1)
        yield self.assert_redis_op(None, 'ltrim', 'list', 1, 2)
        yield self.assert_redis_op(['2', '3'], 'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_ltrim_mid_range(self):
        for i in range(1, 6):
            yield self.assert_redis_op(i - 1, 'rpush', 'list', str(i))
        yield self.assert_redis_op(
            ['1', '2', '3', '4', '5'], 'lrange', 'list', 0, -1)
        yield self.assert_redis_op(None, 'ltrim', 'list', 2, 3)
        yield self.assert_redis_op(['3', '4'], 'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_ltrim_keep_all(self):
        for i in range(1, 4):
            yield self.assert_redis_op(i - 1, 'rpush', 'list', str(i))
        yield self.assert_redis_op(['1', '2', '3'], 'lrange', 'list', 0, -1)
        yield self.assert_redis_op(None, 'ltrim', 'list', 0, -1)
        yield self.assert_redis_op(['1', '2', '3'], 'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_lrem_negative_num(self):
        for i in range(5):
            yield self.assert_redis_op(2 * i, 'rpush', 'list', 'v%d' % i)
            yield self.assert_redis_op(2 * i + 1, 'rpush', 'list', 1)
        yield self.assert_redis_op(2, 'lrem', 'list', 1, -2)
        yield self.assert_redis_op(
            ['v0', 1, 'v1', 1, 'v2', 1, 'v3', 'v4'], 'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_expire_persist_ttl(self):
        # Missing key.
        yield self.assert_redis_op(None, 'ttl', "tempval")
        yield self.assert_redis_op(0, 'expire', "tempval", 10)
        yield self.assert_redis_op(0, 'persist', "tempval")
        # Persistent key.
        yield self.redis.set("tempval", 1)
        yield self.assert_redis_op(None, 'ttl', "tempval")
        yield self.assert_redis_op(0, 'persist', "tempval")
        yield self.assert_redis_op(1, 'expire', "tempval", 10)
        # Temporary key.
        yield self.assert_redis_op(9, 'ttl', "tempval")
        yield self.assert_redis_op(1, 'expire', "tempval", 5)
        yield self.assert_redis_op(4, 'ttl', "tempval")
        yield self.assert_redis_op(1, 'persist', "tempval")
        # Persistent key again.
        yield self.redis.set("tempval", 1)
        yield self.assert_redis_op(None, 'ttl', "tempval")
        yield self.assert_redis_op(0, 'persist', "tempval")
        yield self.assert_redis_op(1, 'expire', "tempval", 10)

    @inlineCallbacks
    def test_type(self):
        yield self.assert_redis_op('none', 'type', 'unknown_key')
        yield self.redis.set("string_key", "a")
        yield self.assert_redis_op('string', 'type', 'string_key')
        yield self.redis.lpush("list_key", "a")
        yield self.assert_redis_op('list', 'type', 'list_key')
        yield self.redis.sadd("set_key", "a")
        yield self.assert_redis_op('set', 'type', 'set_key')
        yield self.redis.zadd("zset_key", a=1.0)
        yield self.assert_redis_op('zset', 'type', 'zset_key')
        yield self.redis.hset("hash_key", "a", 1.0)
        yield self.assert_redis_op('hash', 'type', 'hash_key')

    @inlineCallbacks
    def test_scan_simple(self):
        for i in range(20):
            yield self.redis.set("key%02d" % i, str(i))
        # Ordered the way FakeRedis.scan() returns them.
        result_keys = self.redis._sort_keys_by_hash(
            ["key%02d" % i for i in range(20)])

        self.assertEqual(
            (yield self.redis.scan(None)),
            ('10', result_keys[:10]))
        self.assertEqual(
            (yield self.redis.scan(None, count=5)),
            ('5', result_keys[:5]))
        self.assertEqual(
            (yield self.redis.scan('5', count=5)),
            ('10', result_keys[5:10]))
        self.assertEqual(
            (yield self.redis.scan('15', count=5)),
            (None, result_keys[15:]))
        self.assertEqual(
            (yield self.redis.scan(None, count=20)),
            (None, result_keys))

    @inlineCallbacks
    def test_scan_interleaved_key_changes(self):
        for i in range(20):
            yield self.redis.set("key%02d" % i, str(i))
        # Ordered the way FakeRedis.scan() returns them.
        result_keys = self.redis._sort_keys_by_hash(
            ["key%02d" % i for i in range(20)])

        self.assertEqual(
            (yield self.redis.scan(None)),
            ('10', result_keys[:10]))

        # Set and delete a bunch of keys to change some internal state. The
        # next call to scan() will return duplicates.
        for i in range(20):
            yield self.redis.set("transient%02d" % i, str(i))
            yield self.redis.delete("transient%02d" % i)

        self.assertEqual(
            (yield self.redis.scan('10')),
            ('31', result_keys[5:15]))
        self.assertEqual(
            (yield self.redis.scan('31')),
            (None, result_keys[15:]))

    @inlineCallbacks
    def test_scan_no_keys(self):
        self.assertEqual(
            (yield self.redis.scan(None)),
            (None, []))


class TestFakeRedisCharsetHandling(VumiTestCase):

    def get_redis(self, *args, **kwargs):
        redis = FakeRedis(*args, **kwargs)
        self.add_cleanup(redis.teardown)
        return redis

    def assert_redis_op(self, redis, expected, op, *args, **kw):
        self.assertEqual(expected, getattr(redis, op)(*args, **kw))

    @inlineCallbacks
    def test_charset_encoding_default(self):
        # Redis client assumes utf-8
        redis = self.get_redis()
        yield redis.set('name', u'Zoë Destroyer of Ascii')
        yield self.assert_redis_op(redis, 'Zo\xc3\xab Destroyer of Ascii',
            'get', 'name')

    @inlineCallbacks
    def test_charset_encoding_custom_replace(self):
        redis = self.get_redis(charset='ascii', errors='replace')
        yield redis.set('name', u'Zoë Destroyer of Ascii')
        yield self.assert_redis_op(redis, 'Zo? Destroyer of Ascii',
            'get', 'name')

    @inlineCallbacks
    def test_charset_encoding_custom_ignore(self):
        redis = self.get_redis(charset='ascii', errors='ignore')
        yield redis.set('name', u'Zoë Destroyer of Ascii')
        yield self.assert_redis_op(redis, 'Zo Destroyer of Ascii',
            'get', 'name')


class TestFakeRedisAsync(TestFakeRedis):
    def setUp(self):
        self.redis = FakeRedis(async=True)

    def assert_redis_op(self, expected, op, *args, **kw):
        d = getattr(self.redis, op)(*args, **kw)
        return d.addCallback(lambda r: self.assertEqual(expected, r))

    def assert_error(self, func, *args, **kw):
        return self.assertFailure(func(*args, **kw), Exception)
