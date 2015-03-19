# -*- coding: utf-8 -*-

import os

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred

from vumi.persist.fake_redis import FakeRedis, ResponseError
from vumi.tests.helpers import VumiTestCase


class FakeRedisTestMixin(object):
    """
    Test methods (and some unimplemented stubs) for FakeRedis.
    """

    def get_redis(self, **kwargs):
        """
        Return a Redis object (or a wrapper around one).
        """
        raise NotImplementedError(".get_redis() method not implemented.")

    def assert_redis_op(self, redis, expected, op, *args, **kw):
        """
        Assert that a redis operation returns the expected result.
        """
        raise NotImplementedError(".assert_redis_op() method not implemented.")

    def assert_redis_error(self, redis, op, *args, **kw):
        """
        Assert that a redis operation raises an exception.
        """
        raise NotImplementedError(
            ".assert_redis_error() method not implemented.")

    def wait(self, delay):
        """
        Wait some number of seconds, either for real or by advancing a clock.
        """
        raise NotImplementedError(".wait() method not implemented.")

    @inlineCallbacks
    def test_delete(self):
        redis = yield self.get_redis()
        yield redis.set("delete_me", 1)
        yield self.assert_redis_op(redis, True, 'delete', "delete_me")
        yield self.assert_redis_op(redis, False, 'delete', "delete_me")

    @inlineCallbacks
    def test_incr(self):
        redis = yield self.get_redis()
        yield redis.set("inc", 1)
        yield self.assert_redis_op(redis, '1', 'get', "inc")
        yield self.assert_redis_op(redis, 2, 'incr', "inc")
        yield self.assert_redis_op(redis, 3, 'incr', "inc")
        yield self.assert_redis_op(redis, '3', 'get', "inc")

    @inlineCallbacks
    def test_incrby(self):
        redis = yield self.get_redis()
        yield redis.set("inc", 1)
        yield self.assert_redis_op(redis, '1', 'get', "inc")
        yield self.assert_redis_op(redis, 3, 'incr', "inc", 2)
        yield self.assert_redis_op(redis, '3', 'get', "inc")

    @inlineCallbacks
    def test_decr(self):
        redis = yield self.get_redis()
        yield redis.set("dec", 4)
        yield self.assert_redis_op(redis, '4', 'get', "dec")
        yield self.assert_redis_op(redis, 3, 'decr', "dec")
        yield self.assert_redis_op(redis, 2, 'decr', "dec")
        yield self.assert_redis_op(redis, '2', 'get', "dec")

    @inlineCallbacks
    def test_decrby(self):
        redis = yield self.get_redis()
        yield redis.set("dec", 4)
        yield self.assert_redis_op(redis, '4', 'get', "dec")
        yield self.assert_redis_op(redis, 2, 'decr', "dec", 2)
        yield self.assert_redis_op(redis, '2', 'get', "dec")

    @inlineCallbacks
    def test_setnx(self):
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, False, 'exists', "mykey")
        yield self.assert_redis_op(redis, True, 'setnx', "mykey", "value")
        yield self.assert_redis_op(redis, "value", 'get', "mykey")
        yield self.assert_redis_op(redis, False, 'setnx', "mykey", "other")
        yield self.assert_redis_op(redis, "value", 'get', "mykey")

    @inlineCallbacks
    def test_setex(self):
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, False, 'exists', "mykey")
        yield self.assert_redis_op(redis, True, 'setex', "mykey", 10, "value")
        yield self.assert_redis_op(redis, "value", 'get', "mykey")
        yield self.assert_redis_op(redis, 10, 'ttl', "mykey")

    @inlineCallbacks
    def test_incr_with_by_param(self):
        redis = yield self.get_redis()
        yield redis.set("inc", 1)
        yield self.assert_redis_op(redis, '1', 'get', "inc")
        yield self.assert_redis_op(redis, 2, 'incr', "inc", 1)
        yield self.assert_redis_op(redis, 4, 'incr', "inc", 2)
        yield self.assert_redis_op(redis, 7, 'incr', "inc", 3)
        yield self.assert_redis_op(redis, 11, 'incr', "inc", 4)
        yield self.assert_redis_op(redis, 111, 'incr', "inc", 100)
        yield self.assert_redis_op(redis, '111', 'get', "inc")

    @inlineCallbacks
    def test_zadd(self):
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, 1, 'zadd', 'set', one=1.0)
        yield self.assert_redis_op(redis, 0, 'zadd', 'set', one=2.0)
        yield self.assert_redis_op(
            redis, [('one', 2.0)], 'zrange', 'set', 0, -1, withscores=True)
        yield self.assert_redis_error(redis, "zadd", "set", one='foo')
        yield self.assert_redis_error(redis, "zadd", "set", one=None)

    @inlineCallbacks
    def test_zrange(self):
        redis = yield self.get_redis()
        yield redis.zadd('set', one=0.1, two=0.2, three=0.3)
        yield self.assert_redis_op(redis, ['one'], 'zrange', 'set', 0, 0)
        yield self.assert_redis_op(
            redis, ['one', 'two'], 'zrange', 'set', 0, 1)
        yield self.assert_redis_op(
            redis, ['one', 'two', 'three'], 'zrange', 'set', 0, 2)
        yield self.assert_redis_op(
            redis, ['one', 'two', 'three'], 'zrange', 'set', 0, 3)
        yield self.assert_redis_op(
            redis, ['one', 'two', 'three'], 'zrange', 'set', 0, -1)
        yield self.assert_redis_op(
            redis, [('one', 0.1), ('two', 0.2), ('three', 0.3)],
            'zrange', 'set', 0, -1, withscores=True)
        yield self.assert_redis_op(
            redis, ['three', 'two', 'one'], 'zrange', 'set', 0, -1, desc=True)
        yield self.assert_redis_op(
            redis, [('three', 0.3), ('two', 0.2), ('one', 0.1)],
            'zrange', 'set', 0, -1, withscores=True, desc=True)
        yield self.assert_redis_op(
            redis, [('three', 0.3)],
            'zrange', 'set', 0, 0, withscores=True, desc=True)

    @inlineCallbacks
    def test_zrangebyscore(self):
        redis = yield self.get_redis()
        yield redis.zadd(
            'set', one=0.1, two=0.2, three=0.3, four=0.4, five=0.5)
        yield self.assert_redis_op(
            redis, ['two', 'three', 'four'], 'zrangebyscore', 'set', 0.2, 0.4)
        yield self.assert_redis_op(
            redis, ['two', 'three'], 'zrangebyscore', 'set', 0.2, 0.4, 0, 2)
        yield self.assert_redis_op(
            redis, ['three'], 'zrangebyscore', 'set', '(0.2', '(0.4')
        yield self.assert_redis_op(
            redis, ['two', 'three', 'four', 'five'],
            'zrangebyscore', 'set', '0.2', '+inf')
        yield self.assert_redis_op(
            redis, ['one', 'two'], 'zrangebyscore', 'set', '-inf', '0.2')

    @inlineCallbacks
    def test_zcount(self):
        redis = yield self.get_redis()
        yield redis.zadd(
            'set', one=0.1, two=0.2, three=0.3, four=0.4, five=0.5)
        yield self.assert_redis_op(redis, 3, 'zcount', 'set', 0.2, 0.4)

    @inlineCallbacks
    def test_zrangebyscore_with_scores(self):
        redis = yield self.get_redis()
        yield redis.zadd(
            'set', one=0.1, two=0.2, three=0.3, four=0.4, five=0.5)
        yield self.assert_redis_op(
            redis, [('two', 0.2), ('three', 0.3), ('four', 0.4)],
            'zrangebyscore', 'set', 0.2, 0.4, withscores=True)

    @inlineCallbacks
    def test_zcard(self):
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, 0, 'zcard', 'set')
        yield redis.zadd('set', one=0.1, two=0.2)
        yield self.assert_redis_op(redis, 2, 'zcard', 'set')
        yield redis.zadd('set', three=0.3)
        yield self.assert_redis_op(redis, 3, 'zcard', 'set')

    @inlineCallbacks
    def test_zrem(self):
        redis = yield self.get_redis()
        yield redis.zadd('set', one=0.1, two=0.2)
        yield self.assert_redis_op(redis, True, 'zrem', 'set', 'one')
        yield self.assert_redis_op(redis, False, 'zrem', 'set', 'one')
        yield self.assert_redis_op(
            redis, [('two', 0.2)], 'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank(self):
        redis = yield self.get_redis()
        yield redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(redis, 2, 'zremrangebyrank', 'set', 0, 1)
        yield self.assert_redis_op(
            redis, [('three', 3)], 'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank_empty_range(self):
        redis = yield self.get_redis()
        yield redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(redis, 0, 'zremrangebyrank', 'set', 10, 11)
        yield self.assert_redis_op(
            redis, [('one', 1), ('two', 2), ('three', 3)],
            'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank_negative_empty_range(self):
        redis = yield self.get_redis()
        yield redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(
            redis, 0, 'zremrangebyrank', 'set', -11, -10)
        yield self.assert_redis_op(
            redis, [('one', 1), ('two', 2), ('three', 3)],
            'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank_negative_start(self):
        redis = yield self.get_redis()
        yield redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(redis, 2, 'zremrangebyrank', 'set', -2, 2)
        yield self.assert_redis_op(
            redis, [('one', 1)], 'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank_negative_start_empty_range(self):
        redis = yield self.get_redis()
        yield redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(redis, 0, 'zremrangebyrank', 'set', -1, 1)
        yield self.assert_redis_op(
            redis, [('one', 1), ('two', 2), ('three', 3)],
            'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank_negative_stop(self):
        redis = yield self.get_redis()
        yield redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(redis, 2, 'zremrangebyrank', 'set', 1, -1)
        yield self.assert_redis_op(
            redis, [('one', 1)], 'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zremrangebyrank_negative_stop_empty_range(self):
        redis = yield self.get_redis()
        yield redis.zadd('set', one=1, two=2, three=3)
        yield self.assert_redis_op(redis, 0, 'zremrangebyrank', 'set', 0, -5)
        yield self.assert_redis_op(
            redis, [('one', 1), ('two', 2), ('three', 3)],
            'zrange', 'set', 0, -1, withscores=True)

    @inlineCallbacks
    def test_zscore(self):
        redis = yield self.get_redis()
        yield redis.zadd('set', one=0.1, two=0.2)
        yield self.assert_redis_op(redis, 0.1, 'zscore', 'set', 'one')
        yield self.assert_redis_op(redis, 0.2, 'zscore', 'set', 'two')

    @inlineCallbacks
    def test_hgetall_returns_copy(self):
        redis = yield self.get_redis()
        yield redis.hset("hash", "foo", "1")
        data = yield redis.hgetall("hash")
        data["foo"] = "2"
        yield self.assert_redis_op(redis, {"foo": "1"}, 'hgetall', "hash")

    @inlineCallbacks
    def test_hincrby(self):
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, 1, 'hincrby', "inc", "field1")
        yield self.assert_redis_op(redis, 2, 'hincrby', "inc", "field1")
        yield self.assert_redis_op(redis, 5, 'hincrby', "inc", "field1", 3)
        yield self.assert_redis_op(redis, 7, 'hincrby', "inc", "field1", "2")
        yield self.assert_redis_error(redis, "hincrby", "inc", "field1", "1.5")
        yield redis.hset("inc", "field2", "a")
        yield self.assert_redis_error(redis, "hincrby", "inc", "field2")
        yield redis.set("key", "string")
        yield self.assert_redis_error(redis, "hincrby", "key", "field1")

    @inlineCallbacks
    def test_hexists(self):
        redis = yield self.get_redis()
        yield redis.hset('key', 'field', 1)
        yield self.assert_redis_op(redis, True, 'hexists', 'key', 'field')
        yield redis.hdel('key', 'field')
        yield self.assert_redis_op(redis, False, 'hexists', 'key', 'field')

    @inlineCallbacks
    def test_hsetnx(self):
        redis = yield self.get_redis()
        yield redis.hset('key', 'field', 1)
        self.assert_redis_op(redis, 0, 'hsetnx', 'key', 'field', 2)
        self.assertEqual((yield redis.hget('key', 'field')), '1')
        self.assert_redis_op(redis, 1, 'hsetnx', 'key', 'other-field', 2)
        self.assertEqual((yield redis.hget('key', 'other-field')), '2')

    @inlineCallbacks
    def test_sadd(self):
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, 1, 'sadd', 'set', 1)
        yield self.assert_redis_op(redis, 3, 'sadd', 'set', 2, 3, 4)
        yield self.assert_redis_op(
            redis, set(['1', '2', '3', '4']), 'smembers', 'set')

    @inlineCallbacks
    def test_smove(self):
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, 1, 'sadd', 'set1', 1)
        yield self.assert_redis_op(redis, 1, 'sadd', 'set2', 2)
        yield self.assert_redis_op(redis, True, 'smove', 'set1', 'set2', '1')
        yield self.assert_redis_op(redis, set(), 'smembers', 'set1')
        yield self.assert_redis_op(redis, set(['1', '2']), 'smembers', 'set2')

        yield self.assert_redis_op(redis, False, 'smove', 'set1', 'set2', '1')
        yield self.assert_redis_op(redis, True, 'smove', 'set2', 'set3', '1')
        yield self.assert_redis_op(redis, set(['2']), 'smembers', 'set2')
        yield self.assert_redis_op(redis, set(['1']), 'smembers', 'set3')

    @inlineCallbacks
    def test_sunion(self):
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, 1, 'sadd', 'set1', 1)
        yield self.assert_redis_op(redis, 1, 'sadd', 'set2', 2)
        yield self.assert_redis_op(redis, set(['1']), 'sunion', 'set1')
        yield self.assert_redis_op(
            redis, set(['1', '2']), 'sunion', 'set1', 'set2')
        yield self.assert_redis_op(redis, set(), 'sunion', 'other')

    @inlineCallbacks
    def test_lpush(self):
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, 1, 'lpush', 'list', 1)
        yield self.assert_redis_op(redis, ['1'], 'lrange', 'list', 0, -1)
        yield self.assert_redis_op(redis, 2, 'lpush', 'list', 'a')
        yield self.assert_redis_op(redis, ['a', '1'], 'lrange', 'list', 0, -1)
        yield self.assert_redis_op(redis, 3, 'lpush', 'list', '7')
        yield self.assert_redis_op(
            redis, ['7', 'a', '1'], 'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_rpush(self):
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, 1, 'rpush', 'list', 1)
        yield self.assert_redis_op(redis, ['1'], 'lrange', 'list', 0, -1)
        yield self.assert_redis_op(redis, 2, 'rpush', 'list', 'a')
        yield self.assert_redis_op(redis, ['1', 'a'], 'lrange', 'list', 0, -1)
        yield self.assert_redis_op(redis, 3, 'rpush', 'list', '7')
        yield self.assert_redis_op(
            redis, ['1', 'a', '7'], 'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_rpop(self):
        redis = yield self.get_redis()
        yield redis.lpush('key', 1)
        yield redis.lpush('key', 'a')
        yield redis.lpush('key', '3')
        yield self.assert_redis_op(redis, '1', 'rpop', 'key')
        yield self.assert_redis_op(redis, 'a', 'rpop', 'key')
        yield self.assert_redis_op(redis, '3', 'rpop', 'key')
        yield self.assert_redis_op(redis, None, 'rpop', 'key')

    @inlineCallbacks
    def test_rpoplpush(self):
        redis = yield self.get_redis()
        yield redis.lpush('source', 1)
        yield redis.lpush('source', 'a')
        yield redis.lpush('source', '3')
        yield self.assert_redis_op(
            redis, '1', 'rpoplpush', 'source', 'destination')
        yield self.assert_redis_op(
            redis, 'a', 'rpoplpush', 'source', 'destination')
        yield self.assert_redis_op(
            redis, '3', 'rpoplpush', 'source', 'destination')
        yield self.assert_redis_op(redis, None, 'rpop', 'source')
        yield self.assert_redis_op(redis, '1', 'rpop', 'destination')
        yield self.assert_redis_op(redis, 'a', 'rpop', 'destination')
        yield self.assert_redis_op(redis, '3', 'rpop', 'destination')
        yield self.assert_redis_op(redis, None, 'rpop', 'destination')

    @inlineCallbacks
    def test_lrem(self):
        redis = yield self.get_redis()
        for i in range(5):
            yield self.assert_redis_op(
                redis, 2 * i + 1, 'rpush', 'list', 'v%d' % i)
            yield self.assert_redis_op(redis, 2 * i + 2, 'rpush', 'list', 1)
        yield self.assert_redis_op(redis, 5, 'lrem', 'list', 1)
        yield self.assert_redis_op(
            redis, ['v0', 'v1', 'v2', 'v3', 'v4'], 'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_lrem_positive_num(self):
        redis = yield self.get_redis()
        for i in range(5):
            yield self.assert_redis_op(
                redis, 2 * i + 1, 'rpush', 'list', 'v%d' % i)
            yield self.assert_redis_op(redis, 2 * i + 2, 'rpush', 'list', 1)
        yield self.assert_redis_op(redis, 2, 'lrem', 'list', 1, 2)
        yield self.assert_redis_op(
            redis, ['v0', 'v1', 'v2', '1', 'v3', '1', 'v4', '1'],
            'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_lrem_negative_num(self):
        redis = yield self.get_redis()
        for i in range(5):
            yield self.assert_redis_op(
                redis, 2 * i + 1, 'rpush', 'list', 'v%d' % i)
            yield self.assert_redis_op(redis, 2 * i + 2, 'rpush', 'list', 1)
        yield self.assert_redis_op(redis, 2, 'lrem', 'list', 1, -2)
        yield self.assert_redis_op(
            redis, ['v0', '1', 'v1', '1', 'v2', '1', 'v3', 'v4'],
            'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_ltrim(self):
        redis = yield self.get_redis()
        for i in range(1, 5):
            yield self.assert_redis_op(redis, i, 'rpush', 'list', str(i))
        yield self.assert_redis_op(
            redis, ['1', '2', '3', '4'], 'lrange', 'list', 0, -1)
        yield self.assert_redis_op(redis, True, 'ltrim', 'list', 1, 2)
        yield self.assert_redis_op(redis, ['2', '3'], 'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_ltrim_mid_range(self):
        redis = yield self.get_redis()
        for i in range(1, 6):
            yield self.assert_redis_op(redis, i, 'rpush', 'list', str(i))
        yield self.assert_redis_op(
            redis, ['1', '2', '3', '4', '5'], 'lrange', 'list', 0, -1)
        yield self.assert_redis_op(redis, True, 'ltrim', 'list', 2, 3)
        yield self.assert_redis_op(redis, ['3', '4'], 'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_ltrim_keep_all(self):
        redis = yield self.get_redis()
        for i in range(1, 4):
            yield self.assert_redis_op(redis, i, 'rpush', 'list', str(i))
        yield self.assert_redis_op(
            redis, ['1', '2', '3'], 'lrange', 'list', 0, -1)
        yield self.assert_redis_op(redis, True, 'ltrim', 'list', 0, -1)
        yield self.assert_redis_op(
            redis, ['1', '2', '3'], 'lrange', 'list', 0, -1)

    @inlineCallbacks
    def test_expire_persist_ttl(self):
        redis = yield self.get_redis()
        # Missing key.
        yield self.assert_redis_op(redis, None, 'ttl', "tempval")
        yield self.assert_redis_op(redis, 0, 'expire', "tempval", 10)
        yield self.assert_redis_op(redis, 0, 'persist', "tempval")
        # Persistent key.
        yield redis.set("tempval", 1)
        yield self.assert_redis_op(redis, None, 'ttl', "tempval")
        yield self.assert_redis_op(redis, 0, 'persist', "tempval")
        yield self.assert_redis_op(redis, 1, 'expire', "tempval", 10)
        # Temporary key.
        yield self.assert_redis_op(redis, 10, 'ttl', "tempval")
        yield self.wait(redis, 0.6)  # Wait a bit for the TTL to change.
        yield self.assert_redis_op(redis, 9, 'ttl', "tempval")
        yield self.assert_redis_op(redis, 1, 'expire', "tempval", 5)
        yield self.assert_redis_op(redis, 5, 'ttl', "tempval")
        yield self.assert_redis_op(redis, 1, 'persist', "tempval")
        # Persistent key again.
        yield redis.set("tempval", 1)
        yield self.assert_redis_op(redis, None, 'ttl', "tempval")
        yield self.assert_redis_op(redis, 0, 'persist', "tempval")
        yield self.assert_redis_op(redis, 1, 'expire', "tempval", 10)

    @inlineCallbacks
    def test_type(self):
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, 'none', 'type', 'unknown_key')
        yield redis.set("string_key", "a")
        yield self.assert_redis_op(redis, 'string', 'type', 'string_key')
        yield redis.lpush("list_key", "a")
        yield self.assert_redis_op(redis, 'list', 'type', 'list_key')
        yield redis.sadd("set_key", "a")
        yield self.assert_redis_op(redis, 'set', 'type', 'set_key')
        yield redis.zadd("zset_key", a=1.0)
        yield self.assert_redis_op(redis, 'zset', 'type', 'zset_key')
        yield redis.hset("hash_key", "a", 1.0)
        yield self.assert_redis_op(redis, 'hash', 'type', 'hash_key')

    @inlineCallbacks
    def test_charset_encoding_default(self):
        # Redis client assumes utf-8
        redis = yield self.get_redis()
        yield redis.set('name', u'Zoë Destroyer of Ascii')
        yield self.assert_redis_op(
            redis, 'Zo\xc3\xab Destroyer of Ascii', 'get', 'name')

    @inlineCallbacks
    def test_charset_encoding_custom_replace(self):
        redis = yield self.get_redis(charset='ascii', errors='replace')
        yield redis.set('name', u'Zoë Destroyer of Ascii')
        yield self.assert_redis_op(
            redis, 'Zo? Destroyer of Ascii', 'get', 'name')

    @inlineCallbacks
    def test_charset_encoding_custom_ignore(self):
        redis = yield self.get_redis(charset='ascii', errors='ignore')
        yield redis.set('name', u'Zoë Destroyer of Ascii')
        yield self.assert_redis_op(
            redis, 'Zo Destroyer of Ascii', 'get', 'name')

    @inlineCallbacks
    def test_scan_no_keys(self):
        """
        Real and fake Redis implementation all return the same (empty) response
        when we scan for keys that don't exist. Other scanning methods are in
        FakeRedisUnverifiedTestMixin because we can't fake the same arbitrary
        order in which keys are returned from real Redis.
        """
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, [None, []], 'scan', None)

    @inlineCallbacks
    def test_pfadd_and_pfcount(self):
        """
        We can't test these two things separately, so test them together.
        """
        redis = yield self.get_redis()
        yield self.assert_redis_op(redis, 1, 'pfadd', 'hll1', 'a')
        yield self.assert_redis_op(redis, 1, 'pfcount', 'hll1')
        yield self.assert_redis_op(redis, 0, 'pfadd', 'hll1', 'a')
        yield self.assert_redis_op(redis, 1, 'pfcount', 'hll1')

        yield self.assert_redis_op(redis, 1, 'pfadd', 'hll2', 'a', 'b')
        yield self.assert_redis_op(redis, 2, 'pfcount', 'hll2')
        yield self.assert_redis_op(redis, 0, 'pfadd', 'hll2', 'a', 'b')
        yield self.assert_redis_op(redis, 2, 'pfcount', 'hll2')


class FakeRedisUnverifiedTestMixin(object):
    """
    This mixin adds some extra tests that are not verified against real Redis.

    Each test in here should explain why verification isn't possible.
    """

    @inlineCallbacks
    def test_scan_simple(self):
        """
        Scanning returns keys in an order that depends on arbitrary state in
        the Redis server, so we can't fake it in a way that's identical to real
        Redis.
        """
        redis = yield self.get_redis()
        for i in range(20):
            yield redis.set("key%02d" % i, str(i))
        # Ordered the way FakeRedis.scan() returns them.
        result_keys = redis._sort_keys_by_hash(
            ["key%02d" % i for i in range(20)])

        self.assert_redis_op(redis, ['10', result_keys[:10]], 'scan', None)
        self.assert_redis_op(
            redis, ['5', result_keys[:5]], 'scan', None, count=5)
        self.assert_redis_op(
            redis, ['10', result_keys[5:10]], 'scan', '5', count=5)
        self.assert_redis_op(
            redis, [None, result_keys[15:]], 'scan', '15', count=5)
        self.assert_redis_op(
            redis, [None, result_keys], 'scan', None, count=20)

    @inlineCallbacks
    def test_scan_interleaved_key_changes(self):
        """
        Scanning returns keys in an order that depends on arbitrary state in
        the Redis server, so we can't fake it in a way that's identical to real
        Redis.
        """
        redis = yield self.get_redis()
        for i in range(20):
            yield redis.set("key%02d" % i, str(i))
        # Ordered the way FakeRedis.scan() returns them.
        result_keys = redis._sort_keys_by_hash(
            ["key%02d" % i for i in range(20)])

        self.assert_redis_op(redis, ['10', result_keys[:10]], 'scan', None)

        # Set and delete a bunch of keys to change some internal state. The
        # next call to scan() will return duplicates.
        for i in range(20):
            yield redis.set("transient%02d" % i, str(i))
            yield redis.delete("transient%02d" % i)

        self.assert_redis_op(redis, ['31', result_keys[5:15]], 'scan', '10')
        self.assert_redis_op(redis, [None, result_keys[15:]], 'scan', '31')

    @inlineCallbacks
    def test_pfadd_and_pfcount_large(self):
        """
        for large sets, we get approximate counts. Redis and hyperloglog use
        different hash functions, so we get different approximations out of
        them and can't verify the results.
        """
        redis = yield self.get_redis()
        values = ['v%s' % i for i in xrange(1000)]
        yield self.assert_redis_op(redis, 1, 'pfadd', 'hll1', *values)
        yield self.assert_redis_op(redis, 998, 'pfcount', 'hll1')
        yield self.assert_redis_op(redis, 0, 'pfadd', 'hll1', *values)
        yield self.assert_redis_op(redis, 998, 'pfcount', 'hll1')


class TestFakeRedis(FakeRedisUnverifiedTestMixin, FakeRedisTestMixin,
                    VumiTestCase):

    def get_redis(self, **kwargs):
        redis = FakeRedis(**kwargs)
        self.add_cleanup(redis.teardown)
        return redis

    def assert_redis_op(self, redis, expected, op, *args, **kw):
        self.assertEqual(expected, getattr(redis, op)(*args, **kw))

    def assert_redis_error(self, redis, op, *args, **kw):
        self.assertRaises(Exception, getattr(redis, op), *args, **kw)

    def wait(self, redis, delay):
        redis.clock.advance(delay)


class TestFakeRedisAsync(FakeRedisUnverifiedTestMixin, FakeRedisTestMixin,
                         VumiTestCase):

    def get_redis(self, **kwargs):
        redis = FakeRedis(async=True, **kwargs)
        self.add_cleanup(redis.teardown)
        return redis

    def assert_redis_op(self, redis, expected, op, *args, **kw):
        d = getattr(redis, op)(*args, **kw)
        return d.addCallback(lambda r: self.assertEqual(expected, r))

    def assert_redis_error(self, redis, op, *args, **kw):
        d = getattr(redis, op)(*args, **kw)
        return self.assertFailure(d, Exception)

    def wait(self, redis, delay):
        redis.clock.advance(delay)


class RedisPairWrapper(object):
    def __init__(self, test_case, fake_redis, real_redis):
        self._test_case = test_case
        self._fake_redis = fake_redis
        self._real_redis = real_redis
        self._perform_operation = self._real_redis.call_decorator(
            self._perform_operation_gen)

    def _perform_operation_gen(self, op, *args, **kw):
        """
        Perform an operation on both the fake and real Redises and assert that
        the responses and errors are the same.

        NOTE: This method is a generator and is not used directly. It's wrapped
              with an appropriate sync/async wrapper in __init__() above.
        """
        results = []
        errors = []
        for redis in [self._fake_redis, self._real_redis]:
            try:
                result = yield getattr(redis, op)(*args, **kw)
            except Exception as e:
                errors.append(e)
                if results != []:
                    self._test_case.fail(
                        "Fake redis returned %r but real redis raised %r" % (
                            results[0], errors[0]))
            else:
                results.append(result)
                if errors != []:
                    self._test_case.fail(
                        "Real redis returned %r but fake redis raised %r" % (
                            results[0], errors[0]))

        # First, handle errors.
        if errors:
            # We match based on the exception type name so that ResponseErrors
            # from different places are considered equal. We ignore the error
            # message in the check, but we display it to aid debugging.
            self._test_case.assertEqual(
                type(errors[0]).__name__, type(errors[1]).__name__,
                ("Fake redis (a) and real redis (b) errors different:"
                 "\n a = %r\n b = %r") % tuple(errors))
            raise errors[0]

        # Now handle results.
        self._test_case.assertEqual(
            results[0], results[1],
            "Fake redis (a) and real redis (b) responses different:")
        returnValue(results[0])

    def __getattr__(self, name):
        return lambda *args, **kw: self._perform_operation(name, *args, **kw)


class TestFakeRedisVerify(FakeRedisTestMixin, VumiTestCase):

    if 'VUMITEST_REDIS_DB' not in os.environ:
        skip = ("This test requires a real Redis server. Set VUMITEST_REDIS_DB"
                " to run it.")

    def get_redis(self, **kwargs):
        from vumi.persist.redis_manager import RedisManager
        # Fake redis
        fake_redis = FakeRedis(**kwargs)
        self.add_cleanup(fake_redis.teardown)
        # Real redis
        config = {
            'FAKE_REDIS': 'yes',
            'key_prefix': 'redistest',
        }
        config.update(kwargs)
        real_redis = RedisManager.from_config(config)
        self.add_cleanup(self.cleanup_manager, real_redis)
        real_redis._purge_all()
        # Both redises
        return RedisPairWrapper(self, fake_redis, real_redis)

    def cleanup_manager(self, manager):
        manager._purge_all()
        manager._close()

    def assert_redis_op(self, redis, expected, op, *args, **kw):
        self.assertEqual(expected, getattr(redis, op)(*args, **kw))

    def assert_redis_error(self, redis, op, *args, **kw):
        self.assertRaises(ResponseError, getattr(redis, op), *args, **kw)

    def wait(self, redis, delay):
        redis._fake_redis.clock.advance(delay)
        d = Deferred()
        reactor.callLater(delay, d.callback, None)
        return d


class TestFakeRedisVerifyAsync(FakeRedisTestMixin, VumiTestCase):

    if 'VUMITEST_REDIS_DB' not in os.environ:
        skip = ("This test requires a real Redis server. Set VUMITEST_REDIS_DB"
                " to run it.")

    @inlineCallbacks
    def get_redis(self, **kwargs):
        from vumi.persist.txredis_manager import TxRedisManager
        # Fake redis
        fake_redis = FakeRedis(async=True, **kwargs)
        self.add_cleanup(fake_redis.teardown)
        # Real redis
        config = {
            'FAKE_REDIS': 'yes',
            'key_prefix': 'redistest',
        }
        config.update(kwargs)
        real_redis = yield TxRedisManager.from_config(config)
        self.add_cleanup(self.cleanup_manager, real_redis)
        # Both redises
        yield real_redis._purge_all()

        returnValue(RedisPairWrapper(self, fake_redis, real_redis))

    @inlineCallbacks
    def cleanup_manager(self, manager):
        yield manager._purge_all()
        yield manager._close()

    def assert_redis_op(self, redis, expected, op, *args, **kw):
        d = getattr(redis, op)(*args, **kw)
        return d.addCallback(lambda r: self.assertEqual(expected, r))

    def assert_redis_error(self, redis, op, *args, **kw):
        d = getattr(redis, op)(*args, **kw)
        return self.assertFailure(d, ResponseError)

    def wait(self, redis, delay):
        redis._fake_redis.clock.advance(delay)
        d = Deferred()
        reactor.callLater(delay, d.callback, None)
        return d
