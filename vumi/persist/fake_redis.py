# -*- test-case-name: vumi.persist.tests.test_fake_redis -*-

import fnmatch
from functools import wraps
from itertools import takewhile, dropwhile
import os

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.task import Clock


FAKE_REDIS_WAIT = float(os.environ.get('VUMI_FAKE_REDIS_WAIT', '0.005'))


def maybe_async(func):
    @wraps(func)
    def wrapper(self, *args, **kw):
        result = func(self, *args, **kw)
        description = "<%s args=%s kw=%s>" % (func.__name__, args, kw)
        return self._delay_result(result, description)
    wrapper.sync = func
    return wrapper


class FakeRedis(object):
    """In process and memory implementation of redis-like data store.

    It's intended to match the Python redis module API closely so that
    it can be used in place of the redis module when testing.

    Known limitations:

    * Exceptions raised are not guaranteed to match the exception
      types raised by the real Python redis module.
    """

    def __init__(self, charset='utf-8', errors='strict', async=False):
        self._data = {}
        self._expiries = {}
        self._is_async = async
        self.clock = Clock()
        self._charset = charset
        self._charset_errors = errors
        self._delayed_calls = []

    def teardown(self):
        self._clean_up_expires()
        self._clean_up_delayed_calls()

    def _encode(self, value):
        # Replicated from
        # redis-py's redis/connection.py
        if isinstance(value, str):
            return value
        if not isinstance(value, unicode):
            value = str(value)
        if isinstance(value, unicode):
            value = value.encode(self._charset, self._charset_errors)
        return value

    def _clean_up_expires(self):
        for key in self._expiries.keys():
            delayed = self._expiries.pop(key)
            if not (delayed.cancelled or delayed.called):
                delayed.cancel()

    def _clean_up_delayed_calls(self):
        pending = []
        for delayed, description in self._delayed_calls:
            if not (delayed.cancelled or delayed.called):
                pending.append(description)
                delayed.cancel()
        if pending:
            raise RuntimeError(
                "Pending Redis operations: %s" % ", ".join(pending))

    def _delay_result(self, result, description):
        """
        Return the result with some fake delay. If we're in async mode, add
        some real delay to catch code that doesn't properly wait for the
        deferred to fire.
        """
        if self._is_async:
            d = Deferred()
            self.clock.callLater(0.1, d.callback, result)
            # Add some latency to catch things that don't wait on deferreds.
            delayed = reactor.callLater(
                FAKE_REDIS_WAIT, self.clock.advance, 0.1)
            self._delayed_calls.append((delayed, description))
            return d
        else:
            # Same delay in the sync case.
            self.clock.advance(0.1)
            return result

    # Global operations

    @maybe_async
    def type(self, key):
        value = self._data.get(key)
        if value is None:
            return 'none'
        if isinstance(value, basestring):
            return 'string'
        if isinstance(value, list):
            return 'list'
        if isinstance(value, set):
            return 'set'
        if isinstance(value, Zset):
            return 'zset'
        if isinstance(value, dict):
            return 'hash'

    @maybe_async
    def exists(self, key):
        return key in self._data

    @maybe_async
    def keys(self, pattern='*'):
        return fnmatch.filter(self._data.keys(), pattern)

    @maybe_async
    def flushdb(self):
        self._data = {}

    # String operations

    @maybe_async
    def get(self, key):
        return self._data.get(key)

    @maybe_async
    def set(self, key, value):
        value = self._encode(value)  # set() sets string value
        self._data[key] = value

    @maybe_async
    def setex(self, key, time, value):
        self.set.sync(self, key, value)
        self.expire.sync(self, key, time)
        return True

    @maybe_async
    def setnx(self, key, value):
        value = self._encode(value)  # set() sets string value
        if key not in self._data:
            self._data[key] = value
            return 1
        return 0

    @maybe_async
    def delete(self, key):
        existed = (key in self._data)
        self._data.pop(key, None)
        return existed

    # Integer operations

    # The python redis lib combines incr & incrby into incr(key, amount=1)
    @maybe_async
    def incr(self, key, amount=1):
        old_value = self._data.get(key)
        if old_value is None:
            old_value = 0
        new_value = int(old_value) + amount
        self.set.sync(self, key, new_value)
        return new_value

    @maybe_async
    def decr(self, key, amount=1):
        old_value = self._data.get(key)
        if old_value is None:
            old_value = 0
        new_value = int(old_value) - amount
        self.set.sync(self, key, new_value)
        return new_value

    # Hash operations

    @maybe_async
    def hset(self, key, field, value):
        mapping = self._data.setdefault(key, {})
        new_field = field not in mapping
        mapping[field] = value
        return int(new_field)

    @maybe_async
    def hsetnx(self, key, field, value):
        if self.hexists.sync(self, key, field):
            return 0
        return self.hset.sync(self, key, field, value)

    @maybe_async
    def hget(self, key, field):
        value = self._data.get(key, {}).get(field)
        if value is not None:
            return self._encode(value)

    @maybe_async
    def hdel(self, key, *fields):
        mapping = self._data.get(key)
        if mapping is None:
            return 0
        deleted = 0
        for field in fields:
            if field in mapping:
                del mapping[field]
                deleted += 1
        return deleted

    @maybe_async
    def hmset(self, key, mapping):
        hval = self._data.setdefault(key, {})
        hval.update(dict([(k, v) for k, v in mapping.items()]))

    @maybe_async
    def hgetall(self, key):
        return dict((self._encode(k), self._encode(v)) for k, v in
            self._data.get(key, {}).items())

    @maybe_async
    def hlen(self, key):
        return len(self._data.get(key, {}))

    @maybe_async
    def hvals(self, key):
        return map(self._encode, self._data.get(key, {}).values())

    @maybe_async
    def hincrby(self, key, field, amount=1):
        value = self._data.get(key, {}).get(field, "0")
        # the int(str(..)) coerces amount to an int but rejects floats
        value = int(value) + int(str(amount))
        self._data.setdefault(key, {})[field] = str(value)
        return value

    @maybe_async
    def hexists(self, key, field):
        return int(field in self._data.get(key, {}))

    # Set operations

    @maybe_async
    def sadd(self, key, *values):
        sval = self._data.setdefault(key, set())
        old_len = len(sval)
        sval.update(map(self._encode, values))
        return len(sval) - old_len

    @maybe_async
    def smembers(self, key):
        return self._data.get(key, set())

    @maybe_async
    def spop(self, key):
        sval = self._data.get(key, set())
        if not sval:
            return None
        return sval.pop()

    @maybe_async
    def srem(self, key, value):
        sval = self._data.get(key, set())
        if value in sval:
            sval.remove(value)
            return 1
        return 0

    @maybe_async
    def scard(self, key):
        return len(self._data.get(key, set()))

    @maybe_async
    def smove(self, src, dst, value):
        result = self.srem.sync(self, src, value)
        if result:
            self.sadd.sync(self, dst, value)
        return result

    @maybe_async
    def sunion(self, key, *args):
        union = set()
        for rkey in (key,) + args:
            union.update(self._data.get(rkey, set()))
        return union

    @maybe_async
    def sismember(self, key, value):
        sval = self._data.get(key, set())
        return value in sval

    # Sorted set operations

    @maybe_async
    def zadd(self, key, **valscores):
        zval = self._data.setdefault(key, Zset())
        return zval.zadd(**valscores)

    @maybe_async
    def zrem(self, key, value):
        zval = self._data.setdefault(key, Zset())
        return zval.zrem(value)

    @maybe_async
    def zcard(self, key):
        zval = self._data.get(key, Zset())
        return zval.zcard()

    @maybe_async
    def zrange(self, key, start, stop, desc=False, withscores=False,
               score_cast_func=float):
        zval = self._data.get(key, Zset())
        results = zval.zrange(start, stop, desc=desc,
                              score_cast_func=score_cast_func)
        if withscores:
            return results
        else:
            return [v for v, k in results]

    @maybe_async
    def zrangebyscore(self, key, min='-inf', max='+inf', start=0, num=None,
                withscores=False, score_cast_func=float):
        zval = self._data.get(key, Zset())
        results = zval.zrangebyscore(min, max, start, num,
                              score_cast_func=score_cast_func)
        if withscores:
            return results
        else:
            return [v for v, k in results]

    @maybe_async
    def zcount(self, key, min, max):
        return str(len(self.zrangebyscore.sync(self, key, min, max)))

    @maybe_async
    def zscore(self, key, value):
        zval = self._data.get(key, Zset())
        return zval.zscore(value)

    @maybe_async
    def zremrangebyrank(self, key, start, stop):
        zval = self._data.setdefault(key, Zset())
        return zval.zremrangebyrank(start, stop)

    # List operations
    @maybe_async
    def llen(self, key):
        return len(self._data.get(key, []))

    @maybe_async
    def lpop(self, key):
        if self.llen.sync(self, key):
            return self._data[key].pop(0)

    @maybe_async
    def rpop(self, key):
        if self.llen.sync(self, key):
            return self._data[key].pop(-1)

    @maybe_async
    def lpush(self, key, obj):
        self._data.setdefault(key, []).insert(0, obj)

    @maybe_async
    def rpush(self, key, obj):
        self._data.setdefault(key, []).append(obj)
        return self.llen.sync(self, key) - 1

    @maybe_async
    def lrange(self, key, start, end):
        lval = self._data.get(key, [])
        if end >= 0 or end < -1:
            end += 1
        else:
            end = None
        return lval[start:end]

    @maybe_async
    def lrem(self, key, value, num=0):
        removed = [0]

        def keep(v):
            if v == value and (num == 0 or removed[0] < abs(num)):
                removed[0] += 1
                return False
            return True

        lval = self._data.get(key, [])
        if num >= 0:
            lval = [v for v in lval if keep(v)]
        else:
            lval.reverse()
            lval = [v for v in lval if keep(v)]
            lval.reverse()
        self._data[key] = lval
        return removed[0]

    @maybe_async
    def rpoplpush(self, source, destination):
        value = self.rpop.sync(self, source)
        if value:
            self.lpush.sync(self, destination, value)
            return value

    @maybe_async
    def ltrim(self, key, start, stop):
        lval = self._data.get(key, [])
        if stop != -1:
            # -1 means "end of list", so we skip the deletion. Otherwise we
            # increment the "stop" value to avoid deleting the last value we
            # want to keep.
            del lval[stop + 1:]
        del lval[:start]

    # Expiry operations

    @maybe_async
    def expire(self, key, seconds):
        if key not in self._data:
            return 0
        self.persist.sync(self, key)
        delayed = self.clock.callLater(seconds, self.delete.sync, self, key)
        self._expiries[key] = delayed
        return 1

    @maybe_async
    def ttl(self, key):
        delayed = self._expiries.get(key)
        if delayed is not None and delayed.active():
            return int(delayed.getTime() - self.clock.seconds())
        return None

    @maybe_async
    def persist(self, key):
        delayed = self._expiries.get(key)
        if delayed is not None and delayed.active():
            delayed.cancel()
            return 1
        return 0


class Zset(object):
    """A Redis-like ordered set implementation."""

    def __init__(self):
        self._zval = []

    def _redis_range_to_py_range(self, start, end):
        if start < 0:
            start = len(self._zval) + start
        if end < 0:
            end = len(self._zval) + end
        return start, end

    def zadd(self, **valscores):
        new_zval = [val for val in self._zval if val[1] not in valscores]
        new_zval.extend((float(score), value) for value, score
                            in valscores.items())
        new_zval.sort()
        added = len(new_zval) - len(self._zval)
        self._zval = new_zval
        return added

    def zrem(self, value):
        new_zval = [val for val in self._zval if val[1] != value]
        existed = len(new_zval) != len(self._zval)
        self._zval = new_zval
        return existed

    def zcard(self):
        return len(self._zval)

    def zrange(self, start, stop, desc=False, score_cast_func=float):
        stop += 1  # redis start/stop are element indexes
        if stop == 0:
            stop = None

        # copy before changing in place
        zval = self._zval[:]
        zval.sort(reverse=desc)

        return [(v, score_cast_func(k)) for k, v in zval[start:stop]]

    def zrangebyscore(self, min='-inf', max='+inf', start=0, num=None,
                      score_cast_func=float):
        results = self.zrange(0, -1, score_cast_func=score_cast_func)
        results.sort(key=lambda val: val[1])

        def mkcheck(spec, is_upper_bound):
            spec = str(spec)
            # Handling infinities are easy, so get them out the way first.
            if spec.endswith('-inf'):
                return lambda val: False
            if spec.endswith('+inf'):
                return lambda val: True

            is_exclusive = False
            if spec.startswith('('):
                is_exclusive = True
                spec = spec[1:]
            spec = score_cast_func(spec)

            # For the lower bound, exclusive means drop less than or equal to.
            # For the upper bound, exclusive means take less than.
            if is_exclusive == is_upper_bound:
                return lambda val: val[1] < spec
            return lambda val: val[1] <= spec

        results = dropwhile(mkcheck(min, False), results)
        results = takewhile(mkcheck(max, True), results)
        results = list(results)[start:]
        if num is not None:
            results = results[:num]
        return list(results)

    def zscore(self, val):
        for score, value in self._zval:
            if value == val:
                return score

    def zremrangebyrank(self, start, stop):
        start, stop = self._redis_range_to_py_range(start, stop)
        deleted_keys = self._zval[start:stop + 1]
        del self._zval[start:stop + 1]
        return len(deleted_keys)
