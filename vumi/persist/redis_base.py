from functools import wraps


class Manager(object):

    def __init__(self, client, key_prefix):
        self._client = client
        self._key_prefix = key_prefix

    @staticmethod
    def calls_manager(manager_attr):
        """Decorate a method that calls a manager.

        This redecorates with the `call_decorator` attribute on the Manager
        subclass used, which should be either @inlineCallbacks or
        @flatten_generator.
        """
        if callable(manager_attr):
            # If we don't get a manager attribute name, default to 'manager'.
            return Manager.calls_manager('manager')(manager_attr)

        def redecorate(func):
            @wraps(func)
            def wrapper(self, *args, **kw):
                manager = getattr(self, manager_attr)
                return manager.call_decorator(func)(self, *args, **kw)
            return wrapper

        return redecorate

    @classmethod
    def from_config(cls, config):
        """Construct a manager from a dictionary of options.

        :param dict config:
            Dictionary of options for the manager.
        """
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .from_config(...)")

    def _close(self):
        """Close redis connection."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " ._close()")

    def _purge_all(self):
        """Delete *ALL* keys whose names start with this manager's key prefix.

        Use only in tests.
        """
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " ._purge_all()")

    def _make_redis_call(self, call, *args, **kw):
        """Make a redis API call using the underlying client library.
        """
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " ._make_redis_call()")

    def _filter_redis_results(self, func, results):
        """Filter results of a redis call.
        """
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " ._filter_redis_results()")

    def _key(self, key):
        """
        Generate a key using this manager's key prefix
        """
        return "%s:%s" % (self._key_prefix, key)

    def _unkey(self, key):
        """
        Strip off manager's key prefix from a key
        """
        return key.split(self._key_prefix + ":", 1)[-1]

    # Global operations

    def exists(self, key):
        return self._make_redis_call('exists', self._key(key))

    def keys(self, pattern='*'):
        results = self._make_redis_call('keys', self._key(pattern))
        return self._filter_redis_results(
            lambda keys: [self._unkey(k) for k in keys], results)

    # String operations

    def get(self, key):
        return self._make_redis_call('get', self._key(key))

    def set(self, key, value):
        return self._make_redis_call('set', self._key(key), value)

    def delete(self, key):
        return self._make_redis_call('delete', self._key(key))

    # Integer operations

    def incr(self, key, increment=1):
        return self._make_redis_call('incr', self._key(key), increment)

    # Hash operations

    def hset(self, key, field, value):
        return self._make_redis_call('hset', self._key(key), field, value)

    def hget(self, key, field):
        return self._make_redis_call('hget', self._key(key), field)

    def hdel(self, key, *fields):
        return self._make_redis_call('hdel', self._key(key), *fields)

    def hmset(self, key, mapping):
        return self._make_redis_call('hmset', self._key(key), mapping)

    def hgetall(self, key):
        return self._make_redis_call('hgetall', self._key(key))

    # def hlen(self, key):
    #     return len(self._data.get(key, {}))

    # def hvals(self, key):
    #     return self._data.get(key, {}).values()

    # def hincrby(self, key, field, amount=1):
    #     value = self._data.get(key, {}).get(field, "0")
    #     # the int(str(..)) coerces amount to an int but rejects floats
    #     value = int(value) + int(str(amount))
    #     self._data.setdefault(key, {})[field] = str(value)
    #     return value

    # def hexists(self, key, field):
    #     return int(field in self._data.get(key, {}))

    # Set operations

    def sadd(self, key, *values):
        return self._make_redis_call('sadd', self._key(key), *values)

    def smembers(self, key):
        return self._make_redis_call('smembers', self._key(key))

    def spop(self, key):
        return self._make_redis_call('spop', self._key(key))

    def srem(self, key, value):
        return self._make_redis_call('srem', self._key(key), value)

    def scard(self, key):
        return self._make_redis_call('scard', self._key(key))

    def smove(self, src, dst, value):
        return self._make_redis_call(
            'smove', self._key(src), self._key(dst), value)

    def sunion(self, key, *args):
        return self._make_redis_call('sunion', self._key(key), *args)

    def sismember(self, key, value):
        return self._make_redis_call('sismember', self._key(key), value)

    # # Sorted set operations

    # def zadd(self, key, **valscores):
    #     zval = self._data.setdefault(key, [])
    #     new_zval = [val for val in zval if val[1] not in valscores]
    #     for value, score in valscores.items():
    #         new_zval.append((score, value))
    #     new_zval.sort()
    #     self._data[key] = new_zval

    # def zrem(self, key, value):
    #     zval = self._data.setdefault(key, [])
    #     new_zval = [val for val in zval if val[1] != value]
    #     self._data[key] = new_zval

    # def zcard(self, key):
    #     return len(self._data.get(key, []))

    # def zrange(self, key, start, stop, desc=False, withscores=False,
    #             score_cast_func=float):
    #     zval = self._data.get(key, [])
    #     stop += 1  # redis start/stop are element indexes
    #     if stop == 0:
    #         stop = None
    #     results = sorted(zval[start:stop],
    #                 key=lambda (score, _): score_cast_func(score))
    #     if desc:
    #         results.reverse()
    #     if withscores:
    #         return results
    #     else:
    #         return [v for k, v in results]

    # # List operations
    # def llen(self, key):
    #     return len(self._data.get(key, []))

    # def lpop(self, key):
    #     if self.llen(key):
    #         return self._data[key].pop(0)

    # def lpush(self, key, obj):
    #     self._data.setdefault(key, []).insert(0, obj)

    # def rpush(self, key, obj):
    #     self._data.setdefault(key, []).append(obj)
    #     return self.llen(key) - 1

    # def lrange(self, key, start, end):
    #     lval = self._data.get(key, [])
    #     if end >= 0 or end < -1:
    #         end += 1
    #     else:
    #         end = None
    #     return lval[start:end]

    # def lrem(self, key, value, num=0):
    #     removed = [0]

    #     def keep(v):
    #         if v == value and (num == 0 or removed[0] < abs(num)):
    #             removed[0] += 1
    #             return False
    #         return True

    #     lval = self._data.get(key, [])
    #     if num >= 0:
    #         lval = [v for v in lval if keep(v)]
    #     else:
    #         lval.reverse()
    #         lval = [v for v in lval if keep(v)]
    #         lval.reverse()
    #     self._data[key] = lval
    #     return removed[0]

    # Expiry operations

    def expire(self, key, seconds):
        return self._make_redis_call('expire', self._key(key), seconds)

    def persist(self, key):
        return self._make_redis_call('persist', self._key(key))
