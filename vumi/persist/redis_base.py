# -*- test-case-name: vumi.persist.tests.test_redis_base -*-

import os
from functools import wraps

from vumi.persist.ast_magic import make_function
from vumi.persist.fake_redis import FakeRedis


def make_callfunc(name, redis_call):

    def func(self, *a, **kw):

        def _f(k, v):
            if k in redis_call.key_args:
                return self._key(v)
            return v

        arg_names = list(redis_call.args) + [redis_call.vararg] * len(a)
        aa = [_f(k, v) for k, v in zip(arg_names, a)]
        kk = dict((k, _f(k, v)) for k, v in kw.items())

        result = self._make_redis_call(name, *aa, **kk)
        f_func = redis_call.filter_func
        if f_func:
            if isinstance(f_func, basestring):
                f_func = getattr(self, f_func)
            result = self._filter_redis_results(f_func, result)
        return result

    fargs = ['self'] + list(redis_call.args)
    return make_function(name, func, fargs, redis_call.vararg,
                         redis_call.kwarg, redis_call.defaults)


class RedisCall(object):
    def __init__(self, args, vararg=None, kwarg=None, defaults=(),
                 filter_func=None, key_args=('key',)):
        self.args = args
        self.vararg = vararg
        self.kwarg = kwarg
        self.defaults = defaults
        self.filter_func = filter_func
        self.key_args = key_args


class CallMakerMetaclass(type):
    def __new__(meta, classname, bases, class_dict):
        new_class_dict = {}
        for name, attr in class_dict.items():
            if isinstance(attr, RedisCall):
                attr = make_callfunc(name, attr)

            new_class_dict[name] = attr
        return type.__new__(meta, classname, bases, new_class_dict)


class Manager(object):

    __metaclass__ = CallMakerMetaclass

    def __init__(self, client, config, key_prefix, key_separator=None):
        if key_separator is None:
            key_separator = ':'
        self._client = client
        self._config = config
        self._key_prefix = key_prefix
        self._key_separator = key_separator

    def __deepcopy__(self, memo):
        "This is to let managers pass through config deepcopies in tests."
        return self

    def get_key_prefix(self):
        """This is only intended for use in testing, not production."""
        return self._key_prefix

    def sub_manager(self, sub_prefix):
        key_prefix = self._key(sub_prefix)
        sub_man = self.__class__(self._client, self._config, key_prefix)
        if isinstance(self._client, FakeRedis):
            sub_man._close = self._client.teardown
        return sub_man

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

        # So we can mangle it
        client_config = config.copy()
        manager_config = {
            'config': config.copy(),
            'key_prefix': client_config.pop('key_prefix', None),
            'key_separator': client_config.pop('key_separator', ':'),
        }
        fake_redis = client_config.pop('FAKE_REDIS', None)
        if 'VUMITEST_REDIS_DB' in os.environ:
            fake_redis = None
            client_config['db'] = int(os.environ['VUMITEST_REDIS_DB'])

        if fake_redis is not None:
            if isinstance(fake_redis, cls):
                # We want to unwrap the existing fake_redis to rewrap it.
                fake_redis = fake_redis._client
            if isinstance(fake_redis, FakeRedis):
                # We want to wrap the existing fake_redis.
                pass
            else:
                # We want a new fake redis.
                fake_redis = None
            return cls._fake_manager(fake_redis, manager_config)

        return cls._manager_from_config(client_config, manager_config)

    @classmethod
    def _fake_manager(cls, fake_redis, manager_config):
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " ._fake_manager(...)")

    @classmethod
    def _manager_from_config(cls, client_config, manager_config):
        """Construct a client from a dictionary of options.

        :param dict config:
            Dictionary of options for the manager.
        :param str key_prefix:
            Key prefix for namespacing.
        """
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " ._manager_from_config(...)")

    def close_manager(self):
        return self._close()

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
        if self._key_prefix is None:
            return key
        return "%s%s%s" % (self._key_prefix, self._key_separator, key)

    def _unkey(self, key):
        """
        Strip off manager's key prefix from a key
        """
        prefix = "%s%s" % (self._key_prefix, self._key_separator)
        if key.startswith(prefix):
            return key[len(prefix):]
        return key

    def _unkeys(self, keys):
        return [self._unkey(k) for k in keys]

    def _unkeys_scan(self, scan_results):
        return [scan_results[0], self._unkeys(scan_results[1])]

    # Global operations

    type = RedisCall(['key'])
    exists = RedisCall(['key'])
    keys = RedisCall(['pattern'], defaults=['*'], key_args=['pattern'],
                     filter_func='_unkeys')
    scan = RedisCall(['cursor', 'match', 'count'],
                     defaults=['*', None], key_args=['match'],
                     filter_func='_unkeys_scan')

    # String operations

    get = RedisCall(['key'])
    set = RedisCall(['key', 'value'])
    setnx = RedisCall(['key', 'value'])
    delete = RedisCall(['key'])
    setex = RedisCall(['key', 'seconds', 'value'])

    # Integer operations
    incr = RedisCall(['key', 'amount'], defaults=[1])
    incrby = RedisCall(['key', 'amount'])
    decr = RedisCall(['key', 'amount'], defaults=[1])
    decrby = RedisCall(['key', 'amount'])

    # Hash operations

    hset = RedisCall(['key', 'field', 'value'])
    hsetnx = RedisCall(['key', 'field', 'value'])
    hget = RedisCall(['key', 'field'])
    hdel = RedisCall(['key'], vararg='fields')
    hmset = RedisCall(['key', 'mapping'])
    hgetall = RedisCall(['key'])
    hlen = RedisCall(['key'])
    hvals = RedisCall(['key'])
    hincrby = RedisCall(['key', 'field', 'amount'], defaults=[1])
    hexists = RedisCall(['key', 'field'])

    # Set operations

    sadd = RedisCall(['key'], vararg='values')
    smembers = RedisCall(['key'])
    spop = RedisCall(['key'])
    srem = RedisCall(['key', 'value'])
    scard = RedisCall(['key'])
    smove = RedisCall(['src', 'dst', 'value'], key_args=['src', 'dst'])
    sunion = RedisCall(['key'], vararg='args', key_args=['key', 'args'])
    sismember = RedisCall(['key', 'value'])

    # Sorted set operations

    zadd = RedisCall(['key'], kwarg='valscores')
    zrem = RedisCall(['key', 'value'])
    zcard = RedisCall(['key'])
    zrange = RedisCall(['key', 'start', 'stop', 'desc', 'withscores'],
                       defaults=[False, False])
    zrangebyscore = RedisCall(
        ['key', 'min', 'max', 'start', 'num', 'withscores'],
        defaults=['-inf', '+inf', None, None, False])
    zscore = RedisCall(['key', 'value'])
    zcount = RedisCall(['key', 'min', 'max'])
    zremrangebyrank = RedisCall(['key', 'start', 'stop'])

    # List operations

    llen = RedisCall(['key'])
    lpop = RedisCall(['key'])
    rpop = RedisCall(['key'])
    lpush = RedisCall(['key', 'obj'])
    rpush = RedisCall(['key', 'obj'])
    lrange = RedisCall(['key', 'start', 'end'])
    lrem = RedisCall(['key', 'value', 'num'], defaults=[0])
    rpoplpush = RedisCall(
        ['source'], vararg='destination', key_args=['source', 'destination'])
    ltrim = RedisCall(['key', 'start', 'stop'])

    # Expiry operations

    expire = RedisCall(['key', 'seconds'])
    persist = RedisCall(['key'])
    ttl = RedisCall(['key'])

    # HyperLogLog operations

    pfadd = RedisCall(['key'], vararg='values')
    pfcount = RedisCall(['key'])
