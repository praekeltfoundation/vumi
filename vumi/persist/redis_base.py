from functools import wraps

from vumi.persist.ast_magic import make_function


def make_callfunc(name, args, vararg=None, kwarg=None, defaults=(),
                  filter_func=None, key_args=('key',)):

    def func(self, *a, **kw):

        def _f(k, v):
            if k in key_args:
                return self._key(v)
            return v

        arg_names = list(args) + [vararg] * len(a)
        aa = [_f(k, v) for k, v in zip(arg_names, a)]
        kk = dict((k, _f(k, v)) for k, v in kw.items())

        result = self._make_redis_call(name, *aa, **kk)
        f_func = filter_func
        if f_func:
            if isinstance(filter_func, basestring):
                f_func = getattr(self, f_func)
            result = self._filter_redis_results(f_func, result)
        return result

    fargs = ['self'] + list(args)
    return make_function(name, func, fargs, vararg, kwarg, defaults)


class RedisCall(object):
    def __init__(self, args, vararg=None, kwarg=None, defaults=(),
                 filter_func=None, key_args=('key',)):
        self.args = args
        self.vararg = vararg
        self.kwarg = kwarg
        self.defaults = defaults
        self.filter_func = filter_func
        self.key_args = key_args

        self.packaged = [args, vararg, kwarg, defaults, filter_func, key_args]


class CallMakerMetaclass(type):
    def __new__(meta, classname, bases, class_dict):
        new_class_dict = {}
        for name, attr in class_dict.items():
            if isinstance(attr, RedisCall):
                attr = make_callfunc(name, *attr.packaged)

            new_class_dict[name] = attr
        return type.__new__(meta, classname, bases, new_class_dict)


class Manager(object):

    __metaclass__ = CallMakerMetaclass

    def __init__(self, client, key_prefix):
        self._client = client
        self._key_prefix = key_prefix
        self._key_separator = '#'  # So we can override if necessary.

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
    def from_config(cls, config, key_prefix=None):
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
        return "%s%s%s" % (self._key_prefix, self._key_separator, key)

    def _unkey(self, key):
        """
        Strip off manager's key prefix from a key
        """
        return key.split(
            "%s%s" % (self._key_prefix, self._key_separator), 1)[-1]

    def _unkeys(self, keys):
        return [self._unkey(k) for k in keys]

    # Global operations

    exists = RedisCall(['key'])
    keys = RedisCall(['pattern'], defaults=['*'], key_args=['pattern'],
                     filter_func='_unkeys')

    # String operations

    get = RedisCall(['key'])
    set = RedisCall(['key', 'value'])
    setnx = RedisCall(['key', 'value'])
    delete = RedisCall(['key'])
    setnx = RedisCall(['key', 'value'])

    # Integer operations

    incr = RedisCall(['key', 'amount'], defaults=[1])

    # Hash operations

    hset = RedisCall(['key', 'field', 'value'])
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

    # List operations

    llen = RedisCall(['key'])
    lpop = RedisCall(['key'])
    lpush = RedisCall(['key', 'obj'])
    rpush = RedisCall(['key', 'obj'])
    lrange = RedisCall(['key', 'start', 'end'])
    lrem = RedisCall(['key', 'value', 'num'], defaults=[0])

    # Expiry operations

    expire = RedisCall(['key', 'seconds'])
    persist = RedisCall(['key'])
