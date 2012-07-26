# -*- test-case-name: vumi.persist.tests.test_txredis_manager -*-

import txredis.protocol
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, DeferredList, succeed

from vumi.persist.redis_base import Manager
from vumi.persist.fake_redis import FakeRedis


class VumiRedis(txredis.protocol.Redis):
    def hget(self, key, field):
        d = super(VumiRedis, self).hget(key, field)
        d.addCallback(lambda r: r[field])
        return d

    def lrem(self, key, value, num=0):
        return super(VumiRedis, self).lrem(key, value, count=num)

    def lpop(self, key):
        return self.pop(key, tail=False)

    def rpop(self, key):
        return self.pop(key, tail=True)

    def setex(self, key, seconds, value):
        return self.set(key, value, expire=seconds)

    def setnx(self, key, value):
        return self.set(key, value, preserve=True)

    def zadd(self, key, *args, **kwargs):
        if args:
            if len(args) % 2 != 0:
                raise ValueError("ZADD requires an equal number of "
                                 "values and scores")
        pieces = zip(args[::2], args[1::2])
        pieces.extend(kwargs.iteritems())
        orig_zadd = super(VumiRedis, self).zadd
        deferreds = [orig_zadd(key, member, score) for member, score in pieces]
        return DeferredList(deferreds)

    def zrange(self, key, start, end, desc=False, withscores=False):
        return super(VumiRedis, self).zrange(key, start, end,
                                             withscores=withscores,
                                             reverse=desc)


class VumiRedisClientFactory(txredis.protocol.RedisClientFactory):
    protocol = VumiRedis


class TxRedisManager(Manager):

    call_decorator = staticmethod(inlineCallbacks)

    @classmethod
    def _fake_manager(cls, fake_redis, key_prefix, key_separator):
        if fake_redis is None:
            fake_redis = FakeRedis(async=True)
        manager = cls(fake_redis, key_prefix)
        # Because ._close() assumes a real connection.
        manager._close = fake_redis.teardown
        return succeed(manager)

    @classmethod
    def _manager_from_config(cls, config, key_prefix, key_separator):
        """Construct a manager from a dictionary of options.

        :param dict config:
            Dictionary of options for the manager.
        :param str key_prefix:
            Key prefix for namespacing.
        """

        host = config.pop('host', 'localhost')
        port = config.pop('port', 6379)

        factory = VumiRedisClientFactory(**config)
        d = factory.deferred
        reactor.connectTCP(host, port, factory)
        return d.addCallback(lambda r: cls(r, key_prefix, key_separator))

    @inlineCallbacks
    def _close(self):
        """Close redis connection."""
        yield self._client.factory.stopTrying()
        if self._client.transport is not None:
            yield self._client.transport.loseConnection()

    @inlineCallbacks
    def _purge_all(self):
        """Delete *ALL* keys whose names start with this manager's key prefix.

        Use only in tests.
        """
        deferreds = []
        for key in (yield self.keys()):
            deferreds.append(self.delete(key))
        yield DeferredList(deferreds)

    def _make_redis_call(self, call, *args, **kw):
        """Make a redis API call using the underlying client library.
        """
        return getattr(self._client, call)(*args, **kw)

    def _filter_redis_results(self, func, results):
        """Filter results of a redis call.
        """
        return results.addCallback(func)
