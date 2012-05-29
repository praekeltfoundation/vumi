
import txredis.protocol
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, DeferredList

from vumi.persist.redis_base import Manager


class TxRedisManager(Manager):
    @classmethod
    def from_config(cls, config, key_prefix=None):
        """Construct a manager from a dictionary of options.

        :param dict config:
            Dictionary of options for the manager.
        :param str key_prefix:
            Key prefix for namespacing.
        """

        # FIXME: Handle fake redis better.
        from vumi.tests.utils import FakeRedis
        if config == "FAKE_REDIS":
            raise NotImplementedError()
        if isinstance(config, FakeRedis):
            raise NotImplementedError()

        config = config.copy()  # So we can safely mutilate it.

        config_prefix = config.pop('key_prefix', None)
        if config_prefix is not None:
            key_prefix = "%s:%s" % (config_prefix, key_prefix)

        host = config.pop('host', 'localhost')
        port = config.pop('port', 6379)

        factory = txredis.protocol.RedisClientFactory(**config)
        d = factory.deferred
        reactor.connectTCP(host, port, factory)
        return d.addCallback(lambda r: cls(r, key_prefix))

    @inlineCallbacks
    def _close(self):
        """Close redis connection."""
        yield self._client.factory.stopTrying()
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
