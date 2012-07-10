# -*- test-case-name: vumi.persist.tests.test_txredis_manager -*-

import txredis.protocol
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, DeferredList, succeed

from vumi.persist.redis_base import Manager
from vumi.persist.fake_redis import FakeRedis


class TxRedisManager(Manager):

    call_decorator = staticmethod(inlineCallbacks)

    @classmethod
    def _fake_manager(cls, key_prefix, client=None):
        if client is None:
            client = FakeRedis(async=True)
        manager = cls(client, key_prefix)
        # Because ._close() assumes a real connection.
        manager._close = client.teardown
        return succeed(manager)

    @classmethod
    def _manager_from_config(cls, config, key_prefix):
        """Construct a manager from a dictionary of options.

        :param dict config:
            Dictionary of options for the manager.
        :param str key_prefix:
            Key prefix for namespacing.
        """

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
