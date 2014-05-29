# -*- test-case-name: vumi.persist.tests.test_redis_manager -*-

import redis

from vumi.persist.redis_base import Manager
from vumi.persist.fake_redis import FakeRedis
from vumi.utils import flatten_generator


class VumiRedis(redis.Redis):
    """
    Custom Vumi redis client implementation.
    """

    def scan(self, cursor, match=None, count=None):
        """
        Scans through all the keys in database.
        """
        args = []
        if cursor is None:
            cursor = '0'
        if match is not None:
            args.extend(("MATCH", match))
        if count is not None:
            args.extend(("COUNT", count))
        return self.execute_command("SCAN", cursor, *args)


class RedisManager(Manager):

    call_decorator = staticmethod(flatten_generator)

    @classmethod
    def _fake_manager(cls, fake_redis, manager_config):
        if fake_redis is None:
            fake_redis = FakeRedis(async=False)
        manager_config['config']['FAKE_REDIS'] = fake_redis
        manager = cls(fake_redis, **manager_config)
        # Because ._close() assumes a real connection.
        manager._close = fake_redis.teardown
        return manager

    @classmethod
    def _manager_from_config(cls, config, manager_config):
        """Construct a manager from a dictionary of options.

        :param dict config:
            Dictionary of options for the manager.
        :param str key_prefix:
            Key prefix for namespacing.
        """

        return cls(VumiRedis(**config), **manager_config)

    def _close(self):
        """Close redis connection."""
        # Close all the connections this client may have open.
        self._client.connection_pool.disconnect()

    def _purge_all(self):
        """Delete *ALL* keys whose names start with this manager's key prefix.

        Use only in tests.
        """
        for key in self.keys():
            self.delete(key)

    def _make_redis_call(self, call, *args, **kw):
        """Make a redis API call using the underlying client library.
        """
        return getattr(self._client, call)(*args, **kw)

    def _filter_redis_results(self, func, results):
        """Filter results of a redis call.
        """
        return func(results)
