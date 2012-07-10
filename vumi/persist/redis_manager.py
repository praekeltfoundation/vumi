# -*- test-case-name: vumi.persist.tests.test_redis_manager -*-

import redis

from vumi.persist.redis_base import Manager
from vumi.persist.fake_redis import FakeRedis
from vumi.persist.riak_manager import flatten_generator


class RedisManager(Manager):

    call_decorator = staticmethod(flatten_generator)

    @classmethod
    def _fake_manager(cls, key_prefix, client=None):
        if client is None:
            client = FakeRedis()
        manager = cls(client, key_prefix)
        # Because ._close() assumes a real connection.
        manager._close = client.teardown
        return manager

    @classmethod
    def _manager_from_config(cls, config, key_prefix):
        """Construct a manager from a dictionary of options.

        :param dict config:
            Dictionary of options for the manager.
        :param str key_prefix:
            Key prefix for namespacing.
        """

        return cls(redis.Redis(**config), key_prefix)

    def _close(self):
        """Close redis connection."""
        pass

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
