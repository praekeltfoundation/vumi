# -*- test-case-name: vumi.persist.tests.test_redis_manager -*-

import redis

from vumi.persist.redis_base import Manager


class RedisManager(Manager):
    @classmethod
    def from_config(cls, config, key_prefix=''):
        """Construct a manager from a dictionary of options.

        :param dict config:
            Dictionary of options for the manager.
        :param str key_prefix:
            Key prefix for namespacing.
        """

        # Is there a cleaner way to do this?
        from vumi.persist.tests.fake_redis import FakeRedis
        if config == "FAKE_REDIS":
            config = FakeRedis()
        if isinstance(config, FakeRedis):
            return cls(config, key_prefix)

        config = config.copy()  # So we can safely mutilate it.

        config_prefix = config.pop('key_prefix', None)
        if config_prefix is not None:
            key_prefix = "%s:%s" % (config_prefix, key_prefix)

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
