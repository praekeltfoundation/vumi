# -*- test-case-name: vumi.components.tests.test_session -*-

"""Session management utilities."""

import time

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi import log


class SessionManager(object):
    """A manager for sessions.

    :param TxRedisManager redis:
        Redis manager object.
    :param int max_session_length:
        Time before a session expires. Default is None (never expire).
    :param float gc_period:
        Deprecated and ignored.
    """

    def __init__(self, redis, max_session_length=None, gc_period=None):
        self.max_session_length = max_session_length
        self.redis = redis
        if gc_period is not None:
            log.warning("SessionManager 'gc_period' parameter is deprecated.")

    @inlineCallbacks
    def stop(self, stop_redis=True):
        if stop_redis:
            yield self.redis._close()

    @classmethod
    def from_redis_config(cls, config, key_prefix=None,
                          max_session_length=None, gc_period=None):
        """Create a `SessionManager` instance using `TxRedisManager`.
        """
        from vumi.persist.txredis_manager import TxRedisManager
        d = TxRedisManager.from_config(config)
        if key_prefix is not None:
            d.addCallback(lambda m: m.sub_manager(key_prefix))
        return d.addCallback(lambda m: cls(m, max_session_length, gc_period))

    @inlineCallbacks
    def active_sessions(self):
        """Return a list of active user_ids and associated sessions.

        Queries redis for keys starting with the session key prefix. This is
        O(n) over the total number of keys in redis, but this is still pretty
        quick even for millions of keys. Try not to hit this too often, though.
        """
        keys = yield self.redis.keys('session:*')
        sessions = []
        for user_id in [key.split(':', 1)[1] for key in keys]:
            sessions.append((user_id, (yield self.load_session(user_id))))

        returnValue(sessions)

    def load_session(self, user_id):
        """
        Load session data from Redis
        """
        ukey = "%s:%s" % ('session', user_id)
        return self.redis.hgetall(ukey)

    def schedule_session_expiry(self, user_id, timeout):
        """
        Schedule a session to timeout

        Parameters
        ----------
        user_id : str
            The user's id.
        timeout : int
            The number of seconds after which this session should expire
        """
        ukey = "%s:%s" % ('session', user_id)
        return self.redis.expire(ukey, timeout)

    @inlineCallbacks
    def create_session(self, user_id, **kwargs):
        """
        Create a new session using the given user_id
        """
        yield self.clear_session(user_id)
        defaults = {
            'created_at': time.time()
        }
        defaults.update(kwargs)
        yield self.save_session(user_id, defaults)
        if self.max_session_length:
            yield self.schedule_session_expiry(user_id,
                                               int(self.max_session_length))
        returnValue((yield self.load_session(user_id)))

    def clear_session(self, user_id):
        ukey = "%s:%s" % ('session', user_id)
        return self.redis.delete(ukey)

    @inlineCallbacks
    def save_session(self, user_id, session):
        """
        Save a session

        Parameters
        ----------
        user_id : str
            The user's id.
        session : dict
            The session info, nested dictionaries are not supported. Any
            values that are dictionaries are converted to strings by Redis.

        """
        ukey = "%s:%s" % ('session', user_id)
        for s_key, s_value in session.items():
            yield self.redis.hset(ukey, s_key, s_value)
        returnValue(session)
