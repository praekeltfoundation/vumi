# -*- test-case-name: vumi.persist.tests.test_session -*-

"""Session management utilities."""

import time

from twisted.internet import task
from twisted.internet.defer import inlineCallbacks, returnValue


class SessionManager(object):
    """A manager for sessions.

    :type r_server: redis.Redis
    :param r_server:
        Redis db connection.
    :type prefix: str
    :param prefix:
        Prefix to use for Redis keys.
    :type max_session_length: int
    :param max_session_length:
        Time before a session expires. Default is None (never expire).
    :type gc_period: float
    :param gc_period:
        Time in seconds between checking for session expiry.
    """

    def __init__(self, manager, max_session_length=None,
                 gc_period=1.0):
        self.max_session_length = max_session_length
        self.manager = manager

        self.gc = task.LoopingCall(lambda: self.active_sessions())
        self.gc.start(gc_period)

    @inlineCallbacks
    def stop(self, stop_manager=True):
        if self.gc.running:
            yield self.gc.stop()
        if stop_manager:
            yield self.manager._close()

    @classmethod
    def from_redis_config(cls, config, key_prefix='', max_session_length=None,
                          gc_period=1.0):
        """Create a `SessionManager` instance using `TxRedisManager`.
        """
        from vumi.persist.txredis_manager import TxRedisManager
        d = TxRedisManager.from_config(config, key_prefix)
        return d.addCallback(lambda m: cls(m, max_session_length, gc_period))

    @inlineCallbacks
    def active_sessions(self):
        """
        Return a list of active user_ids and associated sessions. Loops over
        known active_sessions, some of which might have auto expired.
        Implements lazy garbage collection, for each entry it checks if
        the user's session still exists, if not it is removed from the set.
        """
        skey = 'active_sessions'
        sessions = []
        sessions_to_expire = []
        for user_id in (yield self.manager.smembers(skey)):
            ukey = "%s:%s" % ('session', user_id)
            if (yield self.manager.exists(ukey)):
                sessions.append((user_id, (yield self.load_session(user_id))))
            else:
                sessions_to_expire.append(user_id)

        # clear empty ones
        for user_ids in sessions_to_expire:
            yield self.manager.srem(skey, user_id)

        returnValue(sessions)

    def load_session(self, user_id):
        """
        Load session data from Redis
        """
        ukey = "%s:%s" % ('session', user_id)
        return self.manager.hgetall(ukey)

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
        return self.manager.expire(ukey, timeout)

    @inlineCallbacks
    def create_session(self, user_id, **kwargs):
        """
        Create a new session using the given user_id
        """
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
        return self.manager.delete(ukey)

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
            yield self.manager.hset(ukey, s_key, s_value)
        skey = 'active_sessions'
        yield self.manager.sadd(skey, user_id)
        returnValue(session)
