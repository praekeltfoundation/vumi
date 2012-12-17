# -*- test-case-name: vumi.components.tests.test_session -*-

"""Session management utilities."""

import time

from twisted.internet import task
from twisted.internet.defer import inlineCallbacks, returnValue


class SessionManager(object):
    """A manager for sessions.

    :param TxRedisManager redis:
        Redis manager object.
    :param int max_session_length:
        Time before a session expires. Default is None (never expire).
    :param float gc_period:
        Time in seconds between checking for session expiry. Set to zero to
        disable the garbage collection at regular intervals. This will result
        in the garbage collection being done purely lazy when `active_sessions`
        is called.
    """

    def __init__(self, redis, max_session_length=None, gc_period=1.0):
        self.max_session_length = max_session_length
        self.redis = redis

        self._session_created = True  # Start True so that GC runs at startup.
        self.gc = task.LoopingCall(self._active_session_gc)
        if gc_period:
            self.gc_done = self.gc.start(gc_period)

    @inlineCallbacks
    def stop(self, stop_redis=True):
        if self.gc.running:
            self.gc.stop()
            yield self.gc_done
        if stop_redis:
            yield self.redis._close()

    @classmethod
    def from_redis_config(cls, config, key_prefix=None,
                          max_session_length=None, gc_period=1.0):
        """Create a `SessionManager` instance using `TxRedisManager`.
        """
        from vumi.persist.txredis_manager import TxRedisManager
        d = TxRedisManager.from_config(config)
        if key_prefix is not None:
            d.addCallback(lambda m: m.sub_manager(key_prefix))
        return d.addCallback(lambda m: cls(m, max_session_length, gc_period))

    def _active_session_gc(self):
        """
        Garbage-collect expired sessions in the active session set.

        This checks the value of :attr:`_session_created` and only runs the
        garbage collection if it is ``False``.
        """
        if self._session_created:
            self._session_created = False
            return self.active_sessions()

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
        for user_id in (yield self.redis.smembers(skey)):
            ukey = "%s:%s" % ('session', user_id)
            if (yield self.redis.exists(ukey)):
                sessions.append((user_id, (yield self.load_session(user_id))))
            else:
                sessions_to_expire.append(user_id)

        # clear empty ones
        for user_ids in sessions_to_expire:
            yield self.redis.srem(skey, user_id)

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
        defaults = {
            'created_at': time.time()
        }
        defaults.update(kwargs)
        yield self.save_session(user_id, defaults)
        if self.max_session_length:
            yield self.schedule_session_expiry(user_id,
                                               int(self.max_session_length))
        self._session_created = True
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
        skey = 'active_sessions'
        yield self.redis.sadd(skey, user_id)
        returnValue(session)
