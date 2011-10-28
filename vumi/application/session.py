# -*- test-case-name: vumi.application.tests.test_session -*-

"""Session management utilities for ApplicationWorkers."""

import time
import redis
from twisted.internet import task


class SessionManager(object):
    """A manager for sessions.

    :type db: int
    :param db:
        Redis db number.
    :type prefix: str
    :param prefix:
        Prefix to use for Redis keys.
    :type redis_config: dict
    :param redis_config:
        Configuration options for redis.Redis. Default is None (no options).
    :type max_session_length: float
    :param max_session_length:
        Time before a session expires. Default is None (never expire).
    :type gc_period: float
    :param gc_period:
        Time in seconds between checking for session expiry.
    """

    def __init__(self, db, prefix, redis_config=None, max_session_length=None,
                 gc_period=1.0):
        self.max_session_length = max_session_length
        redis_config = redis_config if redis_config is not None else {}
        self.r_server = redis.Redis(db=db, **redis_config)
        self.r_prefix = prefix

        self.gc = task.LoopingCall(lambda: self.active_sessions())
        self.gc.start(gc_period)

    def stop(self):
        return self.gc.stop()

    def active_sessions(self):
        """
        Return a list of active user_ids and associated sessions. Loops over
        known active_sessions, some of which might have auto expired.
        Implements lazy garbage collection, for each entry it checks if
        the user's session still exists, if not it is removed from the set.
        """
        skey = self.r_key('active_sessions')
        for user_id in self.r_server.smembers(skey):
            ukey = self.r_key('session', user_id)
            if self.r_server.exists(ukey):
                yield user_id, self.load_session(user_id)
            else:
                self.r_server.srem(skey, user_id)

    def r_key(self, *args):
        """
        Generate a keyname using this workers prefix
        """
        parts = [self.r_prefix]
        parts.extend(args)
        return ":".join(parts)

    def load_session(self, user_id):
        """
        Load session data from Redis
        """
        ukey = self.r_key('session', user_id)
        return self.r_server.hgetall(ukey)

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
        ukey = self.r_key('session', user_id)
        self.r_server.expire(ukey, timeout)

    def create_session(self, user_id):
        """
        Create a new session using the given user_id
        """
        session = self.save_session(user_id, {
            'created_at': time.time()
        })
        if self.max_session_length:
            self.schedule_session_expiry(user_id, self.max_session_length)
        return session

    def clear_session(self, user_id):
        ukey = self.r_key('session', user_id)
        self.r_server.delete(ukey)

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
        ukey = self.r_key('session', user_id)
        for s_key, s_value in session.items():
            self.r_server.hset(ukey, s_key, s_value)
        skey = self.r_key('active_sessions')
        self.r_server.sadd(skey, user_id)
        return session
