# -*- test-case-name: vumi.application.tests.test_session -*-

"""Session management utilities for ApplicationWorkers."""

import warnings
import time

from twisted.internet import task


class SessionManager(object):
    """A manager for sessions.

    :type r_server: redis.Redis
    :param r_server:
        Redis db connection.
    :type prefix: str
    :param prefix:
        Prefix to use for Redis keys.
    :type max_session_length: float
    :param max_session_length:
        Time before a session expires. Default is None (never expire).
    :type gc_period: float
    :param gc_period:
        Time in seconds between checking for session expiry.
    """

    def __init__(self, r_server, prefix, max_session_length=None,
                 gc_period=1.0):
        warnings.warn("vumi.application.SessionManager is deprecated. Use "
              "vumi.components.session instead.", category=DeprecationWarning)
        self.max_session_length = max_session_length
        self.r_server = r_server
        self.r_prefix = prefix

        self.gc = task.LoopingCall(lambda: self.active_sessions())
        self.gc.start(gc_period)

    def stop(self):
        if self.gc.running:
            return self.gc.stop()

    def active_sessions(self):
        """
        Return a list of active user_ids and associated sessions. Loops over
        known active_sessions, some of which might have auto expired.
        Implements lazy garbage collection, for each entry it checks if
        the user's session still exists, if not it is removed from the set.
        """
        skey = self.r_key('active_sessions')
        sessions_to_expire = []
        for user_id in self.r_server.smembers(skey):
            ukey = self.r_key('session', user_id)
            if self.r_server.exists(ukey):
                yield user_id, self.load_session(user_id)
            else:
                sessions_to_expire.append(user_id)

        # clear empty ones
        for user_ids in sessions_to_expire:
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

    def create_session(self, user_id, **kwargs):
        """
        Create a new session using the given user_id
        """
        defaults = {
            'created_at': time.time()
        }
        defaults.update(kwargs)
        self.save_session(user_id, defaults)
        if self.max_session_length:
            self.schedule_session_expiry(user_id, self.max_session_length)
        return self.load_session(user_id)

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
