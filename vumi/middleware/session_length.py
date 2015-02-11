# -*- test-case-name: vumi.middleware.tests.test_session_length -*-

import time

from twisted.internet.defer import inlineCallbacks

from vumi.message.TransportUserMessage import SESSION_NEW, SESSION_CLOSE
from vumi.middleware.base import BaseMiddleware
from vumi.persist.txredis_manager import TxRedisManager


class SessionLengthMiddleware(BaseMiddleware):
    """ Middleware for storing the session length in the message.

    Session length is stored if the end of the session is reached.

    Configuration option:

    :param dict redis:
        Redis configuration parameters.
    """
    @inlineCallbacks
    def setup_middleware(self):
        r_config = self.config.get('redis_manager', {})
        self.redis = yield TxRedisManager.from_config(r_config)

    @inlineCallbacks
    def teardown_middleware(self):
        yield self.redis.close_manager()

    def handle_event(self, event, connector_name):
        if event.get('event_type') == SESSION_NEW:
            self.redis.set(
                '%s:%s' % (event.get('from_addr'), 'session_created'),
                str(time.time()))
        elif event.get('event_type') == SESSION_CLOSE:
            created_time = self.redis.get(
                '%s:%s' % (event.get('from_addr'), 'session_created'))
            if created_time:
                created_time = float(created_time)
                time_diff = time.time() - created_time
                event['session_length'] = time_diff
        return event
