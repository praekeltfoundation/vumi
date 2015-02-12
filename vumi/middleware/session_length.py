# -*- test-case-name: vumi.middleware.tests.test_session_length -*-

import time

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import TransportUserMessage
from vumi.middleware.base import BaseMiddleware
from vumi.persist.txredis_manager import TxRedisManager


class SessionLengthMiddleware(BaseMiddleware):
    """ Middleware for storing the session length in the message.

    Session length is stored if the end of the session is reached.

    Configuration option:

    :param dict redis:
        Redis configuration parameters.
    """
    SESSION_NEW, SESSION_CLOSE = (
        TransportUserMessage.SESSION_NEW, TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def setup_middleware(self):
        r_config = self.config.get('redis_manager', {})
        self.redis = yield TxRedisManager.from_config(r_config)

    @inlineCallbacks
    def teardown_middleware(self):
        yield self.redis.close_manager()

    @inlineCallbacks
    def handle_inbound(self, message, connector_name):
        redis_key = '%s:%s' % (message.get('from_addr'), 'session_created')
        if message.get('event_type') == self.SESSION_NEW:
            yield self.redis.set(redis_key, str(time.time()))
        elif message.get('event_type') == self.SESSION_CLOSE:
            created_time = yield self.redis.get(redis_key)
            if created_time:
                created_time = float(created_time)
                if not message['helper_metadata'].get('billing'):
                    message['helper_metadata']['billing'] = {}
                message['helper_metadata']['billing']['session_length'] = (
                    time.time() - created_time)
                yield self.redis.delete(redis_key)
        returnValue(message)
