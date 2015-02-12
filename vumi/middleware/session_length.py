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

    def _set_message_billing_metadata(self, message, field, value):
        if not message['helper_metadata'].get('billing'):
            message['helper_metadata']['billing'] = {}
        message['helper_metadata']['billing'][field] = value

    def _set_session_start_time(self, message, time):
        self._set_message_billing_metadata(message, 'session_start', time)

    def _set_session_end_time(self, message, time):
        self._set_message_billing_metadata(message, 'session_end', time)

    @inlineCallbacks
    def _process_message(self, message, redis_key):
        if message.get('event_type') == self.SESSION_NEW:
            start_time = time.time()
            yield self.redis.set(redis_key, str(start_time))
            self._set_session_start_time(message, start_time)
        elif message.get('event_type') == self.SESSION_CLOSE:
            self._set_session_end_time(message, time.time())
            created_time = yield self.redis.get(redis_key)
            if created_time:
                self._set_session_start_time(message, float(created_time))
                yield self.redis.delete(redis_key)
        else:
            created_time = yield self.redis.get(redis_key)
            if created_time:
                self._set_session_start_time(message, float(created_time))

    @inlineCallbacks
    def handle_inbound(self, message, connector_name):
        redis_key = '%s:%s' % (message.get('from_addr'), 'session_created')
        yield self._process_message(message, redis_key)
        returnValue(message)

    @inlineCallbacks
    def handle_outbound(self, message, connector_name):
        redis_key = '%s:%s' % (message.get('to_addr'), 'session_created')
        yield self._process_message(message, redis_key)
        returnValue(message)
