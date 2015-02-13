# -*- test-case-name: vumi.middleware.tests.test_session_length -*-

import time

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import TransportUserMessage
from vumi.middleware.base import BaseMiddleware
from vumi.persist.txredis_manager import TxRedisManager


class SessionLengthMiddleware(BaseMiddleware):
    """ Middleware for storing the session length in the message.

    Stores the start timestamp of a session in the message payload in
    message['helper_metadata'][field_name]['session_start'] for all messages
    in the session, as well as the end timestamp if the session in
    message['helper_metadata'][field_name]['session_end'] if the message
    marks the end of the session.

    Configuration option:

    :param dict redis:
        Redis configuration parameters.
    :param int timeout:
        Redis key timeout (in seconds). Defaults to 120.
    :param str field_name:
        The field name to use when storing the timestamps in the message
        helper_metadata. Defaults to 'session'.
    """
    SESSION_NEW, SESSION_CLOSE = (
        TransportUserMessage.SESSION_NEW, TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def setup_middleware(self):
        r_config = self.config.get('redis_manager', {})
        self.redis = yield TxRedisManager.from_config(r_config)
        self.timeout = self.config.get('timeout', 120)
        self.field_name = self.config.get('field_name', 'session')

    @inlineCallbacks
    def teardown_middleware(self):
        yield self.redis.close_manager()

    def _set_message_billing_metadata(self, message, field, value):
        if not message['helper_metadata'].get(self.field_name):
            message['helper_metadata'][self.field_name] = {}
        message['helper_metadata'][self.field_name][field] = value

    def _set_session_start_time(self, message, time):
        self._set_message_billing_metadata(message, 'session_start', time)

    def _set_session_end_time(self, message, time):
        self._set_message_billing_metadata(message, 'session_end', time)

    @inlineCallbacks
    def _process_message(self, message, redis_key):
        if message.get('session_event') == self.SESSION_NEW:
            start_time = time.time()
            yield self.redis.setex(redis_key,  self.timeout, str(start_time))
            self._set_session_start_time(message, start_time)
        elif message.get('session_event') == self.SESSION_CLOSE:
            self._set_session_end_time(message, time.time())
            created_time = yield self.redis.get(redis_key)
            if created_time:
                self._set_session_start_time(message, float(created_time))
                yield self.redis.delete(redis_key)
        else:
            created_time = yield self.redis.get(redis_key)
            if created_time:
                self._set_session_start_time(message, float(created_time))

    def _generate_redis_key(self, message, address):
        return '%s:%s:%s' % (
            message.get('transport_name'), address, 'session_created')

    @inlineCallbacks
    def handle_inbound(self, message, connector_name):
        redis_key = self._generate_redis_key(message, message.get('from_addr'))
        yield self._process_message(message, redis_key)
        returnValue(message)

    @inlineCallbacks
    def handle_outbound(self, message, connector_name):
        redis_key = self._generate_redis_key(message, message.get('to_addr'))
        yield self._process_message(message, redis_key)
        returnValue(message)
