# -*- test-case-name: vumi.middleware.tests.test_session_length -*-

from confmodel import Config
from confmodel.fields import ConfigDict, ConfigInt, ConfigText

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.message import TransportUserMessage
from vumi.middleware.base import BaseMiddleware, BaseMiddlewareConfig
from vumi.persist.txredis_manager import TxRedisManager


class SessionLengthMiddlewareConfig(BaseMiddlewareConfig):
    """
    Configuration class for the session length middleware.
    """

    redis = ConfigDict("Redis config", default={}, static=True)
    timeout = ConfigInt("Redis key timeout (secs)", default=600, static=True)
    field_name = ConfigText(
        "Field name in message helper_metadata", default="session",
        static=True)


class SessionLengthMiddleware(BaseMiddleware):
    """ Middleware for storing the session length in the message.

    Stores the start timestamp of a session in the message payload in
    message['helper_metadata'][field_name]['session_start'] for all messages
    in the session, as well as the end timestamp if the session in
    message['helper_metadata'][field_name]['session_end'] if the message
    marks the end of the session.

    Configuration options:

    :param dict redis:
        Redis configuration parameters.
    :param int timeout:
        Redis key timeout (in seconds). Defaults to 600.
    :param str field_name:
        The field name to use when storing the timestamps in the message
        helper_metadata. Defaults to 'session'.
    """
    CONFIG_CLASS = SessionLengthMiddlewareConfig
    SESSION_NEW, SESSION_CLOSE = (
        TransportUserMessage.SESSION_NEW, TransportUserMessage.SESSION_CLOSE)

    SESSION_START = 'session_start'
    SESSION_END = 'session_end'

    @inlineCallbacks
    def setup_middleware(self):
        self.redis = yield TxRedisManager.from_config(self.config.redis)
        self.timeout = self.config.timeout
        self.field_name = self.config.field_name
        self.clock = reactor

    @inlineCallbacks
    def teardown_middleware(self):
        yield self.redis.close_manager()

    def _time(self):
        return self.clock.seconds()

    def _generate_redis_key(self, message, address):
        return '%s:%s:%s' % (
            message.get('transport_name'), address, 'session_created')

    def _set_metadata(self, message, key, value):
        metadata = message['helper_metadata'].setdefault(self.field_name, {})
        metadata[key] = float(value)

    def _has_metadata(self, message, key):
        metadata = message['helper_metadata'].setdefault(self.field_name, {})
        return key in metadata

    @inlineCallbacks
    def _set_new_start_time(self, message, redis_key, time):
        yield self.redis.setex(redis_key, self.timeout, str(time))
        self._set_metadata(message, self.SESSION_START, time)

    @inlineCallbacks
    def _set_current_start_time(self, message, redis_key, clear):
        created_time = yield self.redis.get(redis_key)

        if created_time is not None:
            self._set_metadata(message, self.SESSION_START, created_time)

            if clear:
                yield self.redis.delete(redis_key)

    def _set_end_time(self, message, time):
        self._set_metadata(message, self.SESSION_END, time)

    @inlineCallbacks
    def _handle_start(self, message, redis_key):
        if not self._has_metadata(message, self.SESSION_START):
            time = self._time()
            yield self._set_new_start_time(message, redis_key, time)

    @inlineCallbacks
    def _handle_end(self, message, redis_key):
        if not self._has_metadata(message, self.SESSION_END):
            self._set_end_time(message, self._time())

        if not self._has_metadata(message, self.SESSION_START):
            yield self._set_current_start_time(message, redis_key, clear=True)

    @inlineCallbacks
    def _handle_default(self, message, redis_key):
        if not self._has_metadata(message, self.SESSION_START):
            yield self._set_current_start_time(message, redis_key, clear=False)

    @inlineCallbacks
    def _process_message(self, message, redis_key):
        if message.get('session_event') == self.SESSION_NEW:
            yield self._handle_start(message, redis_key)
        elif message.get('session_event') == self.SESSION_CLOSE:
            yield self._handle_end(message, redis_key)
        else:
            yield self._handle_default(message, redis_key)

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
