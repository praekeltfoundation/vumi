# -*- test-case-name: vumi.middleware.tests.test_session_length -*-

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

    redis_manager = ConfigDict("Redis config", default={}, static=True)
    timeout = ConfigInt(
        "Redis key timeout (secs)",
        default=600, static=True)
    key_namespace = ConfigText(
        "Namespace to use to lookup and set the (address, timestamp) "
        "key-value pairs in redis. In none is given, the middleware will use "
        "the `transport_name` message field instead.",
        default=None, static=True)
    field_name = ConfigText(
        "Field name in message helper_metadata",
        default="session", static=True)


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

    DIRECTION_INBOUND = 'inbound'
    DIRECTION_OUTBOUND = 'outbound'

    @inlineCallbacks
    def setup_middleware(self):
        self.redis = yield TxRedisManager.from_config(
            self.config.redis_manager)
        self.key_namespace = self.config.key_namespace
        self.timeout = self.config.timeout
        self.field_name = self.config.field_name
        self.clock = reactor

    @inlineCallbacks
    def teardown_middleware(self):
        yield self.redis.close_manager()

    def _time(self):
        return self.clock.seconds()

    def _key_namespace(self, message):
        if self.key_namespace is not None:
            return self.key_namespace
        else:
            return message.get('transport_name')

    def _key_address(self, message, direction):
        if direction == self.DIRECTION_INBOUND:
            return message['from_addr']
        elif direction == self.DIRECTION_OUTBOUND:
            return message['to_addr']

    def _key(self, message, direction):
        return ':'.join((
            self._key_namespace(message),
            self._key_address(message, direction),
            'session_created'))

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
        redis_key = self._key(message, self.DIRECTION_INBOUND)
        yield self._process_message(message, redis_key)
        returnValue(message)

    @inlineCallbacks
    def handle_outbound(self, message, connector_name):
        redis_key = self._key(message, self.DIRECTION_OUTBOUND)
        yield self._process_message(message, redis_key)
        returnValue(message)
