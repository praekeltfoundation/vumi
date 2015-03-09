# -*- test-case-name: vumi.middleware.tests.test_session_length -*-

from confmodel.fields import ConfigDict, ConfigInt, ConfigText

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi import log
from vumi.errors import VumiError
from vumi.message import TransportUserMessage
from vumi.middleware.base import BaseMiddleware, BaseMiddlewareConfig
from vumi.middleware.tagger import TaggingMiddleware
from vumi.persist.txredis_manager import TxRedisManager


class SessionLengthMiddlewareError(VumiError):
    """
    Raised when the SessionLengthMiddleware encounters unexcepted messages.
    """


class SessionLengthMiddlewareConfig(BaseMiddlewareConfig):
    """
    Configuration class for the session length middleware.
    """

    redis_manager = ConfigDict("Redis config", default={}, static=True)
    timeout = ConfigInt(
        "Redis key timeout (secs)",
        default=600, static=True)
    namespace_type = ConfigText(
        "Namespace to use to lookup and set the (address, timestamp) "
        "key-value pairs in redis. Possible types: "
        "    - transport_name: the message's `transport_name` field is used."
        "    - tag: the tag associated to the message is used. *Note*: this "
        "      requires the `TaggingMiddleware` to be earlier in the "
        "      middleware chain.",
        default='transport_name', static=True)
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

    NAMESPACE_HANDLERS = {
        'tag': 'get_tag',
        'transport_name': 'get_transport_name'
    }

    @inlineCallbacks
    def setup_middleware(self):
        self.redis = yield TxRedisManager.from_config(
            self.config.redis_manager)
        self.timeout = self.config.timeout
        self.field_name = self.config.field_name
        self.clock = reactor

        namespace_type = self.config.namespace_type
        self.namespace_handler = getattr(
            self, self.NAMESPACE_HANDLERS[namespace_type])

    @inlineCallbacks
    def teardown_middleware(self):
        yield self.redis.close_manager()

    def get_transport_name(self, message):
        return message['transport_name']

    def get_tag(self, message):
        tag = TaggingMiddleware.map_msg_to_tag(message)
        if tag is None:
            return None
        else:
            return ":".join(tag)

    def _time(self):
        return self.clock.seconds()

    def _key_address(self, message, direction):
        if direction == self.DIRECTION_INBOUND:
            return message['from_addr']
        elif direction == self.DIRECTION_OUTBOUND:
            return message['to_addr']

    def _key(self, namespace, key_addr):
        return ':'.join((namespace, key_addr, 'session_created'))

    def _set_metadata(self, message, key, value):
        metadata = message['helper_metadata'].setdefault(self.field_name, {})
        metadata[key] = float(value)

    def _has_metadata(self, message, key):
        metadata = message['helper_metadata'].get(self.field_name, {})
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
    def _process_message(self, message, direction):
        namespace = self.namespace_handler(message)
        if namespace is None:
            log.error(SessionLengthMiddlewareError(
                "Session length redis namespace cannot be None, "
                "skipping message"))
            returnValue(message)

        key_addr = self._key_address(message, direction)
        if key_addr is None:
            log.error(SessionLengthMiddlewareError(
                "Session length key address cannot be None, "
                "skipping message"))
            returnValue(message)

        redis_key = self._key(namespace, key_addr)

        if message.get('session_event') == self.SESSION_NEW:
            yield self._handle_start(message, redis_key)
        elif message.get('session_event') == self.SESSION_CLOSE:
            yield self._handle_end(message, redis_key)
        else:
            yield self._handle_default(message, redis_key)

        returnValue(message)

    def handle_inbound(self, message, connector_name):
        return self._process_message(message, self.DIRECTION_INBOUND)

    def handle_outbound(self, message, connector_name):
        return self._process_message(message, self.DIRECTION_OUTBOUND)
