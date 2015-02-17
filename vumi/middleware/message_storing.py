# -*- test-case-name: vumi.middleware.tests.test_message_storing -*-

from confmodel.fields import ConfigBool, ConfigDict, ConfigText

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.middleware.base import BaseMiddleware, BaseMiddlewareConfig
from vumi.middleware.tagger import TaggingMiddleware
from vumi.components.message_store import MessageStore
from vumi.config import ConfigRiak
from vumi.persist.txriak_manager import TxRiakManager
from vumi.persist.txredis_manager import TxRedisManager


class StoringMiddlewareConfig(BaseMiddlewareConfig):
    """
    Config class for the storing middleware.
    """
    store_prefix = ConfigText(
        "Prefix for message store keys in key-value store.",
        default='message_store', static=True)
    redis_manager = ConfigDict(
        "Redis configuration parameters", default={}, static=True)
    riak_manager = ConfigRiak(
        "Riak configuration parameters. Must contain at least a bucket_prefix"
        " key", required=True, static=True)
    store_on_consume = ConfigBool(
        "``True`` to store consumed messages as well as published ones, "
        "``False`` to store only published messages.", default=True,
        static=True)


class StoringMiddleware(BaseMiddleware):
    """Middleware for storing inbound and outbound messages and events.

    Failures are not stored currently because these are typically
    stored by :class:`vumi.transports.FailureWorker` instances.

    Messages are always stored. However, in order for messages to be
    associated with a particular batch_id (
    see :class:`vumi.application.MessageStore`) a batch needs to be
    created in the message store (typically by an application worker
    that initiates sending outbound messages) and messages need to be
    tagged with a tag associated with the batch (typically by an
    application worker or middleware such as
    :class:`vumi.middleware.TaggingMiddleware`).

    Configuration options:

    :param string store_prefix:
        Prefix for message store keys in key-value store.
        Default is 'message_store'.
    :param dict redis_manager:
        Redis configuration parameters.
    :param dict riak_manager:
        Riak configuration parameters. Must contain at least
        a bucket_prefix key.
    :param bool store_on_consume:
        ``True`` to store consumed messages as well as published ones,
        ``False`` to store only published messages.
        Default is ``True``.
    """

    CONFIG_CLASS = StoringMiddlewareConfig

    @inlineCallbacks
    def setup_middleware(self):
        store_prefix = self.config.store_prefix
        r_config = self.config.redis_manager
        self.redis = yield TxRedisManager.from_config(r_config)
        manager = TxRiakManager.from_config(self.config.riak_manager)
        self.store = MessageStore(manager,
                                  self.redis.sub_manager(store_prefix))
        self.store_on_consume = self.config.store_on_consume

    @inlineCallbacks
    def teardown_middleware(self):
        yield self.redis.close_manager()

    def handle_consume_inbound(self, message, connector_name):
        if not self.store_on_consume:
            return message
        return self.handle_inbound(message, connector_name)

    @inlineCallbacks
    def handle_inbound(self, message, connector_name):
        tag = TaggingMiddleware.map_msg_to_tag(message)
        yield self.store.add_inbound_message(message, tag=tag)
        returnValue(message)

    def handle_consume_outbound(self, message, connector_name):
        if not self.store_on_consume:
            return message
        return self.handle_outbound(message, connector_name)

    @inlineCallbacks
    def handle_outbound(self, message, connector_name):
        tag = TaggingMiddleware.map_msg_to_tag(message)
        yield self.store.add_outbound_message(message, tag=tag)
        returnValue(message)

    def handle_consume_event(self, event, connector_name):
        if not self.store_on_consume:
            return event
        return self.handle_event(event, connector_name)

    @inlineCallbacks
    def handle_event(self, event, connector_name):
        transport_metadata = event.get('transport_metadata', {})
        # FIXME: The SMPP transport writes a 'datetime' object
        #        in the 'date' of the transport_metadata.
        #        json.dumps() that RiakObject uses is unhappy with that.
        if 'date' in transport_metadata:
            date = transport_metadata['date']
            if not isinstance(date, basestring):
                transport_metadata['date'] = date.isoformat()
        yield self.store.add_event(event)
        returnValue(event)
