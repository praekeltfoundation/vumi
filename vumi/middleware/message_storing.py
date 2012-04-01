# -*- test-case-name: vumi.middleware.tests.test_message_storing -*-

import redis

from vumi.middleware.base import BaseMiddleware
from vumi.middleware.tagger import TaggingMiddleware


class StoringMiddleware(BaseMiddleware):
    """
    Middleware for storing inbound and outbound messages and events.

    Failures are not stored currently because these are typically
    stored by :class:`vumi.transports.FailureWorker`s.

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
    :param dict redis:
        Redis configuration parameters.
    """

    def setup_middleware(self):
        store_prefix = self.config.get('store_prefix', 'message_store')
        r_config = self.config.get('redis', {})
        r_server = redis.Redis(**r_config)
        # import delayed to avoid circular import
        from vumi.application import MessageStore
        self.store = MessageStore(r_server, store_prefix)

    def handle_inbound(self, message, endpoint):
        tag = TaggingMiddleware.map_msg_to_tag(message)
        self.store.add_inbound_message(message, tag)

    def handle_outbound(self, message, endpoint):
        tag = TaggingMiddleware.map_msg_to_tag(message)
        self.store.add_outbound_message(message, tag)

    def handle_event(self, event, endpoint):
        self.store.add_event(event)
