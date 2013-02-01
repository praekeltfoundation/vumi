# -*- test-case-name: vumi.application.tests.test_base -*-

"""Basic tools for building a vumi ApplicationWorker."""

import copy
import warnings

from twisted.internet.defer import inlineCallbacks, succeed
from twisted.python import log

from vumi.service import Worker
from vumi.errors import ConfigError
from vumi.message import TransportUserMessage
from vumi.middleware import setup_middlewares_from_config
from vumi.endpoints import ReceiveInboundConnector


SESSION_NEW = TransportUserMessage.SESSION_NEW
SESSION_CLOSE = TransportUserMessage.SESSION_CLOSE
SESSION_RESUME = TransportUserMessage.SESSION_RESUME


class ApplicationWorker(Worker):
    """Base class for an application worker.

    Handles :class:`vumi.message.TransportUserMessage` and
    :class:`vumi.message.TransportEvent` messages.

    Application workers may send outgoing messages using
    :meth:`reply_to` (for replies to incoming messages) or
    :meth:`send_to` (for messages that are not replies).

    Messages sent via :meth:`send_to` pass optional additional data
    from configuration to the TransportUserMessage constructor, based
    on the tag parameter passed to send_to. This usually contains
    information useful for routing the message.

    An example :meth:`send_to` configuration might look like::

      - send_to:
        - default:
          transport_name: sms_transport

    Currently 'transport_name' **must** be defined for each send_to
    section since all existing dispatchers rely on this for routing
    outbound messages.

    The available tags are defined by the :attr:`SEND_TO_TAGS` class
    attribute. Sub-classes must override this attribute with a set of
    tag names if they wish to use :meth:`send_to`. If applications
    have only a single tag, it is suggested to name that tag `default`
    (this makes calling `send_to` easier since the value of the tag
    parameter may be omitted).

    By default :attr:`SEND_TO_TAGS` is empty and all calls to
    :meth:`send_to` will fail (this is to make it easy to identify
    which tags an application requires `send_to` configuration for).
    """

    transport_name = None
    start_message_consumer = True

    SEND_TO_TAGS = frozenset([])

    @inlineCallbacks
    def startWorker(self):
        log.msg('Starting a %s worker with config: %s'
                % (self.__class__.__name__, self.config))
        self._connectors = {}
        self._validate_config()
        self.transport_name = self.config['transport_name']
        self.amqp_prefetch_count = self.config.get('amqp_prefetch_count', 20)
        self.send_to_options = self.config.get('send_to', {})

        self._event_handlers = {
            'ack': self.consume_ack,
            'nack': self.consume_nack,
            'delivery_report': self.consume_delivery_report,
            }
        self._session_handlers = {
            SESSION_NEW: self.new_session,
            SESSION_CLOSE: self.close_session,
            }

        yield self.setup_transport_connector()

        yield self.setup_middleware()

        yield self.setup_application()

        # Apply pre-fetch limits if we need to.
        if self.amqp_prefetch_count is not None:
            yield self.setup_amqp_qos()

        if self.start_message_consumer:
            yield self._setup_transport_consumer()

    @inlineCallbacks
    def stopWorker(self):
        for connector_name in self._connectors.keys():
            connector = self._connectors.pop(connector_name)
            yield connector.teardown()
        yield self.teardown_application()

    def _validate_config(self):
        if 'transport_name' not in self.config:
            raise ConfigError("Missing 'transport_name' field in config.")
        send_to_options = self.config.get('send_to', {})
        for tag in self.SEND_TO_TAGS:
            if tag not in send_to_options:
                raise ConfigError("No configuration for send_to tag %r but"
                                  " at least a transport_name is required."
                                  % (tag,))
            if 'transport_name' not in send_to_options[tag]:
                raise ConfigError("The configuration for send_to tag %r must"
                                  " contain a transport_name." % (tag,))

        return self.validate_config()

    def validate_config(self):
        """
        Application-specific config validation happens in here.

        Subclasses may override this method to perform extra config validation.
        """
        pass

    def setup_application(self):
        """
        All application specific setup should happen in here.

        Subclasses should override this method to perform extra setup.
        """
        pass

    def teardown_application(self):
        """
        Clean-up of setup done in setup_application should happen here.
        """
        pass

    @inlineCallbacks
    def setup_middleware(self):
        """
        Middleware setup happens here.

        Subclasses should not override this unless they need to do nonstandard
        middleware setup.
        """
        # TODO: Make this more flexible
        middlewares = yield setup_middlewares_from_config(self, self.config)
        for connector in self._connectors.values():
            connector.set_middlewares(middlewares)

    def _dispatch_event_raw(self, event):
        event_type = event.get('event_type')
        handler = self._event_handlers.get(event_type,
                                           self.consume_unknown_event)
        return handler(event)

    def dispatch_event(self, event):
        """Dispatch to event_type specific handlers."""
        return self._dispatch_event_raw(event)

    def consume_unknown_event(self, event):
        log.msg("Unknown event type in message %r" % (event,))

    def consume_ack(self, event):
        """Handle an ack message."""
        pass

    def consume_nack(self, event):
        """Handle a nack message"""
        pass

    def consume_delivery_report(self, event):
        """Handle a delivery report."""
        pass

    def _dispatch_user_message_raw(self, message):
        session_event = message.get('session_event')
        handler = self._session_handlers.get(session_event,
                                             self.consume_user_message)
        return handler(message)

    def dispatch_user_message(self, message):
        """Dispatch user messages to handler."""
        return self._dispatch_user_message_raw(message)

    def consume_user_message(self, message):
        """Respond to user message."""
        pass

    def new_session(self, message):
        """Respond to a new session.

        Defaults to calling consume_user_message.
        """
        return self.consume_user_message(message)

    def close_session(self, message):
        """Close a session.

        The .reply_to() method should not be called when the session is closed.
        """
        pass

    def _publish_message(self, message, endpoint_name=None):
        publisher = self._connectors[self.transport_name]
        return publisher.publish_outbound(message, endpoint_name=endpoint_name)

    def reply_to(self, original_message, content, continue_session=True,
                 **kws):
        reply = original_message.reply(content, continue_session, **kws)
        endpoint_name = original_message.get_routing_endpoint()
        return self._publish_message(reply, endpoint_name=endpoint_name)

    def reply_to_group(self, original_message, content, continue_session=True,
                       **kws):
        reply = original_message.reply_group(content, continue_session, **kws)
        return self._publish_message(reply)

    def send_to(self, to_addr, content, tag='default', **kw):
        # FIXME: Do we do the right stuff here?
        if tag not in self.SEND_TO_TAGS:
            raise ValueError("Tag %r not defined in SEND_TO_TAGS" % (tag,))
        options = copy.deepcopy(self.send_to_options[tag])
        options.update(kw)
        msg = TransportUserMessage.send(to_addr, content, **options)
        return self._publish_message(msg)

    def setup_connector(self, connector_name):
        if connector_name in self._connectors:
            log.warning("Connector %r already set up." % (connector_name,))
            # So we always get a deferred from here.
            return succeed(self._connectors[connector_name])
        connector = ReceiveInboundConnector(self, connector_name)
        self._connectors[connector_name] = connector
        d = connector.setup()
        return d.addCallback(lambda r: connector)

    @inlineCallbacks
    def setup_transport_connector(self):
        connector = yield self.setup_connector(self.transport_name)
        connector.set_inbound_handler(self.dispatch_user_message)
        connector.set_event_handler(self.dispatch_event)

    @inlineCallbacks
    def setup_transport_connection(self, endpoint_name, transport_name,
                                   message_consumer, event_consumer):
        warnings.warn(
            "setup_transport_connection() is deprecated. Use connectors and"
            " endpoints instead.", category=DeprecationWarning)

        connector = yield self.setup_connector(transport_name)
        connector.set_inbound_handler(message_consumer)
        connector.set_event_handler(event_consumer)

        # FIXME: We probably shouldn't allow this access here.
        setattr(self, '%s_publisher' % (endpoint_name,),
                connector._publishers['outbound'])
        setattr(self, '%s_consumer' % (endpoint_name,),
                connector._consumers['inbound'])
        setattr(self, '%s_event_consumer' % (endpoint_name,),
                connector._consumers['event'])

    def _setup_transport_publisher(self):
        warnings.warn(
            "_setup_transport_publisher() is deprecated. Use connectors and"
            " endpoints instead.", category=DeprecationWarning)
        return self.setup_transport_connector()

    def _setup_transport_consumer(self):
        return self.unpause_connectors()

    def pause_connectors(self):
        for connector in self._connectors.values():
            connector.pause()

    def unpause_connectors(self):
        for connector in self._connectors.values():
            connector.unpause()

    def _setup_event_consumer(self):
        return self.transport_event_consumer.unpause()

    @inlineCallbacks
    def setup_amqp_qos(self):
        for conn in self._connectors.values():
            yield conn.set_consumer_prefetch(int(self.amqp_prefetch_count))
