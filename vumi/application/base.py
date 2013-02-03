# -*- test-case-name: vumi.application.tests.test_base -*-

"""Basic tools for building a vumi ApplicationWorker."""

import copy

from twisted.internet.defer import inlineCallbacks, succeed
from twisted.python import log

from vumi.config import Config, ConfigText, ConfigInt, ConfigField
from vumi.service import Worker
from vumi.errors import ConfigError
from vumi.message import TransportUserMessage, TransportEvent
from vumi.middleware import MiddlewareStack, setup_middlewares_from_config


SESSION_NEW = TransportUserMessage.SESSION_NEW
SESSION_CLOSE = TransportUserMessage.SESSION_CLOSE
SESSION_RESUME = TransportUserMessage.SESSION_RESUME


class ApplicationConfig(Config):
    """Base config definition for transports.

    You should subclass this and add transport-specific fields.
    """

    transport_name = ConfigText(
        "The name this application instance will use to create its queues.",
        required=True)
    amqp_prefetch_count = ConfigInt(
        "The number of messages processed concurrently from each AMQP queue.",
        default=20)
    send_to = ConfigField("'send_to' configuration dict.", default={})


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

    CONFIG_CLASS = ApplicationConfig
    SEND_TO_TAGS = frozenset([])

    @inlineCallbacks
    def startWorker(self):
        log.msg('Starting a %s worker with config: %s'
                % (self.__class__.__name__, self.config))
        self._consumers = []
        self._validate_config()

        config = self.CONFIG_CLASS(self.config)
        self.transport_name = config.transport_name
        self.amqp_prefetch_count = config.amqp_prefetch_count
        self.send_to_options = config.send_to

        self._event_handlers = {
            'ack': self.consume_ack,
            'nack': self.consume_nack,
            'delivery_report': self.consume_delivery_report,
            }
        self._session_handlers = {
            SESSION_NEW: self.new_session,
            SESSION_CLOSE: self.close_session,
            }

        yield self._setup_transport_publisher()

        yield self.setup_middleware()

        yield self.setup_application()

        # Apply pre-fetch limits if we need to.
        if self.amqp_prefetch_count is not None:
            yield self._setup_amqp_qos()

        if self.start_message_consumer:
            yield self._setup_transport_consumer()
            yield self._setup_event_consumer()

    @inlineCallbacks
    def stopWorker(self):
        while self._consumers:
            consumer = self._consumers.pop()
            yield consumer.stop()
        yield self.teardown_application()
        yield self.teardown_middleware()

    def get_config(self, msg=None):
        """This should return a message-specific config object.

        It deliberately returns a deferred even when this isn't strictly
        necessary to ensure that workers will continue to work when per-message
        configuration needs to be fetched from elsewhere.
        """
        return succeed(self.CONFIG_CLASS(self.config))

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
        middlewares = yield setup_middlewares_from_config(self, self.config)
        self._middlewares = MiddlewareStack(middlewares)

    def teardown_middleware(self):
        """
        Middleware teardown happens here.

        Subclasses should not override this unless they need to do nonstandard
        middleware teardown.
        """
        return self._middlewares.teardown()

    def _dispatch_event_raw(self, event):
        event_type = event.get('event_type')
        handler = self._event_handlers.get(event_type,
                                           self.consume_unknown_event)
        return handler(event)

    def dispatch_event(self, event):
        """Dispatch to event_type specific handlers."""
        d = self._middlewares.apply_consume("event", event,
                                            self.transport_name)
        d.addCallback(self._dispatch_event_raw)
        return d

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
        d = self._middlewares.apply_consume("inbound", message,
                                            self.transport_name)
        d.addCallback(self._dispatch_user_message_raw)
        return d

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

    def _publish_message(self, message):
        d = self._middlewares.apply_publish("outbound", message,
                                            self.transport_name)
        d.addCallback(self.transport_publisher.publish_message)
        return d

    def reply_to(self, original_message, content, continue_session=True,
                 **kws):
        reply = original_message.reply(content, continue_session, **kws)
        return self._publish_message(reply)

    def reply_to_group(self, original_message, content, continue_session=True,
                       **kws):
        reply = original_message.reply_group(content, continue_session, **kws)
        return self._publish_message(reply)

    def send_to(self, to_addr, content, tag='default', **kw):
        if tag not in self.SEND_TO_TAGS:
            raise ValueError("Tag %r not defined in SEND_TO_TAGS" % (tag,))
        options = copy.deepcopy(self.send_to_options[tag])
        options.update(kw)
        msg = TransportUserMessage.send(to_addr, content, **options)
        return self._publish_message(msg)

    @inlineCallbacks
    def setup_transport_connection(self, endpoint_name, transport_name,
                                   message_consumer, event_consumer):
        consumer = yield self.consume(
            '%s.inbound' % (transport_name,), message_consumer,
            message_class=TransportUserMessage, paused=True)
        event_consumer = yield self.consume(
            '%s.event' % (transport_name,), event_consumer,
            message_class=TransportEvent, paused=True)
        publisher = yield self.publish_to('%s.outbound' % (transport_name,))

        self._consumers.extend([consumer, event_consumer])
        setattr(self, '%s_publisher' % (endpoint_name,), publisher)
        setattr(self, '%s_consumer' % (endpoint_name,), consumer)
        setattr(self, '%s_event_consumer' % (endpoint_name,), event_consumer)

    def _setup_transport_publisher(self):
        return self.setup_transport_connection(
            'transport', self.config['transport_name'],
            self.dispatch_user_message, self.dispatch_event)

    def _setup_transport_consumer(self):
        return self.transport_consumer.unpause()

    def _setup_event_consumer(self):
        return self.transport_event_consumer.unpause()

    @inlineCallbacks
    def _setup_amqp_qos(self):
        for consumer in self._consumers:
            yield consumer.channel.basic_qos(
                0, int(self.amqp_prefetch_count), False)
