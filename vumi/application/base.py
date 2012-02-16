# -*- test-case-name: vumi.application.tests.test_base -*-

"""Basic tools for building a vumi ApplicationWorker."""

import copy

from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.service import Worker
from vumi.errors import ConfigError
from vumi.message import TransportUserMessage, TransportEvent


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
        self._consumers = []
        self._validate_config()
        self.transport_name = self.config['transport_name']
        self.send_to_options = self.config.get('send_to', {})

        self._event_handlers = {
            'ack': self.consume_ack,
            'delivery_report': self.consume_delivery_report,
            }
        self._session_handlers = {
            SESSION_NEW: self.new_session,
            SESSION_CLOSE: self.close_session,
            }

        yield self._setup_transport_publisher()

        yield self.setup_application()

        if self.start_message_consumer:
            yield self._setup_transport_consumer()
            yield self._setup_event_consumer()

    @inlineCallbacks
    def stopWorker(self):
        while self._consumers:
            consumer = self._consumers.pop()
            yield consumer.stop()
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

    def dispatch_event(self, event):
        """Dispatch to event_type specific handlers."""
        event_type = event.get('event_type')
        handler = self._event_handlers.get(event_type,
                                           self.consume_unknown_event)
        return handler(event)

    def consume_unknown_event(self, event):
        log.msg("Unknown event type in message %r" % (event,))

    def consume_ack(self, event):
        """Handle an ack message."""
        pass

    def consume_delivery_report(self, event):
        """Handle a delivery report."""
        pass

    def dispatch_user_message(self, message):
        """Dispatch user messages to handler."""
        session_event = message.get('session_event')
        handler = self._session_handlers.get(session_event,
                                             self.consume_user_message)
        return handler(message)

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

    def _reply_to(self, repl_func, content, continue_session=True, **kws):
        reply = repl_func(content, continue_session, **kws)
        self.transport_publisher.publish_message(reply)
        return reply

    def reply_to(self, original_message, content, continue_session=True,
                 **kws):
        return self._reply_to(
            original_message.reply, content, continue_session, **kws)

    def reply_to_group(self, original_message, content, continue_session=True,
                       **kws):
        return self._reply_to(
            original_message.reply_group, content, continue_session, **kws)

    def send_to(self, to_addr, content, tag='default', **kw):
        if tag not in self.SEND_TO_TAGS:
            raise ValueError("Tag %r not defined in SEND_TO_TAGS" % (tag,))
        options = copy.deepcopy(self.send_to_options[tag])
        options.update(kw)
        msg = TransportUserMessage.send(to_addr, content, **options)
        self.transport_publisher.publish_message(msg)
        return msg

    @inlineCallbacks
    def _setup_transport_publisher(self):
        self.transport_publisher = yield self.publish_to(
            '%(transport_name)s.outbound' % self.config)

    @inlineCallbacks
    def _setup_transport_consumer(self):
        self.transport_consumer = yield self.consume(
            '%(transport_name)s.inbound' % self.config,
            self.dispatch_user_message,
            message_class=TransportUserMessage)
        self._consumers.append(self.transport_consumer)

    @inlineCallbacks
    def _setup_event_consumer(self):
        self.transport_event_consumer = yield self.consume(
            '%(transport_name)s.event' % self.config,
            self.dispatch_event,
            message_class=TransportEvent)
        self._consumers.append(self.transport_event_consumer)
