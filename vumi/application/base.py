# -*- test-case-name: vumi.application.tests.test_base -*-

"""Basic tools for building a vumi ApplicationWorker."""

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
    """

    transport_name = None
    start_message_consumer = True

    @inlineCallbacks
    def startWorker(self):
        log.msg('Starting a %s worker with config: %s'
                % (self.__class__.__name__,  self.config))
        self._consumers = []
        self._validate_config()
        self.transport_name = self.config['transport_name']

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

    def reply_to(self, original_message, content, continue_session=True,
                 **kws):
        reply = original_message.reply(content, continue_session, **kws)
        self.transport_publisher.publish_message(reply)

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
