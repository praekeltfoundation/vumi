"""Basic tools for building a vumi ApplicationWorker."""

from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.service import Worker
from vumi.message import TransportUserMessage, TransportEvent


class ApplicationWorker(Worker):
    """Base class for an application worker.

    Handles :class:`vumi.message.TransportUserMessage` and
    :class:`vumi.message.TransportEvent` messages.
    """

    @inlineCallbacks
    def startWorker(self):
        log.msg('Starting a %s worker with config: %s'
                % (self.__class__.__name__,  self.config))
        self.transport_publisher = yield self.publish_to(
            '%(transport_name)s.outbound' % self.config)
        self.transport_consumer = yield self.consume(
            '%(transport_name)s.inbound' % self.config,
            self.consume_message)
        self._handlers = {
            TransportUserMessage.MESSAGE_TYPE: self.consume_user_message,
            TransportEvent.MESSAGE_TYPE: self.consume_event,
            }
        self._event_handlers = {
            'ack': self.consume_ack,
            'delivery_report': self.consume_delivery_report,
            }

    def consume_message(self, message):
        """Dispatch a message."""
        message_type = message.get('message_type')
        handler = self._handlers.get(message_type)
        if handler is None:
            return self.consume_bad_message(message)
        return handler(message)

    def consume_bad_message(self, message):
        log.err("Unexpected message type in message %r" % (message,))

    def consume_event(self, event):
        """Dispatch to event_type specific handlers."""
        event_type = event.get('event_type')
        handler = self._event_handlers.get(event_type)
        if handler is None:
            return self.consume_unknown_event(event)
        return handler(event)

    def consume_unknown_event(self, event):
        log.msg("Unknown event type in message %r" % (event,))

    def reply_to(self, original_message, content, continue_session=True,
                 **kws):
        reply = original_message.reply(content, continue_session, **kws)
        self.transport_publisher.publish_message(reply)

    def consume_user_message(self, message):
        """Respond to user message."""
        pass

    def consume_ack(self, event):
        """Handle an ack message."""
        pass

    def consume_delivery_report(self, event):
        """Handle a delivery report."""
        pass
