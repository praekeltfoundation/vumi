# -*- test-case-name: vumi.transports.tests.test_base -*-

"""
Common infrastructure for transport workers.

This is likely to get used heavily fast, so try get your changes in early.
"""

from twisted.internet.defer import inlineCallbacks, maybeDeferred
from twisted.python import log

from vumi.errors import ConfigError
from vumi.message import TransportUserMessage, TransportEvent
from vumi.service import Worker
from vumi.transports.failures import FailureMessage


class Transport(Worker):
    SUPPRESS_FAILURE_EXCEPTIONS = True

    transport_name = None

    @inlineCallbacks
    def startWorker(self):
        """
        Set up basic transport worker stuff.

        You shouldn't have to override this in subclasses.
        """
        self._consumers = []

        self._validate_config()
        self.transport_name = self.config['transport_name']
        self.concurrent_sends = self.config.get('concurrent_sends')

        yield self._setup_failure_publisher()
        yield self._setup_message_publisher()
        yield self._setup_event_publisher()

        yield self.setup_transport()

        yield self._setup_message_consumer()

    @inlineCallbacks
    def stopWorker(self):
        for consumer in self._consumers:
            yield consumer.stop()

    def get_rkey(self, mtype):
        return '%s.%s' % (self.transport_name, mtype)

    def publish_rkey(self, name):
        return self.publish_to(self.get_rkey(name))

    def _validate_config(self):
        if 'transport_name' not in self.config:
            raise ConfigError("Missing 'transport_name' field in config.")
        return self.validate_config()

    def validate_config(self):
        """
        Transport-specific config validation happens in here.
        """
        pass

    def setup_transport(self):
        """
        All transport_specific setup should happen in here.
        """
        pass

    @inlineCallbacks
    def _setup_message_publisher(self):
        self.message_publisher = yield self.publish_rkey('inbound')

    @inlineCallbacks
    def _setup_message_consumer(self):
        self.message_consumer = yield self.consume(
            self.get_rkey('outbound'), self._process_message,
            message_class=TransportUserMessage)

        # Apply concurrency throttling if we need to.
        if self.concurrent_sends is not None:
            yield self.message_consumer.channel.basic_qos(
                0, int(self.concurrent_sends), False)

    @inlineCallbacks
    def _setup_event_publisher(self):
        self.event_publisher = yield self.publish_rkey('event')

    @inlineCallbacks
    def _setup_failure_publisher(self):
        self.failure_publisher = yield self.publish_rkey('failures')

    def send_failure(self, message, reason):
        """Send a failure report."""
        # TODO: Make the failure handling code smarter.
        try:
            self.failure_publisher.publish_message(FailureMessage(
                    message=message.payload, reason=reason))
            self.failure_published()
        except:
            log.err("Error publishing failure: %s, %s" % (message, reason))
            raise

    def failure_published(self):
        pass

    def publish_message(self, **kw):
        return self.message_publisher.publish_message(
            TransportUserMessage(**kw))

    def publish_event(self, **kw):
        return self.event_publisher.publish_message(TransportEvent(**kw))

    def publish_ack(self, **kw):
        kw.setdefault('event_type', 'ack')
        kw.setdefault('transport_name', self.transport_name)
        kw.setdefault('transport_metadata', {})
        return self.publish_event(**kw)

    def publish_delivery_report(self, **kw):
        kw.setdefault('event_type', 'delivery_report')
        kw.setdefault('transport_name', self.transport_name)
        kw.setdefault('transport_metadata', {})
        return self.publish_event(**kw)

    def _process_message(self, message):
        def _send_failure(f):
            self.send_failure(message, f.getTraceback())
            if self.SUPPRESS_FAILURE_EXCEPTIONS:
                return None
            return f
        d = maybeDeferred(self.handle_outbound_message, message)
        d.addErrback(_send_failure)
        return d

    def handle_outbound_message(self, message):
        """
        This must be overridden to read outbound messages and do the right
        thing with them.
        """
        raise NotImplementedError()
