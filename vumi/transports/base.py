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
    """
    Base class for transport workers.

    The following attributes are available for subclasses to control behaviour:

    * :attr:`start_message_consumer` -- Set to ``False`` if the message
      consumer should not be started. The subclass is responsible for starting
      it in this case.
    """

    SUPPRESS_FAILURE_EXCEPTIONS = True

    transport_name = None
    start_message_consumer = True

    @inlineCallbacks
    def startWorker(self):
        """
        Set up basic transport worker stuff.

        You shouldn't have to override this in subclasses.
        """
        self._consumers = []

        self._validate_config()
        if 'TRANSPORT_NAME' in self.config:
            log.msg("NOTE: 'TRANSPORT_NAME' in config is deprecated. "
                    "Use 'transport_name' instead.")
            self.config.setdefault('transport_name',
                                   self.config['TRANSPORT_NAME'])
        self.transport_name = self.config['transport_name']
        self.concurrent_sends = self.config.get('concurrent_sends')

        yield self._setup_failure_publisher()
        yield self._setup_message_publisher()
        yield self._setup_event_publisher()

        yield self.setup_transport()

        self.message_consumer = None
        if self.start_message_consumer:
            yield self._setup_message_consumer()

    @inlineCallbacks
    def stopWorker(self):
        while self._consumers:
            consumer = self._consumers.pop()
            yield consumer.stop()
        yield self.teardown_transport()

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

        Subclasses may override this method to perform extra config validation.
        """
        pass

    def setup_transport(self):
        """
        All transport_specific setup should happen in here.

        Subclasses should override this method to perform extra setup.
        """
        pass

    def teardown_transport(self):
        """
        Clean-up of setup done in setup_transport should happen here.
        """
        pass

    @inlineCallbacks
    def _setup_message_publisher(self):
        self.message_publisher = yield self.publish_rkey('inbound')

    @inlineCallbacks
    def _setup_message_consumer(self):
        if self.message_consumer is not None:
            log.msg("Consumer already exists, not restarting.")
            return

        self.message_consumer = yield self.consume(
            self.get_rkey('outbound'), self._process_message,
            message_class=TransportUserMessage)
        self._consumers.append(self.message_consumer)

        # Apply concurrency throttling if we need to.
        if self.concurrent_sends is not None:
            yield self.message_consumer.channel.basic_qos(
                0, int(self.concurrent_sends), False)

    def _teardown_message_consumer(self):
        if self.message_consumer is None:
            log.msg("Consumer does not exist, not stopping.")
            return
        self._consumers.remove(self.message_consumer)
        consumer, self.message_consumer = self.message_consumer, None
        return consumer.stop()

    @inlineCallbacks
    def _setup_event_publisher(self):
        self.event_publisher = yield self.publish_rkey('event')

    @inlineCallbacks
    def _setup_failure_publisher(self):
        self.failure_publisher = yield self.publish_rkey('failures')

    def send_failure(self, message, exception, traceback):
        """Send a failure report."""
        try:
            failure_code = getattr(exception, "failure_code",
                                   FailureMessage.FC_UNSPECIFIED)
            self.failure_publisher.publish_message(FailureMessage(
                    message=message.payload, failure_code=failure_code,
                    reason=traceback))
            self.failure_published()
        except:
            log.err("Error publishing failure: %s, %s, %s"
                    % (message, exception, traceback))
            raise

    def failure_published(self):
        pass

    def publish_message(self, **kw):
        """
        Publish a :class:`TransportUserMessage` message.

        Some default parameters are handled, so subclasses don't have
        to provide a lot of boilerplate.
        """
        kw.setdefault('transport_name', self.transport_name)
        kw.setdefault('transport_metadata', {})
        return self.message_publisher.publish_message(
            TransportUserMessage(**kw))

    def publish_event(self, **kw):
        """
        Publish a :class:`TransportEvent` message.

        Some default parameters are handled, so subclasses don't have
        to provide a lot of boilerplate.
        """
        kw.setdefault('transport_name', self.transport_name)
        kw.setdefault('transport_metadata', {})
        return self.event_publisher.publish_message(TransportEvent(**kw))

    def publish_ack(self, user_message_id, sent_message_id, **kw):
        """
        Helper method for publishing an ``ack`` event.
        """
        return self.publish_event(user_message_id=user_message_id,
                                  sent_message_id=sent_message_id,
                                  event_type='ack', **kw)

    def publish_delivery_report(self, user_message_id, delivery_status, **kw):
        """
        Helper method for publishing a ``delivery_report`` event.
        """
        return self.publish_event(user_message_id=user_message_id,
                                  delivery_status=delivery_status,
                                  event_type='delivery_report', **kw)

    def _process_message(self, message):
        def _send_failure(f):
            self.send_failure(message, f.value, f.getTraceback())
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

    @staticmethod
    def generate_message_id():
        """
        Generate a message id.
        """

        return TransportUserMessage.generate_id()
