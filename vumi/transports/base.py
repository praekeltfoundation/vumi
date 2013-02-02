# -*- test-case-name: vumi.transports.tests.test_base -*-

"""
Common infrastructure for transport workers.

This is likely to get used heavily fast, so try get your changes in early.
"""

from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.config import Config, ConfigString, ConfigInt
from vumi.message import TransportUserMessage, TransportEvent
from vumi.service import Worker
from vumi.transports.failures import FailureMessage
from vumi.middleware import MiddlewareStack, setup_middlewares_from_config


class TransportConfig(Config):
    """Base config definition for transports.

    You should subclass this and add transport-specific fields.
    """

    transport_name = ConfigString(
        "The name this transport instance will use to create its queues.",
        required=True)
    amqp_prefetch_count = ConfigInt(
        "The number of messages processed concurrently from each AMQP queue.")


class Transport(Worker):
    """
    Base class for transport workers.

    The following attributes are available for subclasses to control behaviour:

    * :attr:`start_message_consumer` -- Set to ``False`` if the message
      consumer should not be started. The subclass is responsible for starting
      it in this case.
    """

    SUPPRESS_FAILURE_EXCEPTIONS = True
    CONFIG_CLASS = TransportConfig

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

        # TODO: These have been deprecated long enough, methinks.
        if 'TRANSPORT_NAME' in self.config:
            log.msg("NOTE: 'TRANSPORT_NAME' in config is deprecated. "
                    "Use 'transport_name' instead.")
            self.config.setdefault('transport_name',
                                   self.config['TRANSPORT_NAME'])

        if 'concurrent_sends' in self.config:
            log.msg("NOTE: 'concurrent_sends' in config is deprecated. "
                    "use 'amqp_prefetch_count' instead.")
            self.config.setdefault('amqp_prefetch_count',
                                    self.config['concurrent_sends'])

        config = self.CONFIG_CLASS(self.config)
        self.transport_name = config.transport_name
        self.amqp_prefetch_count = config.amqp_prefetch_count

        yield self.setup_transport_connection()
        yield self.setup_middleware()
        yield self.setup_transport()

        # Apply concurrency throttling if we need to.
        if self.amqp_prefetch_count is not None:
            yield self.setup_amqp_qos()

        if self.start_message_consumer:
            yield self.message_consumer.unpause()

    @inlineCallbacks
    def stopWorker(self):
        while self._consumers:
            consumer = self._consumers.pop()
            yield consumer.stop()
        yield self.teardown_transport()
        yield self.teardown_middleware()

    def get_rkey(self, mtype):
        return '%s.%s' % (self.transport_name, mtype)

    def publish_rkey(self, name):
        return self.publish_to(self.get_rkey(name))

    def _validate_config(self):
        # We assume that all required fields will either come from the base
        # config or will have placeholder values that don't fail validation.
        # This object is only created to trigger validation.
        # TODO: Eventually we'll be able to remove the legacy validate_config()
        # and just use config objects.
        self.CONFIG_CLASS(self.config)
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

    @inlineCallbacks
    def setup_amqp_qos(self):
        for consumer in self._consumers:
            yield consumer.channel.basic_qos(0, int(self.amqp_prefetch_count),
                                                False)

    def teardown_transport(self):
        """
        Clean-up of setup done in setup_transport should happen here.
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

    @inlineCallbacks
    def setup_transport_connection(self):
        self.message_consumer = yield self.consume(
            self.get_rkey('outbound'), self._process_message,
            message_class=TransportUserMessage, paused=True)
        self._consumers.append(self.message_consumer)

        # Set up publishers
        self.message_publisher = yield self.publish_rkey('inbound')
        self.event_publisher = yield self.publish_rkey('event')
        self.failure_publisher = yield self.publish_rkey('failures')

    def send_failure(self, message, exception, traceback):
        """Send a failure report."""
        try:
            failure_code = getattr(exception, "failure_code",
                                   FailureMessage.FC_UNSPECIFIED)
            failure_msg = FailureMessage(
                    message=message.payload, failure_code=failure_code,
                    reason=traceback)
            d = self._middlewares.apply_publish("failure", failure_msg,
                                                self.transport_name)
            d.addCallback(self.failure_publisher.publish_message)
            d.addCallback(lambda _f: self.failure_published())
        except:
            log.err("Error publishing failure: %s, %s, %s"
                    % (message, exception, traceback))
            raise
        return d

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
        msg = TransportUserMessage(**kw)
        d = self._middlewares.apply_publish("inbound", msg,
                                            self.transport_name)
        d.addCallback(self.message_publisher.publish_message)
        return d

    def publish_event(self, **kw):
        """
        Publish a :class:`TransportEvent` message.

        Some default parameters are handled, so subclasses don't have
        to provide a lot of boilerplate.
        """
        kw.setdefault('transport_name', self.transport_name)
        kw.setdefault('transport_metadata', {})
        event = TransportEvent(**kw)
        d = self._middlewares.apply_publish("event", event,
                                            self.transport_name)
        d.addCallback(self.event_publisher.publish_message)
        return d

    def publish_ack(self, user_message_id, sent_message_id, **kw):
        """
        Helper method for publishing an ``ack`` event.
        """
        return self.publish_event(user_message_id=user_message_id,
                                  sent_message_id=sent_message_id,
                                  event_type='ack', **kw)

    def publish_nack(self, user_message_id, reason, **kw):
        """
        Helper method for publishing a ``nack`` event.
        """
        return self.publish_event(user_message_id=user_message_id,
                                  nack_reason=reason, event_type='nack', **kw)

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
            log.err(f)
            if self.SUPPRESS_FAILURE_EXCEPTIONS:
                return None
            return f

        d = self._middlewares.apply_consume("outbound", message,
                                            self.transport_name)
        d.addCallback(self.handle_outbound_message)
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
