# -*- test-case-name: vumi.transports.tests.test_base -*-

"""
Common infrastructure for transport workers.

This is likely to get used heavily fast, so try get your changes in early.
"""

from twisted.internet.defer import maybeDeferred

from vumi import log
from vumi.config import ConfigText
from vumi.message import TransportUserMessage, TransportEvent
from vumi.worker import BaseWorker, then_call
from vumi.transports.failures import FailureMessage


class TransportConfig(BaseWorker.CONFIG_CLASS):
    """Base config definition for transports.

    You should subclass this and add transport-specific fields.
    """

    transport_name = ConfigText(
        "The name this transport instance will use to create its queues.",
        required=True, static=True)


class Transport(BaseWorker):
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

    def _validate_config(self):
        config = self.get_static_config()
        self.transport_name = config.transport_name
        self.validate_config()

    def setup_connectors(self):
        d = self.setup_ro_connector(self.transport_name)

        def cb(connector):
            self.add_outbound_handler(
                self.handle_outbound_message, connector=connector)
            return connector

        return d.addCallback(cb)

    def setup_worker(self):
        """
        Set up basic transport worker stuff.

        You shouldn't have to override this in subclasses.
        """
        d = self.setup_failure_publisher()
        then_call(d, self.setup_transport)
        if self.start_message_consumer:
            then_call(d, self.unpause_connectors)
        return d

    def teardown_worker(self):
        d = self.pause_connectors()
        d.addCallback(lambda r: self.teardown_transport())
        return d

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

    def setup_failure_publisher(self):
        d = self.publish_to('%s.failures' % (self.transport_name,))

        def cb(publisher):
            self.failure_publisher = publisher

        return d.addCallback(cb)

    def send_failure(self, message, exception, traceback):
        """Send a failure report."""
        try:
            failure_code = getattr(exception, "failure_code",
                                   FailureMessage.FC_UNSPECIFIED)
            failure_msg = FailureMessage(
                message=message.payload, failure_code=failure_code,
                reason=traceback)
            connector = self.connectors[self.transport_name]
            d = connector._middlewares.apply_publish(
                "failure", failure_msg, self.transport_name)
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
        return self.connectors[self.transport_name].publish_inbound(msg)

    def publish_event(self, **kw):
        """
        Publish a :class:`TransportEvent` message.

        Some default parameters are handled, so subclasses don't have
        to provide a lot of boilerplate.
        """
        kw.setdefault('transport_name', self.transport_name)
        kw.setdefault('transport_metadata', {})
        event = TransportEvent(**kw)
        return self.connectors[self.transport_name].publish_event(event)

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

    def _send_failure_eb(self, f, message):
        self.send_failure(message, f.value, f.getTraceback())
        log.err(f)
        if self.SUPPRESS_FAILURE_EXCEPTIONS:
            return None
        return f

    def _make_message_processor(self, handler):
        def processor(message):
            d = maybeDeferred(handler, message)
            d.addErrback(self._send_failure_eb, message)
            return d

        return processor

    def add_outbound_handler(self, handler, endpoint_name=None,
                             connector=None):
        if connector is None:
            connector = self.connectors[self.transport_name]

        processor = self._make_message_processor(handler)
        connector.set_outbound_handler(processor, endpoint_name=endpoint_name)

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
