# -*- test-case-name: vumi.transports.tests.test_base -*-

"""
Common infrastructure for transport workers.

This is likely to get used heavily fast, so try get your changes in early.
"""

import warnings

from twisted.internet.defer import inlineCallbacks, succeed, maybeDeferred

from vumi import log
from vumi.errors import ConfigError
from vumi.message import TransportUserMessage, TransportEvent
from vumi.service import Worker
from vumi.transports.failures import FailureMessage
from vumi.middleware import setup_middlewares_from_config
from vumi.endpoints import ReceiveOutboundConnector


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
        self._connectors = {}

        self._validate_config()
        if 'TRANSPORT_NAME' in self.config:
            log.msg("NOTE: 'TRANSPORT_NAME' in config is deprecated. "
                    "Use 'transport_name' instead.")
            self.config.setdefault('transport_name',
                                   self.config['TRANSPORT_NAME'])
        self.transport_name = self.config['transport_name']

        if 'concurrent_sends' in self.config:
            log.msg("NOTE: 'concurrent_sends' in config is deprecated. "
                    "use 'amqp_prefetch_count' instead.")
            self.config.setdefault('amqp_prefetch_count',
                                    self.config['concurrent_sends'])
        self.amqp_prefetch_count = self.config.get('amqp_prefetch_count')

        yield self.setup_failure_publisher()
        yield self.setup_transport_connector()
        yield self.setup_middleware()
        yield self.setup_transport()

        # Apply concurrency throttling if we need to.
        if self.amqp_prefetch_count is not None:
            yield self.setup_amqp_qos()

        if self.start_message_consumer:
            yield self.unpause_transport_connector()

    @inlineCallbacks
    def stopWorker(self):
        for connector_name in self._connectors.keys():
            connector = self._connectors.pop(connector_name)
            yield connector.teardown()
        yield self.teardown_transport()

    def get_rkey(self, mtype):
        warnings.warn(
            "get_rkey() is deprecated. Use connectors and"
            " endpoints instead.", category=DeprecationWarning)
        return '%s.%s' % (self.transport_name, mtype)

    def publish_rkey(self, name):
        warnings.warn(
            "publish_rkey() is deprecated. Use connectors and"
            " endpoints instead.", category=DeprecationWarning)
        return self.publish_to(self.get_rkey(name))

    @inlineCallbacks
    def setup_failure_publisher(self):
        self.failure_publisher = yield self.publish_to(
            '%s.failures' % (self.transport_name,))

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

    @inlineCallbacks
    def setup_amqp_qos(self):
        for conn in self._connectors.values():
            yield conn.set_consumer_prefetch(int(self.amqp_prefetch_count))

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
        # TODO: Make this more flexible
        middlewares = yield setup_middlewares_from_config(self, self.config)
        for connector in self._connectors.values():
            connector.set_middlewares(middlewares)

    def setup_connector(self, connector_name):
        if connector_name in self._connectors:
            log.warning("Connector %r already set up." % (connector_name,))
            # So we always get a deferred from here.
            return succeed(self._connectors[connector_name])
        connector = ReceiveOutboundConnector(self, connector_name)
        self._connectors[connector_name] = connector
        d = connector.setup()
        return d.addCallback(lambda r: connector)

    @inlineCallbacks
    def setup_transport_connector(self):
        connector = yield self.setup_connector(self.transport_name)
        connector.set_outbound_handler(self._process_message)

    @inlineCallbacks
    def setup_transport_connection(self):
        warnings.warn(
            "setup_transport_connection() is deprecated. Use connectors and"
            " endpoints instead.", category=DeprecationWarning)
        yield self.setup_transport_connector()
        connector_pubs = self._connectors[self.transport_name]._publishers

        # Set up publishers
        self.message_publisher = connector_pubs['inbound']
        self.event_publisher = connector_pubs['event']

    def send_failure(self, message, exception, traceback):
        """Send a failure report."""
        try:
            failure_code = getattr(exception, "failure_code",
                                   FailureMessage.FC_UNSPECIFIED)
            failure_msg = FailureMessage(
                    message=message.payload, failure_code=failure_code,
                    reason=traceback)
            connector = self._connectors[self.transport_name]
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
        return self._connectors[self.transport_name].publish_inbound(msg)

    def publish_event(self, **kw):
        """
        Publish a :class:`TransportEvent` message.

        Some default parameters are handled, so subclasses don't have
        to provide a lot of boilerplate.
        """
        kw.setdefault('transport_name', self.transport_name)
        kw.setdefault('transport_metadata', {})
        event = TransportEvent(**kw)
        return self._connectors[self.transport_name].publish_event(event)

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

    def pause_transport_connector(self):
        if self.transport_name not in self._connectors:
            log.warning("Trying to pause connectors that don't exist.")
            return
        return self._connectors[self.transport_name].pause()

    def unpause_transport_connector(self):
        if self.transport_name not in self._connectors:
            log.warning("Trying to unpause connectors that don't exist.")
            return
        return self._connectors[self.transport_name].unpause()
