"""Basic tools for workers that handle TransportMessages."""

import warnings

from twisted.internet.defer import succeed, maybeDeferred
from twisted.python import log

from vumi.service import Worker
from vumi.middleware import setup_middlewares_from_config
from vumi.endpoints import ReceiveInboundConnector, ReceiveOutboundConnector


def then_call(d, func, *args, **kw):
    return d.addCallback(lambda r: func(*args, **kw))


def DeprecatedAttribute(object):
    def __init__(self, name):
        self.name = '_%s_value' % (name,)
        self.warned_name = '_%s_warned' % (name,)

    def check_warned(self, obj):
        if getattr(obj, self.warned_name, False):
            return
        warnings.warn(
            "Direct use of transport publishers and consumers is deprecated."
            " Use connectors and endpoints instead.",
            category=DeprecationWarning)
        setattr(obj, self.warned_name, True)

    def __get__(self, obj, cls):
        self.check_warned(obj)
        return getattr(obj, self.name)

    def __set__(self, obj, value):
        self.check_warned(obj)
        setattr(obj, self.name, value)


class BaseWorker(Worker):
    """Base class for a message processing worker.

    This contains common functionality used by application, transport and
    dispatcher workers.
    """

    DEFAULT_AMQP_PREFETCH = 20

    start_message_consumer = True

    # Some descriptors for common deprecated attributes.
    transport_publisher = DeprecatedAttribute('transport_publisher')
    transport_consumer = DeprecatedAttribute('transport_consumer')
    transport_event_consumer = DeprecatedAttribute('transport_event_consumer')
    message_publisher = DeprecatedAttribute('message_publisher')
    event_publisher = DeprecatedAttribute('event_publisher')

    def startWorker(self):
        log.msg('Starting a %s worker with config: %s'
                % (self.__class__.__name__, self.config))
        self._connectors = {}

        d = maybeDeferred(self._validate_config)
        then_call(d, self.setup_connectors)
        then_call(d, self.setup_middleware)
        then_call(d, self._worker_specific_setup)
        then_call(d, self._finish_worker_setup)
        then_call(d, self._setup_unpause)
        return d

    def setup_connectors(self):
        raise NotImplementedError()

    def _setup_unpause(self):
        raise NotImplementedError()

    def _worker_specific_setup(self):
        pass

    def _worker_specific_teardown(self):
        pass

    def _finish_worker_setup(self):
        # Apply pre-fetch limits if we need to.
        if self.amqp_prefetch_count is not None:
            self.setup_amqp_qos()

    def stopWorker(self):
        d = succeed(None)
        for connector_name in self._connectors.keys():
            connector = self._connectors.pop(connector_name)
            then_call(d, connector.teardown)
        then_call(d, self._worker_specific_teardown)
        return d

    def _validate_config(self):
        self.amqp_prefetch_count = self.config.get('amqp_prefetch_count', 20)
        return self.validate_config()

    def validate_config(self):
        """
        Application-specific config validation happens in here.

        Subclasses may override this method to perform extra config validation.
        """
        pass

    def setup_middleware(self):
        """
        Middleware setup happens here.

        Subclasses should not override this unless they need to do nonstandard
        middleware setup.
        """
        # TODO: Make this more flexible
        d = setup_middlewares_from_config(self, self.config)

        def cb(middlewares):
            for connector in self._connectors.values():
                connector.set_middlewares(middlewares)
        return d.addCallback(cb)

    def _check_for_deprecated_method(self, method_name):
        # XXX: Is there a better way to do this?
        my_stp = getattr(type(self), method_name)
        base_stp = getattr(BaseWorker, method_name)
        if my_stp == base_stp:
            return False
        warnings.warn(
            "%s() is deprecated. Use connectors and endpoints instead." % (
                method_name,), category=DeprecationWarning)
        return True

    def setup_connector(self, connector_cls, connector_name):
        if connector_name in self._connectors:
            log.warning("Connector %r already set up." % (connector_name,))
            conn = self._connectors[connector_name]
            if not isinstance(conn, connector_cls):
                log.warning("Connector %r is type %r, not %r" % (
                    connector_name, type(conn), connector_cls))
            # So we always get a deferred from here.
            return succeed(self._connectors[connector_name])
        connector = connector_cls(self, connector_name)
        self._connectors[connector_name] = connector
        d = connector.setup()
        return d.addCallback(lambda r: connector)

    def setup_ri_connector(self, connector_name):
        return self.setup_connector(ReceiveInboundConnector, connector_name)

    def setup_ro_connector(self, connector_name):
        return self.setup_connector(ReceiveOutboundConnector, connector_name)

    def each_connector(self):
        for connector in self._connectors.values():
            yield connector

    def pause_connectors(self):
        for connector in self.each_connector():
            connector.pause()

    def unpause_connectors(self):
        for connector in self.each_connector():
            connector.unpause()

    def setup_amqp_qos(self):
        for connector in self.each_connector():
            connector.set_consumer_prefetch(int(self.amqp_prefetch_count))
