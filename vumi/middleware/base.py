# -*- test-case-name: vumi.middleware.tests.test_base -*-
from confmodel import Config

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.utils import load_class_by_string
from vumi.errors import ConfigError, VumiError


class MiddlewareError(VumiError):
    pass


class BaseMiddlewareConfig(Config):
    """
    Config class for the base middleware.
    """


class BaseMiddleware(object):
    """Common middleware base class.

    This is a convenient definition of and set of common functionality
    for middleware classes. You need not subclass this and should not
    instantiate this directly.

    The :meth:`__init__` method should take exactly the following
    options so that your class can be instantiated from configuration
    in a standard way:

    :param string name: Name of the middleware.
    :param dict config: Dictionary of configuraiton items.
    :type worker: vumi.service.Worker
    :param worker:
         Reference to the transport or application being wrapped by
         this middleware.

    If you are subclassing this class, you should not override
    :meth:`__init__`. Custom setup should be done in
    :meth:`setup_middleware` instead. The config class can be overidden by
    replacing the ``config_class`` class variable.
    """
    CONFIG_CLASS = BaseMiddlewareConfig

    def __init__(self, name, config, worker):
        self.name = name
        self.config = self.CONFIG_CLASS(config, static=True)
        self.consume_priority = config.get('consume_priority')
        self.publish_priority = config.get('publish_priority')
        self.worker = worker

    def setup_middleware(self):
        """Any custom setup may be done here.

        :rtype: Deferred or None
        :returns: May return a deferred that is called when setup is
                  complete.
        """
        pass

    def teardown_middleware(self):
        """"Any custom teardown may be done here

        :rtype: Deferred or None
        :returns: May return a Deferred that is called when teardown is
                    complete
        """
        pass

    def handle_consume_inbound(self, message, connector_name):
        """Called when an inbound transport user message is consumed.

        The other methods listed below all function in the same way. Only the
        kind and direction of the message being processed differs.

         * :meth:`handle_publish_inbound`
         * :meth:`handle_consume_outbound`
         * :meth:`handle_publish_outbound`
         * :meth:`handle_consume_event`
         * :meth:`handle_publish_event`
         * :meth:`handle_failure`

        By default, the ``handle_consume_*`` and ``handle_publish_*`` methods
        call their ``handle_*`` equivalents.

        :param vumi.message.TransportUserMessage message:
            Inbound message to process.
        :param string connector_name:
            The name of the connector the message is being received on or sent
            to.
        :rtype: vumi.message.TransportUserMessage
        :returns: The processed message.
        """
        return self.handle_inbound(message, connector_name)

    def handle_publish_inbound(self, message, connector_name):
        """Called when an inbound transport user message is published.

        See :meth:`handle_consume_inbound`.
        """
        return self.handle_inbound(message, connector_name)

    def handle_inbound(self, message, connector_name):
        """Default handler for published and consumed inbound messages.

        See :meth:`handle_consume_inbound`.
        """
        return message

    def handle_consume_outbound(self, message, connector_name):
        """Called when an outbound transport user message is consumed.

        See :meth:`handle_consume_inbound`.
        """
        return self.handle_outbound(message, connector_name)

    def handle_publish_outbound(self, message, connector_name):
        """Called when an outbound transport user message is published.

        See :meth:`handle_consume_inbound`.
        """
        return self.handle_outbound(message, connector_name)

    def handle_outbound(self, message, connector_name):
        """Default handler for published and consumed outbound messages.

        See :meth:`handle_consume_inbound`.
        """
        return message

    def handle_consume_event(self, event, connector_name):
        """Called when a transport event is consumed.

        See :meth:`handle_consume_inbound`.
        """
        return self.handle_event(event, connector_name)

    def handle_publish_event(self, event, connector_name):
        """Called when a transport event is published.

        See :meth:`handle_consume_inbound`.
        """
        return self.handle_event(event, connector_name)

    def handle_event(self, event, connector_name):
        """Default handler for published and consumed events.

        See :meth:`handle_consume_inbound`.
        """
        return event

    def handle_consume_failure(self, failure, connector_name):
        """Called when a failure message is consumed.

        See :meth:`handle_consume_inbound`.
        """
        return self.handle_failure(failure, connector_name)

    def handle_publish_failure(self, failure, connector_name):
        """Called when a failure message is published.

        See :meth:`handle_consume_inbound`.
        """
        return self.handle_failure(failure, connector_name)

    def handle_failure(self, failure, connector_name):
        """Called to process a failure message (
        :class:`vumi.transports.failures.FailureMessage`).

        See :meth:`handle_consume_inbound`.
        """
        return failure


class TransportMiddleware(BaseMiddleware):
    """Message processor middleware for Transports.
    """


class ApplicationMiddleware(BaseMiddleware):
    """Message processor middleware for Applications.
    """


class MiddlewareStack(object):
    """Ordered list of middlewares to pass a Message through.
    """

    def __init__(self, middlewares):
        self.consume_middlewares = self._sort_by_priority(
            middlewares, 'consume_priority')
        self.publish_middlewares = self._sort_by_priority(
            reversed(middlewares), 'publish_priority')

    @staticmethod
    def _sort_by_priority(middlewares, priority_key):
        # We rely on Python's sorting algorithm being stable to preserve
        # order within priority levels.
        return sorted(middlewares, key=lambda mw: getattr(mw, priority_key))

    @inlineCallbacks
    def _handle(self, middlewares, handler_name, message, connector_name):
        method_name = 'handle_%s' % (handler_name,)
        for middleware in middlewares:
            handler = getattr(middleware, method_name)
            message = yield handler(message, connector_name)
            if message is None:
                raise MiddlewareError(
                    'Returned value of %s.%s should never be None' % (
                        middleware, method_name,))
        returnValue(message)

    def apply_consume(self, handler_name, message, connector_name):
        handler_name = 'consume_%s' % (handler_name,)
        return self._handle(
            self.consume_middlewares, handler_name, message, connector_name)

    def apply_publish(self, handler_name, message, connector_name):
        handler_name = 'publish_%s' % (handler_name,)
        return self._handle(
            self.publish_middlewares, handler_name, message, connector_name)

    @inlineCallbacks
    def teardown(self):
        for mw in self.publish_middlewares:
            yield mw.teardown_middleware()


def create_middlewares_from_config(worker, config):
    """Return a list of middleware objects created from a worker
       configuration.
       """
    middlewares = []
    for item in config.get("middleware", []):
        keys = item.keys()
        if len(keys) != 1:
            raise ConfigError(
                "Middleware items contain only a single key-value pair. The"
                " key should be a name for the middleware. The value should be"
                " the full dotted name of the class implementing the"
                " middleware, or a mapping containing the keys 'class' with a"
                " value of the full dotted class name, 'consume_priority' with"
                " the priority level for consuming, and 'publish_priority'"
                " with the priority level for publishing, both integers.")
        middleware_name = keys[0]
        middleware_config = config.get(middleware_name, {})
        if isinstance(item[middleware_name], basestring):
            cls_name = item[middleware_name]
            middleware_config['consume_priority'] = 0
            middleware_config['publish_priority'] = 0
        elif isinstance(item[middleware_name], dict):
            conf = item[middleware_name]
            cls_name = conf.get('class')
            try:
                middleware_config['consume_priority'] = int(conf.get(
                    'consume_priority', 0))
                middleware_config['publish_priority'] = int(conf.get(
                    'publish_priority', 0))
            except ValueError:
                raise ConfigError(
                    "Middleware priority level must be an integer")
        else:
            raise ConfigError(
                "Middleware item values must either be a string with the",
                " full dotted name of the class implementing the middleware,"
                " or a dictionary with 'class', 'consume_priority', and"
                " 'publish_priority' keys.")
        cls = load_class_by_string(cls_name)
        middleware = cls(middleware_name, middleware_config, worker)
        middlewares.append(middleware)
    return middlewares


@inlineCallbacks
def setup_middlewares_from_config(worker, config):
    """Create a list of middleware objects, call .setup_middleware() on
       then and then return the list.
       """
    middlewares = create_middlewares_from_config(worker, config)
    for mw in middlewares:
        yield mw.setup_middleware()
    returnValue(middlewares)
