# -*- test-case-name: vumi.middleware.tests.test_base -*-

from twisted.internet.defer import inlineCallbacks, returnValue, maybeDeferred

from vumi.utils import load_class_by_string
from vumi.errors import ConfigError, VumiError


class MiddlewareError(VumiError):
    pass


class MiddlewareControlFlag():
    pass


class StopPropagation(MiddlewareControlFlag):
    pass


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
    :meth:`setup_middleware` instead.
    """

    def __init__(self, name, config, worker):
        self.name = name
        self.config = config
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

    def handle_inbound(self, message, endpoint):
        """Called when an inbound transport user message is published
        or consumed.

        The other methods -- :meth:`handle_outbound`,
        :meth:`handle_event`, :meth:`handle_failure` -- all function
        in the same way. Only the kind of message being processed
        differs.

        :param vumi.message.TransportUserMessage message:
            Inbound message to process.
        :param string endpoint:
            The `transport_name` of the endpoint the message is being
            received on or send to.
        :rtype: vumi.message.TransportUserMessage
        :returns: The processed message.
        """
        return message

    def handle_outbound(self, message, endpoint):
        """Called to process an outbound transport user message.
        See :meth:`handle_inbound`.
        """
        return message

    def handle_event(self, event, endpoint):
        """Called to process an event message (
        :class:`vumi.message.TransportEvent`).
        See :meth:`handle_inbound`.
        """
        return event

    def handle_failure(self, failure, endpoint):
        """Called to process a failure message (
        :class:`vumi.transports.failures.FailureMessage`).
        See :meth:`handle_inbound`.
        """
        return failure

    def resume_handling(self, handle_name, message, endpoint):
        return self.worker._middlewares.resume_handling(
            self, handle_name, message, endpoint)


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
        self.middlewares = middlewares

    def _get_middleware_index(self, middleware):
        return self.middlewares.index(middleware)

    @inlineCallbacks
    def resume_handling(self, mw, handle_name, message, endpoint):
        mw_index = self._get_middleware_index(mw)
        #In case there are no other middleware after this one
        if mw_index + 1 == len(self.middlewares):
            returnValue(message)
        message = yield self._handle(self.middlewares, handle_name, message, endpoint, mw_index + 1)
        returnValue(message)

    @inlineCallbacks
    def _handle(self, middlewares, handler_name, message, endpoint, from_index=0):
        method_name = 'handle_%s' % (handler_name,)
        for index, middleware in enumerate(middlewares[from_index:]):            
            handler = getattr(middleware, method_name)
            message = yield self._handle_middleware(handler, message, endpoint, index)
            if message is None:
                raise MiddlewareError('Returned value of %s.%s should never ' \
                                'be None' % (middleware, method_name,))
        returnValue(message)

    def _handle_middleware(self, handler, message, endpoint, index):        
        def _handle_control_flag(f):
            if not isinstance(f.value, MiddlewareControlFlag):
                raise f
            if isinstance(f.value, StopPropagation):
                raise f
            raise MiddlewareError('Unknown Middleware Control Flag: %s'
                                  % (f.value,))                
        
        d = maybeDeferred(handler, message, endpoint)
        d.addErrback(_handle_control_flag)
        return d
     
    def process_control_flag(self, f):
        f.trap(StopPropagation)
        if isinstance(f.value, StopPropagation):
            return None

    def apply_consume(self, handler_name, message, endpoint):
        return self._handle(
            self.middlewares, handler_name, message, endpoint)

    def apply_publish(self, handler_name, message, endpoint):
        return self._handle(
            reversed(self.middlewares), handler_name, message, endpoint)

    @inlineCallbacks
    def teardown(self):
        for mw in reversed(self.middlewares):
            yield mw.teardown_middleware()


def create_middlewares_from_config(worker, config):
    """Return a list of middleware objects created from a worker
       configuration.
       """
    middlewares = []
    for item in config.get("middleware", []):
        keys = item.keys()
        if len(keys) != 1:
            raise ConfigError("Middleware items contain only a single"
                              " key-value pair. The key should be a name"
                              " for the middleware. The value should be"
                              " the full dotted name of the class"
                              " implementing the middleware.")
        middleware_name = keys[0]
        cls_name = item[middleware_name]
        middleware_config = config.get(middleware_name, {})
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
