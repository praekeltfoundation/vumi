# -*- test-case-name: vumi.middleware.tests.test_base -*-

from twisted.internet.defer import inlineCallbacks, returnValue


class BaseMiddleware(object):
    """Common middleware base class.

    This is a convenient repository for commonalities between the various
    middlewares. You should not subclass or instantiate this directly.
    """

    def __init__(self, worker, config):
        self.worker = worker
        self.config = config

    def setup_middleware(self):
        pass

    def handle_inbound(self, message, endpoint_name):
        return message

    def handle_outbound(self, message, endpoint_name):
        return message

    def handle_event(self, message, endpoint_name):
        return message

    def handle_failure(self, message, endpoint_name):
        return message


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

    @inlineCallbacks
    def _handle(self, middlewares, handler_name, message, endpoint_name):
        method_name = 'handle_%s' % (handler_name,)
        for middleware in middlewares:
            handler = getattr(middleware, method_name)
            message = yield handler(message, endpoint_name)
        returnValue(message)

    def apply_consume(self, handler_name, message, endpoint_name):
        return self._handle(
            self.middlewares, handler_name, message, endpoint_name)

    def apply_publish(self, handler_name, message, endpoint_name):
        return self._handle(
            reversed(self.middlewares), handler_name, message, endpoint_name)
