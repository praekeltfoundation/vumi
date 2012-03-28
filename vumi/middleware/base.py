# -*- test-case-name: vumi.middleware.tests.test_base -*-

from twisted.internet.defer import inlineCallbacks, returnValue


class BaseMiddleware(object):
    """Common middleware base class.

    This is a convenient repository for commonalities between the various
    middlewares. You should not subclass or instantiate this directly.
    """

    def __init__(self, name, config, worker):
        self.name = name
        self.config = config
        self.worker = worker
        self.endpoints = set(config.get('endpoints', []))

    def setup_middleware(self):
        pass

    def handle_inbound(self, message, endpoint):
        return message

    def handle_outbound(self, message, endpoint):
        return message

    def handle_event(self, message, endpoint):
        return message

    def handle_failure(self, message, endpoint):
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
    def _handle(self, middlewares, handler_name, message, endpoint):
        method_name = 'handle_%s' % (handler_name,)
        for middleware in middlewares:
            handler = getattr(middleware, method_name)
            message = yield handler(message, endpoint)
        returnValue(message)

    def apply_consume(self, handler_name, message, endpoint):
        return self._handle(
            self.middlewares, handler_name, message, endpoint)

    def apply_publish(self, handler_name, message, endpoint):
        return self._handle(
            reversed(self.middlewares), handler_name, message, endpoint)


@inlineCallbacks
def middlewares_from_config(config):
    # TODO: Implement this.
    yield None
    returnValue([])
