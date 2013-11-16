# -*- test-case-name: vumi.middleware.tests.test_tracing -*-

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.middleware.base import BaseMiddleware


class TracingMiddleware(BaseMiddleware):
    """
    Middleware for tracing all the hops and associated timestamps
    a message took when being routed through the system
    """

    def setup_middleware(self):
        pass

    def teardown_middleware(self):
        pass

    def handle_inbound(self, message, connector_name):
        pass

    def handle_outbound(self, message, connector_name):
        pass

    def handle_event(self, message, connector_name):
        pass
