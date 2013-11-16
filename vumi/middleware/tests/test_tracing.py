from vumi.middleware.tracing import TracingMiddleware
from vumi.tests.helpers import VumiTestCase


class TracingMiddlewareTestCase(VumiTestCase):

    use_riak = False

    def setUp(self):
        self.mw = TracingMiddleware("tracing", {}, object())

    def test_something(self):
        pass
