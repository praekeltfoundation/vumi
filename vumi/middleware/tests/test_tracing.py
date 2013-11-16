from twisted.internet.defer import inlineCallbacks

from vumi.middleware.tracing import TracingMiddleware
from vumi.application.tests.utils import ApplicationTestCase
from vumi.application.tests.test_base import DummyApplicationWorker


class TracingMiddlewareTestCase(ApplicationTestCase):

    use_riak = False
    application_class = DummyApplicationWorker

    @inlineCallbacks
    def setUp(self):
        yield super(TracingMiddlewareTestCase, self).setUp()
        dummy_worker = yield self.get_application({})
        self.mw = TracingMiddleware(
            "tracing", self.mk_config({}), dummy_worker)
        yield self.mw.setup_middleware()

    def test_something(self):
        pass
