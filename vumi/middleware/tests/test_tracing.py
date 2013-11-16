from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import Clock

from vumi.middleware.tracing import TracingMiddleware
from vumi.application.tests.utils import ApplicationTestCase
from vumi.application.tests.test_base import DummyApplicationWorker


class TracingMiddlewareTestCase(ApplicationTestCase):

    use_riak = False
    middleware_class = TracingMiddleware
    application_class = DummyApplicationWorker
    clock = Clock()

    @inlineCallbacks
    def setUp(self):
        yield super(TracingMiddlewareTestCase, self).setUp()
        self.patch(
            TracingMiddleware, 'get_clock', lambda *a: self.clock)

    @inlineCallbacks
    def mk_mw(self, name):
        app = yield self.get_application(self.mk_config({
            'transport_name': '%s_transport' % (name,)
        }))
        mw = self.middleware_class(
            '%s_middleware' % (name,), self.mk_config({}), app)
        yield mw.setup_middleware()
        returnValue(mw)

    @inlineCallbacks
    def test_something(self):
        mw1 = yield self.mk_mw('app1')
