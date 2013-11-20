from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import Clock

from vumi.middleware.tracing import TracingMiddleware, msg_key
from vumi.application.tests.utils import ApplicationTestCase
from vumi.application.tests.test_base import DummyApplicationWorker
from vumi.tests.helpers import MessageHelper


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
        self.msg_helper = MessageHelper()

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
    def test_trace_mo_mt_reply(self):
        mo = self.msg_helper.make_inbound('hello')
        msisdn = mo['from_addr']
        mt = self.msg_helper.make_reply(mo, 'hi there')
        ack = self.msg_helper.make_ack(mt)
        dr = self.msg_helper.make_delivery_report(mt)

        mw = yield self.mk_mw('app1')
        yield mw.handle_inbound(mo, 'default')
        self.clock.advance(10)
        yield mw.handle_outbound(mt, 'default')
        self.clock.advance(1)
        yield mw.handle_event(ack, 'default')
        self.clock.advance(1)
        mw.handle_event(dr, 'default')

        mo_trace = yield mw.get_trace(mo['message_id'])
        ttl = yield mw.redis.ttl(msg_key(mo['message_id']))
        self.assertTrue(0 < ttl <= mw.config.lifetime)

        # trace of the inbound message
        [_mo, _mt] = mo_trace
        self.assertEqual(_mo, {
            'transport_name': 'app1_transport',
            'direction': 'inbound',
            'content': 'hello',
            'connector_name': 'default',
            'time': 0.0,
            'message_type': 'user_message',
            'message_id': mo['message_id'],
        })
        self.assertEqual(_mt, {
            'transport_name': 'app1_transport',
            'direction': 'outbound',
            'content': 'hi there',
            'reply_message_id': mt['message_id'],
            'connector_name': 'default',
            'time': 10.0,
            'message_type': 'user_message',
        })

        mt_trace = yield mw.get_trace(mt['message_id'])
        [_mt, _ack, _dr] = mt_trace
        self.assertEqual(_mt, {
            'transport_name': 'app1_transport',
            'direction': 'outbound',
            'content': 'hi there',
            'connector_name': 'default',
            'time': 10.0, 'message_type':
            'user_message',
            'message_id': mt['message_id'],
        })
        self.assertEqual(_ack, {
            'event_id': ack['event_id'],
            'transport_name': 'app1_transport',
            'message_type': 'event',
            'event_type': 'ack',
            'time': 11.0,
        })
        self.assertEqual(_dr, {
            'transport_name': 'app1_transport',
            'event_type': 'delivery_report',
            'event_id': dr['event_id'],
            'time': 12.0,
            'delivery_status': 'delivered',
            'message_type': 'event',
        })
