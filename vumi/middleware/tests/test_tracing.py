from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import Clock

from vumi.middleware.tracing import (TracingMiddleware, msg_key,
                                     TraceManager)
from vumi.application.tests.utils import ApplicationTestCase
from vumi.application.tests.test_base import DummyApplicationWorker
from vumi.tests.helpers import MessageHelper


class TracingMiddlewareTestCase(ApplicationTestCase):

    use_riak = False
    middleware_class = TracingMiddleware
    application_class = DummyApplicationWorker
    clock = None

    @inlineCallbacks
    def setUp(self):
        yield super(TracingMiddlewareTestCase, self).setUp()
        self.clock = Clock()
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

    def get_trace(self, mw, message_id):
        return TraceManager(mw.redis).get_trace(message_id)

    @inlineCallbacks
    def test_trace_inbound(self):
        mo = self.msg_helper.make_inbound('hello')
        mw = yield self.mk_mw('app1')
        yield mw.handle_inbound(mo, 'default')
        [hop] = yield self.get_trace(mw, mo['message_id'])
        self.assertEqual(hop, {
            'transport_name': 'app1_transport',
            'direction': 'inbound',
            'content': 'hello',
            'connector_name': 'default',
            'time': 0.0,
            'message_type': 'user_message',
            'message_id': mo['message_id'],
        })

    @inlineCallbacks
    def test_trace_outbound(self):
        mt = self.msg_helper.make_outbound('hi there')
        mw = yield self.mk_mw('app1')
        yield mw.handle_outbound(mt, 'default')
        [hop] = yield self.get_trace(mw, mt['message_id'])
        self.assertEqual(hop, {
            'transport_name': 'app1_transport',
            'direction': 'outbound',
            'content': 'hi there',
            'message_id': mt['message_id'],
            'connector_name': 'default',
            'time': 0.0,
            'message_type': 'user_message',
        })

    @inlineCallbacks
    def test_tracing_reply_as_part_of_mo_trace(self):
        mo = self.msg_helper.make_inbound('hello')
        mt = self.msg_helper.make_reply(mo, 'hi there')
        mw = yield self.mk_mw('app1')
        yield mw.handle_outbound(mt, 'default')
        [hop] = yield self.get_trace(mw, mo['message_id'])
        self.assertEqual(hop, {
            'transport_name': 'app1_transport',
            'direction': 'outbound',
            'content': 'hi there',
            'connector_name': 'default',
            'reply_message_id': mt['message_id'],
            'time': 0.0,
            'message_type': 'user_message',
        })

    @inlineCallbacks
    def test_trace_ack(self):
        mt = self.msg_helper.make_outbound('hi there')
        ack = self.msg_helper.make_ack(mt)
        mw = yield self.mk_mw('app1')
        yield mw.handle_event(ack, 'default')
        [hop] = yield self.get_trace(mw, mt['message_id'])
        self.assertEqual(hop, {
            'event_id': ack['event_id'],
            'transport_name': 'app1_transport',
            'message_type': 'event',
            'event_type': 'ack',
            'time': 0.0,
        })

    @inlineCallbacks
    def test_trace_dr(self):
        mt = self.msg_helper.make_outbound('hi there')
        dr = self.msg_helper.make_delivery_report(mt)
        mw = yield self.mk_mw('app1')
        yield mw.handle_event(dr, 'default')
        [hop] = yield self.get_trace(mw, mt['message_id'])
        self.assertEqual(hop, {
            'event_id': dr['event_id'],
            'transport_name': 'app1_transport',
            'message_type': 'event',
            'event_type': 'delivery_report',
            'delivery_status': 'delivered',
            'time': 0.0,
        })

    @inlineCallbacks
    def test_trace_timed_order(self):
        mt = self.msg_helper.make_outbound('hi there')
        ack = self.msg_helper.make_ack(mt)
        dr = self.msg_helper.make_delivery_report(mt)
        mw = yield self.mk_mw('app1')
        yield mw.handle_outbound(mt, 'default')
        self.clock.advance(1)
        yield mw.handle_event(ack, 'default')
        self.clock.advance(1)
        yield mw.handle_event(dr, 'default')

        [mt_hop, ack_hop, dr_hop] = yield self.get_trace(mw, mt['message_id'])
        self.assertEqual(mt_hop, {
            'transport_name': 'app1_transport',
            'direction': 'outbound',
            'content': 'hi there',
            'connector_name': 'default',
            'time': 0.0,
            'message_type':'user_message',
            'message_id': mt['message_id'],
        })
        self.assertEqual(ack_hop, {
            'event_id': ack['event_id'],
            'transport_name': 'app1_transport',
            'message_type': 'event',
            'event_type': 'ack',
            'time': 1.0,
        })
        self.assertEqual(dr_hop, {
            'transport_name': 'app1_transport',
            'event_type': 'delivery_report',
            'event_id': dr['event_id'],
            'time': 2.0,
            'delivery_status': 'delivered',
            'message_type': 'event',
        })
