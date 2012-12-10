from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.middleware.window_manager_middleware import WindowManagerMiddleware
from vumi.persist.fake_redis import FakeRedis
from vumi.message import TransportEvent, TransportUserMessage
from vumi.tests.utils import PersistenceMixin, VumiWorkerTestCase


class ToyWorker(object):
    
    transport_name = 'transport'
    messages = []

    def handle_outbound_message(self, msg):
        self.messages.append(msg)


class WindowManagerTestCase(VumiWorkerTestCase, PersistenceMixin):

    @inlineCallbacks
    def setUp(self):
        self._persist_setUp()
        toy_worker = ToyWorker()
        self.transport_name = toy_worker.transport_name
        config = self.mk_config({
            'window_size': 2,
            'flight_lifetime': 1,
            'monitor_loop': 0.5})
        self.mw = WindowManagerMiddleware('mw1', config, toy_worker)
        yield self.mw.setup_middleware()
        #self.mw.wm.redis = self.get_redis_manager()

    @inlineCallbacks
    def tearDown(self):
        self.mw.teardown_middleware()
        yield self._persist_tearDown()

    @inlineCallbacks
    def test_handle_outbound(self):
        msg_1 = self.mkmsg_out(message_id='1')
        yield self.mw.handle_outbound(msg_1, self.transport_name)

        msg_2 = self.mkmsg_out(message_id='2')
        yield self.mw.handle_outbound(msg_2, self.transport_name)

        msg_3 = self.mkmsg_out(message_id='3')
        yield self.mw.handle_outbound(msg_3, self.transport_name)
        
        count_waiting = yield self.mw.wm.count_waiting(self.transport_name)
        self.assertEqual(3, count_waiting)
        
        yield self.mw.wm._monitor_windows(self.mw.send_outbound, False)
        self.assertEqual(1, (yield self.mw.wm.count_waiting(self.transport_name)))
        self.assertEqual(2, (yield self.mw.wm.count_in_flight(self.transport_name)))
        self.assertEqual(2, len(self.mw.worker.messages))

        #acknowledge one of the messages
        ack = self.mkmsg_ack(user_message_id="1")
        yield self.mw.handle_event(ack, self.transport_name)
        self.assertEqual(1, (yield self.mw.wm.count_in_flight(self.transport_name)))

        yield self.mw.wm._monitor_windows(self.mw.send_outbound)
        self.assertEqual(2, (yield self.mw.wm.count_in_flight(self.transport_name)))
