from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial.unittest import TestCase

from vumi.middleware.window_manager_middleware import WindowManagerMiddleware
from vumi.persist.fake_redis import FakeRedis
from vumi.message import TransportEvent, TransportUserMessage
from vumi.tests.utils import PersistenceMixin

class ToyWorker(object):
    
    transport_name = 'transport'
    messages = []

    def handle_outbound_message(self, msg):
        self.messages.append(msg)


class WindowManagerTestCase(TestCase, PersistenceMixin):

    def mkmsg_ack(self, user_message_id='1', sent_message_id='abc',
                    transport_metadata=None, transport_name=None):
        if transport_metadata is None:
            transport_metadata = {}
        if transport_name is None:
            transport_name = self.transport_name
        return TransportEvent(
            event_type='ack',
            user_message_id=user_message_id,
            sent_message_id=sent_message_id,
            transport_name=transport_name,
            transport_metadata=transport_metadata,
            )

    def mkmsg_out(self, content='hello world', message_id='1',
                  to_addr='+41791234567', from_addr='9292', group=None,
                  session_event=None, in_reply_to=None,
                  transport_type='sms', transport_metadata={},
                  transport_name=None, helper_metadata={},
                  ):
        if transport_name is None:
            transport_name = self.transport_name
        if helper_metadata is None:
            helper_metadata = {}
        params = dict(
            to_addr=to_addr,
            from_addr=from_addr,
            group=group,
            message_id=message_id,
            transport_name=transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            session_event=session_event,
            in_reply_to=in_reply_to,
            helper_metadata=helper_metadata,
            )
        return TransportUserMessage(**params)

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

        #acknoledge one
        ack = self.mkmsg_ack(user_message_id="1")
        yield self.mw.handle_event(ack, self.transport_name)
        self.assertEqual(1, (yield self.mw.wm.count_in_flight(self.transport_name)))

        yield self.mw.wm._monitor_windows(self.mw.send_outbound)
        self.assertEqual(2, (yield self.mw.wm.count_in_flight(self.transport_name)))
