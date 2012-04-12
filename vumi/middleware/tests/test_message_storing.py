"""Tests for vumi.middleware.message_storing."""

from twisted.trial.unittest import TestCase

from vumi.middleware.message_storing import StoringMiddleware
from vumi.middleware.tagger import TaggingMiddleware
from vumi.tests.utils import FakeRedis
from vumi.message import TransportUserMessage, TransportEvent


class StoringMiddlewareTestCase(TestCase):

    DEFAULT_CONFIG = {
        }

    def setUp(self):
        dummy_worker = object()
        config = {}
        self.mw = StoringMiddleware("dummy_storer", config, dummy_worker)
        self.mw.setup_middleware()
        self.mw.store.r_server = FakeRedis()
        self.store = self.mw.store

    def mk_msg(self):
        msg = TransportUserMessage(to_addr="45678", from_addr="12345",
                                   transport_name="dummy_endpoint",
                                   transport_type="dummy_transport_type")
        return msg

    def mk_ack(self):
        ack = TransportEvent(event_type="ack", user_message_id="1",
                             sent_message_id="1")
        return ack

    def test_handle_outbound(self):
        msg = self.mk_msg()
        msg_id = msg['message_id']
        response = self.mw.handle_outbound(msg, "dummy_endpoint")
        self.assertTrue(isinstance(response, TransportUserMessage))
        self.assertEqual(self.store.get_outbound_message(msg_id), msg)
        self.assertEqual(self.store.message_batches(msg_id), [])
        self.assertEqual(self.store.message_events(msg_id), [])

    def test_handle_outbound_with_tag(self):
        batch_id = self.store.batch_start([("pool", "tag")])
        msg = self.mk_msg()
        msg_id = msg['message_id']
        TaggingMiddleware.add_tag_to_msg(msg, ["pool", "tag"])
        response = self.mw.handle_outbound(msg, "dummy_endpoint")
        self.assertTrue(isinstance(response, TransportUserMessage))
        self.assertEqual(self.store.get_outbound_message(msg_id), msg)
        self.assertEqual(self.store.message_batches(msg_id), [batch_id])
        self.assertEqual(self.store.message_events(msg_id), [])
        self.assertEqual(self.store.batch_messages(batch_id), [msg_id])
        self.assertEqual(self.store.batch_replies(batch_id), [])

    def test_handle_inbound(self):
        msg = self.mk_msg()
        msg_id = msg['message_id']
        response = self.mw.handle_inbound(msg, "dummy_endpoint")
        self.assertTrue(isinstance(response, TransportUserMessage))
        self.assertEqual(self.store.get_inbound_message(msg_id), msg)

    def test_handle_inbound_with_tag(self):
        batch_id = self.store.batch_start([("pool", "tag")])
        msg = self.mk_msg()
        msg_id = msg['message_id']
        TaggingMiddleware.add_tag_to_msg(msg, ["pool", "tag"])
        response = self.mw.handle_inbound(msg, "dummy_endpoint")
        self.assertTrue(isinstance(response, TransportUserMessage))
        self.assertEqual(self.store.get_inbound_message(msg_id), msg)
        self.assertEqual(self.store.batch_messages(batch_id), [])
        self.assertEqual(self.store.batch_replies(batch_id), [msg_id])

    def test_handle_event(self):
        ack = self.mk_ack()
        event_id = ack['event_id']
        response = self.mw.handle_event(ack, "dummy_endpoint")
        self.assertTrue(isinstance(response, TransportEvent))
        self.assertEqual(self.store.get_event(event_id), ack)
        self.assertEqual(self.store.message_events("1"), [event_id])
