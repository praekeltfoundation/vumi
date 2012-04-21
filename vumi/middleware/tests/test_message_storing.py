"""Tests for vumi.middleware.message_storing."""

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.middleware.message_storing import StoringMiddleware
from vumi.middleware.tagger import TaggingMiddleware
from vumi.tests.utils import FakeRedis
from vumi.message import TransportUserMessage, TransportEvent


class StoringMiddlewareTestCase(TestCase):

    DEFAULT_CONFIG = {
        }

    @inlineCallbacks
    def setUp(self):
        dummy_worker = object()
        config = {
            "riak": {
                "bucket_prefix": "test.",
                },
            }
        self.mw = StoringMiddleware("dummy_storer", config, dummy_worker)
        self.mw.setup_middleware()
        self.mw.store.r_server = FakeRedis()
        self.store = self.mw.store
        yield self.store.manager.purge_all()

    @inlineCallbacks
    def tearDown(self):
        yield self.store.manager.purge_all()

    def mk_msg(self):
        msg = TransportUserMessage(to_addr="45678", from_addr="12345",
                                   transport_name="dummy_endpoint",
                                   transport_type="dummy_transport_type")
        return msg

    def mk_ack(self, user_message_id="1"):
        ack = TransportEvent(event_type="ack", user_message_id=user_message_id,
                             sent_message_id="1")
        return ack

    @inlineCallbacks
    def test_handle_outbound(self):
        msg = self.mk_msg()
        msg_id = msg['message_id']
        response = yield self.mw.handle_outbound(msg, "dummy_endpoint")
        self.assertTrue(isinstance(response, TransportUserMessage))

        stored_msg = yield self.store.get_outbound_message(msg_id)
        message_events = yield self.store.message_events(msg_id)

        self.assertEqual(stored_msg, msg)
        self.assertEqual(message_events, [])

    @inlineCallbacks
    def test_handle_outbound_with_tag(self):
        batch_id = yield self.store.batch_start([("pool", "tag")])
        msg = self.mk_msg()
        msg_id = msg['message_id']
        TaggingMiddleware.add_tag_to_msg(msg, ["pool", "tag"])
        response = yield self.mw.handle_outbound(msg, "dummy_endpoint")
        self.assertTrue(isinstance(response, TransportUserMessage))

        stored_msg = yield self.store.get_outbound_message(msg_id)
        message_events = yield self.store.message_events(msg_id)
        batch_messages = yield self.store.batch_messages(batch_id)
        batch_replies = yield self.store.batch_replies(batch_id)

        self.assertEqual(stored_msg, msg)
        self.assertEqual(message_events, [])
        self.assertEqual(batch_messages, [msg])
        self.assertEqual(batch_replies, [])

    @inlineCallbacks
    def test_handle_inbound(self):
        msg = self.mk_msg()
        msg_id = msg['message_id']
        response = yield self.mw.handle_inbound(msg, "dummy_endpoint")
        self.assertTrue(isinstance(response, TransportUserMessage))

        stored_msg = yield self.store.get_inbound_message(msg_id)

        self.assertEqual(stored_msg, msg)

    @inlineCallbacks
    def test_handle_inbound_with_tag(self):
        batch_id = yield self.store.batch_start([("pool", "tag")])
        msg = self.mk_msg()
        msg_id = msg['message_id']
        TaggingMiddleware.add_tag_to_msg(msg, ["pool", "tag"])
        response = yield self.mw.handle_inbound(msg, "dummy_endpoint")
        self.assertTrue(isinstance(response, TransportUserMessage))

        stored_msg = yield self.store.get_inbound_message(msg_id)
        batch_messages = yield self.store.batch_messages(batch_id)
        batch_replies = yield self.store.batch_replies(batch_id)

        self.assertEqual(stored_msg, msg)
        self.assertEqual(batch_messages, [])
        self.assertEqual(batch_replies, [msg])

    @inlineCallbacks
    def test_handle_event(self):
        msg = self.mk_msg()
        msg_id = msg["message_id"]
        yield self.store.add_outbound_message(msg)

        ack = self.mk_ack(user_message_id=msg_id)
        event_id = ack['event_id']
        response = yield self.mw.handle_event(ack, "dummy_endpoint")
        self.assertTrue(isinstance(response, TransportEvent))

        stored_event = yield self.store.get_event(event_id)
        message_events = yield self.store.message_events(msg_id)

        self.assertEqual(stored_event, ack)
        self.assertEqual(message_events, [ack])
