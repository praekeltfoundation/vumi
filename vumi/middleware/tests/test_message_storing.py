"""Tests for vumi.middleware.message_storing."""

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.middleware.tagger import TaggingMiddleware
from vumi.message import TransportUserMessage, TransportEvent
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


class TestStoringMiddleware(VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))

        # Create and stash a riak manager to clean up afterwards, because we
        # don't get access to the one inside the middleware.
        self.persistence_helper.get_riak_manager()

    @inlineCallbacks
    def setup_middleware(self, config={}):
        # We've already skipped the test by now if we don't have riakasaurus,
        # so it's safe to import stuff that pulls it in without guards.
        from vumi.middleware.message_storing import StoringMiddleware

        config = self.persistence_helper.mk_config(config)
        dummy_worker = object()
        mw = StoringMiddleware("dummy_storer", config, dummy_worker)
        self.add_cleanup(mw.teardown_middleware)
        yield mw.setup_middleware()
        self.store = mw.store
        self.add_cleanup(self.store.manager.purge_all)
        yield self.store.manager.purge_all()
        yield self.store.cache.redis._purge_all()  # just in case
        returnValue(mw)

    def mk_msg(self):
        msg = TransportUserMessage(to_addr="45678", from_addr="12345",
                                   transport_name="dummy_connector",
                                   transport_type="dummy_transport_type")
        return msg

    def mk_ack(self, user_message_id="1"):
        ack = TransportEvent(event_type="ack", user_message_id=user_message_id,
                             sent_message_id="1")
        return ack

    @inlineCallbacks
    def assert_batch_keys(self, batch_id, outbound=[], inbound=[]):
        outbound_keys = yield self.store.batch_outbound_keys(batch_id)
        self.assertEqual(sorted(outbound_keys), sorted(outbound))
        inbound_keys = yield self.store.batch_inbound_keys(batch_id)
        self.assertEqual(sorted(inbound_keys), sorted(inbound))

    @inlineCallbacks
    def assert_outbound_stored(self, msg, batch_id=None, events=[]):
        msg_id = msg['message_id']
        stored_msg = yield self.store.get_outbound_message(msg_id)
        self.assertEqual(stored_msg, msg)

        event_keys = yield self.store.message_event_keys(msg_id)
        self.assertEqual(sorted(event_keys), sorted(events))
        if batch_id is not None:
            yield self.assert_batch_keys(batch_id, outbound=[msg_id])

    @inlineCallbacks
    def assert_outbound_not_stored(self, msg):
        msg_id = msg['message_id']
        stored_msg = yield self.store.get_outbound_message(msg_id)
        self.assertEqual(stored_msg, None)

    @inlineCallbacks
    def assert_inbound_stored(self, msg, batch_id=None):
        msg_id = msg['message_id']
        stored_msg = yield self.store.get_inbound_message(msg_id)
        self.assertEqual(stored_msg, msg)

        if batch_id is not None:
            yield self.assert_batch_keys(batch_id, inbound=[msg_id])

    @inlineCallbacks
    def assert_inbound_not_stored(self, msg):
        msg_id = msg['message_id']
        stored_msg = yield self.store.get_inbound_message(msg_id)
        self.assertEqual(stored_msg, None)

    @inlineCallbacks
    def test_handle_outbound(self):
        mw = yield self.setup_middleware()
        msg1 = self.mk_msg()
        resp1 = yield mw.handle_consume_outbound(msg1, "dummy_connector")
        self.assertEqual(msg1, resp1)
        yield self.assert_outbound_stored(msg1)

        msg2 = self.mk_msg()
        resp2 = yield mw.handle_publish_outbound(msg2, "dummy_connector")
        self.assertEqual(msg2, resp2)
        yield self.assert_outbound_stored(msg2)

        self.assertNotEqual(msg1['message_id'], msg2['message_id'])

    @inlineCallbacks
    def test_handle_outbound_no_consume_store(self):
        mw = yield self.setup_middleware({'store_on_consume': False})
        msg1 = self.mk_msg()
        resp1 = yield mw.handle_consume_outbound(msg1, "dummy_connector")
        self.assertEqual(msg1, resp1)
        yield self.assert_outbound_not_stored(msg1)

        msg2 = self.mk_msg()
        resp2 = yield mw.handle_publish_outbound(msg2, "dummy_connector")
        self.assertEqual(msg2, resp2)
        yield self.assert_outbound_stored(msg2)

        self.assertNotEqual(msg1['message_id'], msg2['message_id'])

    @inlineCallbacks
    def test_handle_outbound_with_tag(self):
        mw = yield self.setup_middleware()
        batch_id = yield self.store.batch_start([("pool", "tag")])
        msg = self.mk_msg()
        TaggingMiddleware.add_tag_to_msg(msg, ["pool", "tag"])
        response = yield mw.handle_outbound(msg, "dummy_connector")
        self.assertEqual(response, msg)
        yield self.assert_outbound_stored(msg, batch_id)

    @inlineCallbacks
    def test_handle_inbound(self):
        mw = yield self.setup_middleware()
        msg1 = self.mk_msg()
        resp1 = yield mw.handle_consume_inbound(msg1, "dummy_connector")
        self.assertEqual(resp1, msg1)
        yield self.assert_inbound_stored(msg1)

        msg2 = self.mk_msg()
        resp2 = yield mw.handle_publish_inbound(msg2, "dummy_connector")
        self.assertEqual(resp2, msg2)
        yield self.assert_inbound_stored(msg2)

        self.assertNotEqual(msg1['message_id'], msg2['message_id'])

    @inlineCallbacks
    def test_handle_inbound_no_consume_store(self):
        mw = yield self.setup_middleware({'store_on_consume': False})
        msg1 = self.mk_msg()
        resp1 = yield mw.handle_consume_inbound(msg1, "dummy_connector")
        self.assertEqual(resp1, msg1)
        yield self.assert_inbound_not_stored(msg1)

        msg2 = self.mk_msg()
        resp2 = yield mw.handle_publish_inbound(msg2, "dummy_connector")
        self.assertEqual(resp2, msg2)
        yield self.assert_inbound_stored(msg2)

        self.assertNotEqual(msg1['message_id'], msg2['message_id'])

    @inlineCallbacks
    def test_handle_inbound_with_tag(self):
        mw = yield self.setup_middleware()
        batch_id = yield self.store.batch_start([("pool", "tag")])
        msg = self.mk_msg()
        TaggingMiddleware.add_tag_to_msg(msg, ["pool", "tag"])
        response = yield mw.handle_inbound(msg, "dummy_connector")
        self.assertEqual(response, msg)
        yield self.assert_inbound_stored(msg, batch_id)

    @inlineCallbacks
    def test_handle_event(self):
        mw = yield self.setup_middleware()
        msg = self.mk_msg()
        msg_id = msg["message_id"]
        yield self.store.add_outbound_message(msg)

        ack1 = self.mk_ack(user_message_id=msg_id)
        event_id1 = ack1['event_id']
        resp1 = yield mw.handle_consume_event(ack1, "dummy_connector")
        self.assertEqual(ack1, resp1)
        yield self.assert_outbound_stored(msg, events=[event_id1])

        ack2 = self.mk_ack(user_message_id=msg_id)
        event_id2 = ack2['event_id']
        resp2 = yield mw.handle_publish_event(ack2, "dummy_connector")
        self.assertEqual(ack2, resp2)
        yield self.assert_outbound_stored(msg, events=[event_id1, event_id2])

    @inlineCallbacks
    def test_handle_event_no_consume_store(self):
        mw = yield self.setup_middleware({'store_on_consume': False})
        msg = self.mk_msg()
        msg_id = msg["message_id"]
        yield self.store.add_outbound_message(msg)

        ack1 = self.mk_ack(user_message_id=msg_id)
        resp1 = yield mw.handle_consume_event(ack1, "dummy_connector")
        self.assertEqual(resp1, ack1)
        yield self.assert_outbound_stored(msg, events=[])

        ack2 = self.mk_ack(user_message_id=msg_id)
        event_id2 = ack2['event_id']
        resp2 = yield mw.handle_publish_event(ack2, "dummy_connector")
        self.assertEqual(resp2, ack2)
        yield self.assert_outbound_stored(msg, events=[event_id2])
