"""Tests for vumi.middleware.message_storing."""

from twisted.internet.defer import inlineCallbacks

from vumi.middleware.tagger import TaggingMiddleware
from vumi.message import TransportUserMessage, TransportEvent
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


class TestStoringMiddleware(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = PersistenceHelper(use_riak=True)
        self.add_cleanup(self.persistence_helper.cleanup)
        dummy_worker = object()
        config = self.persistence_helper.mk_config({})

        # Create and stash a riak manager to clean up afterwards, because we
        # don't get access to the one inside the middleware.
        self.persistence_helper.get_riak_manager()

        # We've already skipped the test by now if we don't have riakasaurus,
        # so it's safe to import stuff that pulls it in without guards.
        from vumi.middleware.message_storing import StoringMiddleware

        self.mw = StoringMiddleware("dummy_storer", config, dummy_worker)
        self.add_cleanup(self.mw.teardown_middleware)
        yield self.mw.setup_middleware()
        self.store = self.mw.store
        self.add_cleanup(self.store.manager.purge_all)
        yield self.store.manager.purge_all()
        yield self.store.cache.redis._purge_all()  # just in case

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
        self.assertEqual(outbound_keys, outbound)
        inbound_keys = yield self.store.batch_inbound_keys(batch_id)
        self.assertEqual(inbound_keys, inbound)

    @inlineCallbacks
    def assert_outbound_stored(self, msg, batch_id=None, events=[]):
        msg_id = msg['message_id']
        stored_msg = yield self.store.get_outbound_message(msg_id)
        self.assertEqual(stored_msg, msg)

        event_keys = yield self.store.message_event_keys(msg_id)
        self.assertEqual(event_keys, events)
        if batch_id is not None:
            yield self.assert_batch_keys(batch_id, outbound=[msg_id])

    @inlineCallbacks
    def assert_inbound_stored(self, msg, batch_id=None):
        msg_id = msg['message_id']
        stored_msg = yield self.store.get_inbound_message(msg_id)
        self.assertEqual(stored_msg, msg)

        if batch_id is not None:
            yield self.assert_batch_keys(batch_id, inbound=[msg_id])

    @inlineCallbacks
    def test_handle_outbound(self):
        msg = self.mk_msg()
        response = yield self.mw.handle_outbound(msg, "dummy_connector")
        self.assertTrue(isinstance(response, TransportUserMessage))
        yield self.assert_outbound_stored(msg)

    @inlineCallbacks
    def test_handle_outbound_with_tag(self):
        batch_id = yield self.store.batch_start([("pool", "tag")])
        msg = self.mk_msg()
        TaggingMiddleware.add_tag_to_msg(msg, ["pool", "tag"])
        response = yield self.mw.handle_outbound(msg, "dummy_connector")
        self.assertTrue(isinstance(response, TransportUserMessage))
        yield self.assert_outbound_stored(msg, batch_id)

    @inlineCallbacks
    def test_handle_inbound(self):
        msg = self.mk_msg()
        response = yield self.mw.handle_inbound(msg, "dummy_connector")
        self.assertTrue(isinstance(response, TransportUserMessage))
        yield self.assert_inbound_stored(msg)

    @inlineCallbacks
    def test_handle_inbound_with_tag(self):
        batch_id = yield self.store.batch_start([("pool", "tag")])
        msg = self.mk_msg()
        TaggingMiddleware.add_tag_to_msg(msg, ["pool", "tag"])
        response = yield self.mw.handle_inbound(msg, "dummy_connector")
        self.assertTrue(isinstance(response, TransportUserMessage))
        yield self.assert_inbound_stored(msg, batch_id)

    @inlineCallbacks
    def test_handle_event(self):
        msg = self.mk_msg()
        msg_id = msg["message_id"]
        yield self.store.add_outbound_message(msg)

        ack = self.mk_ack(user_message_id=msg_id)
        event_id = ack['event_id']
        response = yield self.mw.handle_event(ack, "dummy_connector")
        self.assertTrue(isinstance(response, TransportEvent))
        yield self.assert_outbound_stored(msg, events=[event_id])
