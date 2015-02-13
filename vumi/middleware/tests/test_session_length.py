"""Tests for vumi.middleware.session_length."""

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import TransportUserMessage
from vumi.middleware.session_length import SessionLengthMiddleware
from vumi.tests.helpers import VumiTestCase, PersistenceHelper

SESSION_NEW, SESSION_CLOSE = (
    TransportUserMessage.SESSION_NEW, TransportUserMessage.SESSION_CLOSE)


class TestStaticProviderSettingMiddleware(VumiTestCase):
    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())

    @inlineCallbacks
    def mk_middleware(self, config={}):
        dummy_worker = object()
        config = self.persistence_helper.mk_config(config)
        mw = SessionLengthMiddleware(
            "session_length", config, dummy_worker)
        yield mw.setup_middleware()
        self.redis = mw.redis
        self.add_cleanup(mw.teardown_middleware)
        returnValue(mw)

    def mk_msg(self, to_addr, from_addr, session_event=SESSION_NEW):
        msg = TransportUserMessage(
            to_addr=to_addr, from_addr=from_addr,
            transport_name="dummy_connector",
            transport_type="dummy_transport_type",
            session_event=session_event)
        return msg

    @inlineCallbacks
    def test_incoming_message_session_start(self):
        mw = yield self.mk_middleware()
        msg_start = self.mk_msg('+12345', '+54321')

        msg = yield mw.handle_inbound(msg_start, "dummy_connector")
        value = yield self.redis.get('dummy_connector:+54321:session_created')
        self.assertTrue(value is not None)
        self.assertTrue(isinstance(
                msg['helper_metadata']['session']['session_start'], float))

    @inlineCallbacks
    def test_incoming_message_session_end(self):
        mw = yield self.mk_middleware()
        msg_end = self.mk_msg('+12345', '+54321', session_event=SESSION_CLOSE)

        msg = yield mw.handle_inbound(msg_end, "dummy_connector")
        self.assertTrue(isinstance(
                msg['helper_metadata']['session']['session_end'], float))

    @inlineCallbacks
    def test_incoming_message_session_start_end(self):
        mw = yield self.mk_middleware()
        msg_start = self.mk_msg('+12345', '+54321')
        msg_end = self.mk_msg('+12345', '+54321', session_event=SESSION_CLOSE)

        msg = yield mw.handle_inbound(msg_start, "dummy_connector")
        value = yield self.redis.get('dummy_connector:+54321:session_created')
        self.assertTrue(value is not None)
        self.assertTrue(isinstance(
                msg['helper_metadata']['session']['session_start'], float))

        msg = yield mw.handle_inbound(msg_end, "dummy_connector")
        value = yield self.redis.get('dummy_connector:+54321:session_created')
        self.assertTrue(value is None)
        self.assertTrue(isinstance(
                msg['helper_metadata']['session']['session_start'], float))
        self.assertTrue(isinstance(
            msg['helper_metadata']['session']['session_end'], float))

    @inlineCallbacks
    def test_outgoing_message_session_start(self):
        mw = yield self.mk_middleware()
        msg_start = self.mk_msg('+12345', '+54321')

        msg = yield mw.handle_outbound(msg_start, "dummy_connector")
        value = yield self.redis.get('dummy_connector:+12345:session_created')
        self.assertTrue(value is not None)
        self.assertTrue(isinstance(
                msg['helper_metadata']['session']['session_start'], float))

    @inlineCallbacks
    def test_outgoing_message_session_end(self):
        mw = yield self.mk_middleware()
        msg_end = self.mk_msg('+12345', '+54321', session_event=SESSION_CLOSE)

        msg = yield mw.handle_outbound(msg_end, "dummy_connector")
        self.assertTrue(isinstance(
                msg['helper_metadata']['session']['session_end'], float))

    @inlineCallbacks
    def test_outgoing_message_session_start_end(self):
        mw = yield self.mk_middleware()
        msg_start = self.mk_msg('+12345', '+54321')
        msg_end = self.mk_msg('+12345', '+54321', session_event=SESSION_CLOSE)

        msg = yield mw.handle_outbound(msg_start, "dummy_connector")
        value = yield self.redis.get('dummy_connector:+12345:session_created')
        self.assertTrue(value is not None)
        self.assertTrue(isinstance(
                msg['helper_metadata']['session']['session_start'], float))

        msg = yield mw.handle_outbound(msg_end, "dummy_connector")
        value = yield self.redis.get('dummy_connector:+54321:session_created')
        self.assertTrue(value is None)
        self.assertTrue(isinstance(
                msg['helper_metadata']['session']['session_start'], float))
        self.assertTrue(isinstance(
            msg['helper_metadata']['session']['session_end'], float))

    @inlineCallbacks
    def test_redis_key_timeout(self):
        mw = yield self.mk_middleware()
        msg_start = self.mk_msg('+12345', '+54321')

        yield mw.handle_inbound(msg_start, "dummy_connector")
        ttl = yield self.redis.ttl('dummy_connector::+54321:session_created')
        self.assertTrue(ttl <= 120)

    @inlineCallbacks
    def test_redis_key_custom_timeout(self):
        mw = yield self.mk_middleware({'timeout': 20})
        msg_start = self.mk_msg('+12345', '+54321')

        yield mw.handle_inbound(msg_start, "dummy_connector")
        ttl = yield self.redis.ttl('dummy_connector:+54321:session_created')
        self.assertTrue(ttl <= 20)

    @inlineCallbacks
    def test_custom_message_field_name(self):
        mw = yield self.mk_middleware({'field_name': 'foobar'})
        msg_start = self.mk_msg('+12345', '+54321')

        msg = yield mw.handle_inbound(msg_start, "dummy_connector")
        self.assertTrue(
            msg['helper_metadata']['foobar']['session_start'] is not None)
