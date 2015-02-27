"""Tests for vumi.middleware.session_length."""

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import Clock

from vumi.message import TransportUserMessage
from vumi.middleware.session_length import SessionLengthMiddleware
from vumi.tests.helpers import VumiTestCase, PersistenceHelper

SESSION_NEW, SESSION_CLOSE, SESSION_NONE = (
    TransportUserMessage.SESSION_NEW,
    TransportUserMessage.SESSION_CLOSE,
    TransportUserMessage.SESSION_NONE)

FALLBACK_KEY = SessionLengthMiddleware.FALLBACK_KEY


class TestSessionLengthMiddleware(VumiTestCase):
    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.clock = Clock()

    @inlineCallbacks
    def mk_middleware(self, **kw):
        dummy_worker = object()
        config = self.persistence_helper.mk_config(kw)
        mw = SessionLengthMiddleware("session_length", config, dummy_worker)
        yield mw.setup_middleware()
        self.patch(mw, 'clock', self.clock)
        self.redis = mw.redis
        self.add_cleanup(mw.teardown_middleware)
        returnValue(mw)

    def mk_msg(self, to_addr, from_addr, session_event=SESSION_NEW,
               session_start=None, session_end=None,
               transport_name="dummy_transport"):
        msg = TransportUserMessage(
            to_addr=to_addr, from_addr=from_addr,
            transport_name=transport_name,
            transport_type="dummy_transport_type",
            session_event=session_event)

        if session_start is not None:
            self._set_metadata(msg, 'session_start', session_start)

        if session_end is not None:
            self._set_metadata(msg, 'session_end', session_end)

        return msg

    def _set_metadata(self, msg, name, value, metadata_field_name='session'):
        metadata = msg['helper_metadata'].setdefault(metadata_field_name, {})
        metadata[name] = value

    @inlineCallbacks
    def test_incoming_message_session_start(self):
        mw = yield self.mk_middleware()
        msg_start = self.mk_msg('+12345', '+54321')

        msg = yield mw.handle_inbound(msg_start, "dummy_connector")
        value = yield self.redis.get('dummy_transport:+54321:session_created')
        self.assertEqual(value, '0.0')
        self.assertEqual(
                msg['helper_metadata']['session']['session_start'], 0.0)

    @inlineCallbacks
    def test_incoming_message_session_end(self):
        mw = yield self.mk_middleware()
        msg_end = self.mk_msg('+12345', '+54321', session_event=SESSION_CLOSE)

        msg = yield mw.handle_inbound(msg_end, "dummy_connector")
        self.assertEqual(
                msg['helper_metadata']['session']['session_end'], 0.0)

    @inlineCallbacks
    def test_incoming_message_session_start_end(self):
        mw = yield self.mk_middleware()
        msg_start = self.mk_msg('+12345', '+54321')
        msg_end = self.mk_msg('+12345', '+54321', session_event=SESSION_CLOSE)

        msg = yield mw.handle_inbound(msg_start, "dummy_connector")
        value = yield self.redis.get('dummy_transport:+54321:session_created')
        self.assertEqual(value, '0.0')

        self.assertEqual(
                msg['helper_metadata']['session']['session_start'], 0.0)

        self.clock.advance(1.0)
        msg = yield mw.handle_inbound(msg_end, "dummy_connector")
        value = yield self.redis.get('dummy_transport:+54321:session_created')
        self.assertTrue(value is None)
        self.assertEqual(
                msg['helper_metadata']['session']['session_start'], 0.0)
        self.assertEqual(
            msg['helper_metadata']['session']['session_end'], 1.0)

    @inlineCallbacks
    def test_incoming_message_session_start_no_overwrite(self):
        mw = yield self.mk_middleware()

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_start=23,
            session_event=SESSION_NEW)

        yield self.redis.set('dummy_transport:+12345:session_created', '23')

        msg = yield mw.handle_inbound(msg, "dummy_connector")
        value = yield self.redis.get('dummy_transport:+12345:session_created')
        self.assertEqual(value, '23')

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23)

    @inlineCallbacks
    def test_incoming_message_session_end_no_overwrite(self):
        mw = yield self.mk_middleware()

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_start=23,
            session_end=32,
            session_event=SESSION_CLOSE)

        msg = yield mw.handle_inbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23)

        self.assertEqual(
            msg['helper_metadata']['session']['session_end'], 32)

    @inlineCallbacks
    def test_incoming_message_session_none_no_overwrite(self):
        mw = yield self.mk_middleware()

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_start=23,
            session_event=SESSION_NONE)

        msg = yield mw.handle_inbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23)

    @inlineCallbacks
    def test_incoming_message_session_start_custom_key(self):
        mw = yield self.mk_middleware(key_namespace='foo')

        msg = self.mk_msg('+12345', '+54321')
        msg = yield mw.handle_inbound(msg, "dummy_connector")

        value = yield self.redis.get('foo:+54321:session_created')
        self.assertEqual(value, '0.0')

    @inlineCallbacks
    def test_incoming_message_session_end_custom_key(self):
        mw = yield self.mk_middleware(key_namespace='foo')
        yield self.redis.set('foo:+54321:session_created', '23.0')

        msg = self.mk_msg('+12345', '+54321', session_event=SESSION_CLOSE)
        msg = yield mw.handle_inbound(msg, "dummy_connector")

        value = yield self.redis.get('foo:+54321:session_created')
        self.assertEqual(value, None)

    @inlineCallbacks
    def test_incoming_message_session_none_custom_key(self):
        mw = yield self.mk_middleware(key_namespace='foo')
        yield self.redis.set('foo:+54321:session_created', '23.0')

        msg = self.mk_msg('+12345', '+54321', session_event=SESSION_NONE)
        msg = yield mw.handle_inbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

    @inlineCallbacks
    def test_incoming_message_session_start_default_key(self):
        mw = yield self.mk_middleware()

        msg = self.mk_msg('+12345', '+54321', transport_name=None)
        msg = yield mw.handle_inbound(msg, "dummy_connector")

        value = yield self.redis.get(
            '%s:+54321:session_created' % (FALLBACK_KEY,))

        self.assertEqual(value, '0.0')

    @inlineCallbacks
    def test_incoming_message_session_end_default_key(self):
        mw = yield self.mk_middleware()

        yield self.redis.set(
            '%s:+54321:session_created' % (FALLBACK_KEY,), '23.0')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_CLOSE,
            transport_name=None)

        msg = yield mw.handle_inbound(msg, "dummy_connector")

        value = yield self.redis.get(
            '%s:+54321:session_created' % (FALLBACK_KEY,))
        self.assertEqual(value, None)

    @inlineCallbacks
    def test_incoming_message_session_none_default_key(self):
        mw = yield self.mk_middleware()

        yield self.redis.set(
            '%s:+54321:session_created' % (FALLBACK_KEY,), '23.0')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_NONE,
            transport_name=None)

        msg = yield mw.handle_inbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

    @inlineCallbacks
    def test_outgoing_message_session_start(self):
        mw = yield self.mk_middleware()
        msg_start = self.mk_msg('+12345', '+54321')

        msg = yield mw.handle_outbound(msg_start, "dummy_connector")
        value = yield self.redis.get('dummy_transport:+12345:session_created')
        self.assertEqual(value, '0.0')
        self.assertEqual(
                msg['helper_metadata']['session']['session_start'], 0.0)

    @inlineCallbacks
    def test_outgoing_message_session_end(self):
        mw = yield self.mk_middleware()
        msg_end = self.mk_msg('+12345', '+54321', session_event=SESSION_CLOSE)

        msg = yield mw.handle_outbound(msg_end, "dummy_connector")
        self.assertEqual(
                msg['helper_metadata']['session']['session_end'], 0.0)

    @inlineCallbacks
    def test_outgoing_message_session_start_end(self):
        mw = yield self.mk_middleware()
        msg_start = self.mk_msg('+12345', '+54321')
        msg_end = self.mk_msg('+12345', '+54321', session_event=SESSION_CLOSE)

        msg = yield mw.handle_outbound(msg_start, "dummy_connector")
        value = yield self.redis.get('dummy_transport:+12345:session_created')
        self.assertEqual(value, '0.0')
        self.assertEqual(
                msg['helper_metadata']['session']['session_start'], 0.0)

        self.clock.advance(1.0)
        msg = yield mw.handle_outbound(msg_end, "dummy_connector")
        value = yield self.redis.get('dummy_transport:+54321:session_created')
        self.assertTrue(value is None)
        self.assertEqual(
                msg['helper_metadata']['session']['session_start'], 0.0)
        self.assertEqual(
            msg['helper_metadata']['session']['session_end'], 1.0)

    @inlineCallbacks
    def test_outgoing_message_session_start_no_overwrite(self):
        mw = yield self.mk_middleware()

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_start=23,
            session_event=SESSION_NEW)

        yield self.redis.set('dummy_transport:+12345:session_created', '23')

        msg = yield mw.handle_outbound(msg, "dummy_connector")
        value = yield self.redis.get('dummy_transport:+12345:session_created')
        self.assertEqual(value, '23')

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23)

    @inlineCallbacks
    def test_outgoing_message_session_end_no_overwrite(self):
        mw = yield self.mk_middleware()

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_start=23,
            session_end=32,
            session_event=SESSION_CLOSE)

        msg = yield mw.handle_outbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23)

        self.assertEqual(
            msg['helper_metadata']['session']['session_end'], 32)

    @inlineCallbacks
    def test_outgoing_message_session_none_no_overwrite(self):
        mw = yield self.mk_middleware()

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_start=23,
            session_event=SESSION_NONE)

        msg = yield mw.handle_outbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23)

    @inlineCallbacks
    def test_outgoing_message_session_start_custom_key(self):
        mw = yield self.mk_middleware(key_namespace='foo')

        msg = self.mk_msg('+12345', '+54321')
        msg = yield mw.handle_outbound(msg, "dummy_connector")

        value = yield self.redis.get('foo:+12345:session_created')
        self.assertEqual(value, '0.0')

    @inlineCallbacks
    def test_outgoing_message_session_end_custom_key(self):
        mw = yield self.mk_middleware(key_namespace='foo')
        yield self.redis.set('foo:+12345:session_created', '23.0')

        msg = self.mk_msg('+12345', '+54321', session_event=SESSION_CLOSE)
        msg = yield mw.handle_outbound(msg, "dummy_connector")

        value = yield self.redis.get('foo:+12345:session_created')
        self.assertEqual(value, None)

    @inlineCallbacks
    def test_outgoing_message_session_none_custom_key(self):
        mw = yield self.mk_middleware(key_namespace='foo')
        yield self.redis.set('foo:+12345:session_created', '23.0')

        msg = self.mk_msg('+12345', '+54321', session_event=SESSION_NONE)
        msg = yield mw.handle_outbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

    @inlineCallbacks
    def test_outgoing_message_session_start_default_key(self):
        mw = yield self.mk_middleware()

        msg = self.mk_msg('+12345', '+54321', transport_name=None)
        msg = yield mw.handle_outbound(msg, "dummy_connector")

        value = yield self.redis.get(
            '%s:+12345:session_created' % (FALLBACK_KEY,))

        self.assertEqual(value, '0.0')

    @inlineCallbacks
    def test_outgoing_message_session_end_default_key(self):
        mw = yield self.mk_middleware()

        yield self.redis.set(
            '%s:+12345:session_created' % (FALLBACK_KEY,), '23.0')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_CLOSE,
            transport_name=None)

        msg = yield mw.handle_outbound(msg, "dummy_connector")

        value = yield self.redis.get(
            '%s:+12345:session_created' % (FALLBACK_KEY,))
        self.assertEqual(value, None)

    @inlineCallbacks
    def test_outgoing_message_session_none_default_key(self):
        mw = yield self.mk_middleware()

        yield self.redis.set(
            '%s:+12345:session_created' % (FALLBACK_KEY,), '23.0')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_NONE,
            transport_name=None)

        msg = yield mw.handle_outbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

    @inlineCallbacks
    def test_redis_key_timeout(self):
        mw = yield self.mk_middleware()
        msg_start = self.mk_msg('+12345', '+54321')

        yield mw.handle_inbound(msg_start, "dummy_connector")
        ttl = yield self.redis.ttl('dummy_transport::+54321:session_created')
        self.assertTrue(ttl <= 120)

    @inlineCallbacks
    def test_redis_key_custom_timeout(self):
        mw = yield self.mk_middleware(timeout=20)
        msg_start = self.mk_msg('+12345', '+54321')

        yield mw.handle_inbound(msg_start, "dummy_connector")
        ttl = yield self.redis.ttl('dummy_transport:+54321:session_created')
        self.assertTrue(ttl <= 20)

    @inlineCallbacks
    def test_custom_message_field_name(self):
        mw = yield self.mk_middleware(field_name='foobar')
        msg_start = self.mk_msg('+12345', '+54321')

        msg = yield mw.handle_inbound(msg_start, "dummy_connector")
        self.assertEqual(
            msg['helper_metadata']['foobar']['session_start'], 0.0)
