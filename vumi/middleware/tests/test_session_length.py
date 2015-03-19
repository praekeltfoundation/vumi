"""Tests for vumi.middleware.session_length."""

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import Clock

from vumi.message import TransportUserMessage
from vumi.middleware.session_length import (
    SessionLengthMiddleware, SessionLengthMiddlewareError)
from vumi.middleware.tagger import TaggingMiddleware
from vumi.tests.utils import LogCatcher
from vumi.tests.helpers import VumiTestCase, PersistenceHelper

SESSION_NEW, SESSION_CLOSE, SESSION_NONE = (
    TransportUserMessage.SESSION_NEW,
    TransportUserMessage.SESSION_CLOSE,
    TransportUserMessage.SESSION_NONE)


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
               session_start=None, session_end=None, tag=None,
               transport_name='dummy_transport'):
        msg = TransportUserMessage(
            to_addr=to_addr, from_addr=from_addr,
            transport_name=transport_name,
            transport_type="dummy_transport_type",
            session_event=session_event)

        if tag is not None:
            TaggingMiddleware.add_tag_to_msg(msg, tag)

        if session_start is not None:
            self._set_metadata(msg, 'session_start', session_start)

        if session_end is not None:
            self._set_metadata(msg, 'session_end', session_end)

        return msg

    def _set_metadata(self, msg, name, value, metadata_field_name='session'):
        metadata = msg['helper_metadata'].setdefault(metadata_field_name, {})
        metadata[name] = value

    def assert_middleware_error(self, msg):
        [err] = self.flushLoggedErrors(SessionLengthMiddlewareError)
        self.assertEqual(str(err.value), msg)

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

        yield self.redis.set('dummy_transport:+12345:session_created', '23.0')

        msg = yield mw.handle_inbound(msg, "dummy_connector")
        value = yield self.redis.get('dummy_transport:+12345:session_created')
        self.assertEqual(value, '23.0')

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

    @inlineCallbacks
    def test_incoming_message_session_end_no_overwrite(self):
        mw = yield self.mk_middleware()

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_start=23.0,
            session_end=32.0,
            session_event=SESSION_CLOSE)

        msg = yield mw.handle_inbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

        self.assertEqual(
            msg['helper_metadata']['session']['session_end'], 32.0)

    @inlineCallbacks
    def test_incoming_message_session_none_no_overwrite(self):
        mw = yield self.mk_middleware()

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_start=23.0,
            session_event=SESSION_NONE)

        msg = yield mw.handle_inbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

    @inlineCallbacks
    def test_incoming_message_session_start_transport_namespace_type(self):
        mw = yield self.mk_middleware(namespace_type='transport_name')

        msg = self.mk_msg('+12345', '+54321', transport_name='foo')
        msg = yield mw.handle_inbound(msg, "dummy_connector")

        value = yield self.redis.get('foo:+54321:session_created')
        self.assertEqual(value, '0.0')

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 0.0)

    @inlineCallbacks
    def test_incoming_message_session_end_transport_namespace_type(self):
        mw = yield self.mk_middleware(namespace_type='transport_name')
        yield self.redis.set('foo:+54321:session_created', '23.0')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_CLOSE,
            transport_name='foo')

        msg = yield mw.handle_inbound(msg, "dummy_connector")

        value = yield self.redis.get('foo:+54321:session_created')
        self.assertEqual(value, None)

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

        self.assertEqual(
            msg['helper_metadata']['session']['session_end'], 0.0)

    @inlineCallbacks
    def test_incoming_message_session_none_transport_namespace_type(self):
        mw = yield self.mk_middleware(namespace_type='transport_name')
        yield self.redis.set('foo:+54321:session_created', '23.0')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_CLOSE,
            transport_name='foo')

        msg = yield mw.handle_inbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

    @inlineCallbacks
    def test_incoming_message_session_start_no_transport_name(self):
        mw = yield self.mk_middleware(namespace_type='transport_name')
        msg = self.mk_msg('+12345', '+54321', transport_name=None)

        result_msg = yield mw.handle_inbound(msg, "dummy_connector")
        self.assert_middleware_error(
            "Session length redis namespace cannot be None, skipping message")

        self.assertEqual(msg, result_msg)
        self.assertTrue('session' not in msg['helper_metadata'])

    @inlineCallbacks
    def test_incoming_message_session_end_no_transport_name(self):
        mw = yield self.mk_middleware(namespace_type='transport_name')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_CLOSE,
            transport_name=None)

        result_msg = yield mw.handle_inbound(msg, "dummy_connector")
        self.assert_middleware_error(
            "Session length redis namespace cannot be None, skipping message")

        self.assertEqual(msg, result_msg)
        self.assertTrue('session' not in msg['helper_metadata'])

    @inlineCallbacks
    def test_incoming_message_session_none_no_transport_name(self):
        mw = yield self.mk_middleware(namespace_type='transport_name')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_NONE,
            transport_name=None)

        result_msg = yield mw.handle_inbound(msg, "dummy_connector")
        self.assert_middleware_error(
            "Session length redis namespace cannot be None, skipping message")

        self.assertEqual(msg, result_msg)
        self.assertTrue('session' not in msg['helper_metadata'])

    @inlineCallbacks
    def test_incoming_message_session_start_tag_namespace_type(self):
        mw = yield self.mk_middleware(namespace_type='tag')

        msg = self.mk_msg('+12345', '+54321', tag=('pool1', 'tag1'))
        msg = yield mw.handle_inbound(msg, "dummy_connector")

        value = yield self.redis.get('pool1:tag1:+54321:session_created')
        self.assertEqual(value, '0.0')

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 0.0)

    @inlineCallbacks
    def test_incoming_message_session_end_tag_namespace_type(self):
        mw = yield self.mk_middleware(namespace_type='tag')
        yield self.redis.set('pool1:tag1:+54321:session_created', '23.0')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_CLOSE,
            tag=('pool1', 'tag1'))

        msg = yield mw.handle_inbound(msg, "dummy_connector")

        value = yield self.redis.get('pool1:tag1:+54321:session_created')
        self.assertEqual(value, None)

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

        self.assertEqual(
            msg['helper_metadata']['session']['session_end'], 0.0)

    @inlineCallbacks
    def test_incoming_message_session_none_tag_namespace_type(self):
        mw = yield self.mk_middleware(namespace_type='tag')
        yield self.redis.set('pool1:tag1:+54321:session_created', '23.0')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_CLOSE,
            tag=('pool1', 'tag1'))

        msg = yield mw.handle_inbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

    @inlineCallbacks
    def test_incoming_message_session_start_no_tag(self):
        mw = yield self.mk_middleware(namespace_type='tag')

        # create message with no tag
        msg = self.mk_msg('+12345', '+54321')

        result_msg = yield mw.handle_inbound(msg, "dummy_connector")
        self.assert_middleware_error(
            "Session length redis namespace cannot be None, skipping message")

        self.assertEqual(msg, result_msg)
        self.assertTrue('session' not in msg['helper_metadata'])

    @inlineCallbacks
    def test_incoming_message_session_end_tag_no_tag(self):
        mw = yield self.mk_middleware(namespace_type='tag')

        # create message with no tag
        msg = self.mk_msg('+12345', '+54321', session_event=SESSION_CLOSE)

        result_msg = yield mw.handle_inbound(msg, "dummy_connector")
        self.assert_middleware_error(
            "Session length redis namespace cannot be None, skipping message")

        self.assertEqual(msg, result_msg)
        self.assertTrue('session' not in msg['helper_metadata'])

    @inlineCallbacks
    def test_incoming_message_session_none_tag_no_tag(self):
        mw = yield self.mk_middleware(namespace_type='tag')
        yield self.redis.set('pool1:tag1:+54321:session_created', '23.0')

        # create message with no tag
        msg = self.mk_msg('+12345', '+54321', session_event=SESSION_NONE)

        result_msg = yield mw.handle_inbound(msg, "dummy_connector")
        self.assert_middleware_error(
            "Session length redis namespace cannot be None, skipping message")

        self.assertEqual(msg, result_msg)
        self.assertTrue('session' not in msg['helper_metadata'])

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
    def test_outgoing_message_session_start_no_transport_name(self):
        mw = yield self.mk_middleware(namespace_type='transport_name')

        msg = self.mk_msg('+12345', '+54321', transport_name=None)

        result_msg = yield mw.handle_outbound(msg, "dummy_connector")
        self.assert_middleware_error(
            "Session length redis namespace cannot be None, skipping message")

        self.assertEqual(msg, result_msg)
        self.assertTrue('session' not in msg['helper_metadata'])

    @inlineCallbacks
    def test_outgoing_message_session_end_no_transport_name(self):
        mw = yield self.mk_middleware(namespace_type='transport_name')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_CLOSE,
            transport_name=None)

        result_msg = yield mw.handle_outbound(msg, "dummy_connector")
        self.assert_middleware_error(
            "Session length redis namespace cannot be None, skipping message")

        self.assertEqual(msg, result_msg)
        self.assertTrue('session' not in msg['helper_metadata'])

    @inlineCallbacks
    def test_outgoing_message_session_none_no_transport_name(self):
        mw = yield self.mk_middleware(namespace_type='transport_name')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_NONE,
            transport_name=None)

        result_msg = yield mw.handle_outbound(msg, "dummy_connector")
        self.assert_middleware_error(
            "Session length redis namespace cannot be None, skipping message")

        self.assertEqual(msg, result_msg)
        self.assertTrue('session' not in msg['helper_metadata'])

    @inlineCallbacks
    def test_outgoing_message_session_start_tag_namespace_type(self):
        mw = yield self.mk_middleware(namespace_type='tag')

        msg = self.mk_msg('+12345', '+54321', tag=('pool1', 'tag1'))
        msg = yield mw.handle_outbound(msg, "dummy_connector")

        value = yield self.redis.get('pool1:tag1:+12345:session_created')
        self.assertEqual(value, '0.0')

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 0.0)

    @inlineCallbacks
    def test_outgoing_message_session_end_tag_namespace_type(self):
        mw = yield self.mk_middleware(namespace_type='tag')
        yield self.redis.set('pool1:tag1:+12345:session_created', '23.0')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_CLOSE,
            tag=('pool1', 'tag1'))

        msg = yield mw.handle_outbound(msg, "dummy_connector")

        value = yield self.redis.get('tag1:+12345:session_created')
        self.assertEqual(value, None)

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

        self.assertEqual(
            msg['helper_metadata']['session']['session_end'], 0.0)

    @inlineCallbacks
    def test_outgoing_message_session_none_tag_namespace_type(self):
        mw = yield self.mk_middleware(namespace_type='tag')
        yield self.redis.set('pool1:tag1:+12345:session_created', '23.0')

        msg = self.mk_msg(
            '+12345',
            '+54321',
            session_event=SESSION_NONE,
            tag=('pool1', 'tag1'))

        msg = yield mw.handle_outbound(msg, "dummy_connector")

        self.assertEqual(
            msg['helper_metadata']['session']['session_start'], 23.0)

    @inlineCallbacks
    def test_outgoing_message_session_start_no_tag(self):
        mw = yield self.mk_middleware(namespace_type='tag')

        # create message with no tag
        msg = self.mk_msg('+12345', '+54321')

        result_msg = yield mw.handle_outbound(msg, "dummy_connector")
        self.assert_middleware_error(
            "Session length redis namespace cannot be None, skipping message")

        self.assertEqual(msg, result_msg)
        self.assertTrue('session' not in msg['helper_metadata'])

    @inlineCallbacks
    def test_outgoing_message_session_end_tag_no_tag(self):
        mw = yield self.mk_middleware(namespace_type='tag')

        # create message with no tag
        msg = self.mk_msg('+12345', '+54321', session_event=SESSION_CLOSE)

        result_msg = yield mw.handle_outbound(msg, "dummy_connector")
        self.assert_middleware_error(
            "Session length redis namespace cannot be None, skipping message")

        self.assertEqual(msg, result_msg)
        self.assertTrue('session' not in msg['helper_metadata'])

    @inlineCallbacks
    def test_outgoing_message_session_none_tag_no_tag(self):
        mw = yield self.mk_middleware(namespace_type='tag')
        yield self.redis.set('pool1:tag1:+54321:session_created', '23.0')

        # create message with no tag
        msg = self.mk_msg('+12345', '+54321', session_event=SESSION_NONE)

        result_msg = yield mw.handle_outbound(msg, "dummy_connector")
        self.assert_middleware_error(
            "Session length redis namespace cannot be None, skipping message")

        self.assertEqual(msg, result_msg)
        self.assertTrue('session' not in msg['helper_metadata'])

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

    @inlineCallbacks
    def test_incoming_message_session_no_from_addr(self):
        mw = yield self.mk_middleware()
        msg_start = self.mk_msg('+12345', None)
        msg = yield mw.handle_inbound(msg_start, "dummy_connector")
        self.assertEqual(msg, msg_start)
        self.assert_middleware_error(
            "Session length key address cannot be None, skipping message")

    @inlineCallbacks
    def test_outgoing_message_session_no_to_addr(self):
        mw = yield self.mk_middleware()
        msg_start = self.mk_msg(None, '+54321')
        msg = yield mw.handle_outbound(msg_start, "dummy_connector")
        self.assertEqual(msg, msg_start)
        self.assert_middleware_error(
            "Session length key address cannot be None, skipping message")
