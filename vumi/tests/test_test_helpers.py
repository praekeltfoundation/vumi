from datetime import datetime

from twisted.trial.unittest import TestCase

from vumi.message import TransportUserMessage, TransportEvent
from vumi.tests.helpers import (
    proxyable, generate_proxies, IHelper, MessageHelper)


class TestHelperHelpers(TestCase):
    def test_proxyable(self):
        """
        @proxyable should set a `proxyable` attr on the func it decorates.
        """

        @proxyable
        def is_proxyable():
            pass
        self.assertTrue(hasattr(is_proxyable, 'proxyable'))

        def not_proxyable():
            pass
        self.assertFalse(hasattr(not_proxyable, 'proxyable'))

    def test_generate_proxies(self):
        """
        generate_proxies() should copy proxyable source attrs to target.
        """

        class Source(object):
            @proxyable
            def is_proxyable(self):
                return self

            def not_proxyable(self):
                pass

        class Target(object):
            pass

        source = Source()
        target = Target()

        self.assertFalse(hasattr(target, 'is_proxyable'))
        self.assertFalse(hasattr(target, 'not_proxyable'))

        generate_proxies(target, source)

        self.assertTrue(hasattr(target, 'is_proxyable'))
        self.assertFalse(hasattr(target, 'not_proxyable'))

        # `self` in both the original and proxied versions should be the source
        # rather than the target.
        self.assertEqual(source, source.is_proxyable())
        self.assertEqual(source, target.is_proxyable())

    def test_generate_proxies_multiple_sources(self):
        """
        generate_proxies() should copy attrs from multiple sources.
        """

        class Source1(object):
            @proxyable
            def is_proxyable_1(self):
                return self

        class Source2(object):
            @proxyable
            def is_proxyable_2(self):
                return self

        class Target(object):
            pass

        source1 = Source1()
        source2 = Source2()
        target = Target()

        self.assertFalse(hasattr(target, 'is_proxyable_1'))
        self.assertFalse(hasattr(target, 'is_proxyable_2'))

        generate_proxies(target, source1)
        generate_proxies(target, source2)

        self.assertTrue(hasattr(target, 'is_proxyable_1'))
        self.assertTrue(hasattr(target, 'is_proxyable_2'))

        # `self` in the proxied versions should be the appropriate source.
        self.assertEqual(source1, target.is_proxyable_1())
        self.assertEqual(source2, target.is_proxyable_2())

    def test_generate_proxies_multiple_sources_overlap(self):
        """
        generate_proxies() shouldn't copy proxyables with existing names.
        """

        class Source1(object):
            @proxyable
            def is_proxyable(self):
                return self

        class Source2(object):
            @proxyable
            def is_proxyable(self):
                return self

        class Target(object):
            pass

        source1 = Source1()
        source2 = Source2()
        target = Target()

        generate_proxies(target, source1)
        err = self.assertRaises(Exception, generate_proxies, target, source2)
        self.assertTrue('is_proxyable' in err.args[0])


class TestMessageHelper(TestCase):
    def assert_message_fields(self, msg, field_dict):
        self.assertEqual(field_dict, dict(
            (k, v) for k, v in msg.payload.iteritems() if k in field_dict))

    def test_implements_IHelper(self):
        """
        MessageHelper instances should provide the IHelper interface.
        """
        self.assertTrue(IHelper.providedBy(MessageHelper()))

    def test_defaults(self):
        """
        MessageHelper instances should have the expected parameters defaults.
        """
        msg_helper = MessageHelper()
        self.assertEqual(msg_helper.transport_name, 'sphex')
        self.assertEqual(msg_helper.transport_type, 'sms')
        self.assertEqual(msg_helper.mobile_addr, '+41791234567')
        self.assertEqual(msg_helper.transport_addr, '9292')

    def test_setup_sync(self):
        """
        MessageHelper.setup() should return ``None``, not a Deferred.
        """
        msg_helper = MessageHelper()
        self.assertEqual(msg_helper.setup(), None)

    def test_make_inbound_defaults(self):
        """
        .make_inbound() should build a message with expected default values.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_inbound('inbound message')
        self.assert_message_fields(msg, {
            'content': 'inbound message',
            'from_addr': msg_helper.mobile_addr,
            'to_addr': msg_helper.transport_addr,
            'transport_type': msg_helper.transport_type,
            'transport_name': msg_helper.transport_name,
            'helper_metadata': {},
            'transport_metadata': {},
        })

    def test_make_inbound_with_addresses(self):
        """
        .make_inbound() should build use overridden addresses if provided.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_inbound(
            'inbound message', from_addr='ib_from', to_addr='ib_to')
        self.assert_message_fields(msg, {
            'content': 'inbound message',
            'from_addr': 'ib_from',
            'to_addr': 'ib_to',
            'transport_type': msg_helper.transport_type,
            'transport_name': msg_helper.transport_name,
            'helper_metadata': {},
            'transport_metadata': {},
        })

    def test_make_inbound_with_helper_metadata(self):
        """
        .make_inbound() should use overridden helper_metadata if provided.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_inbound('inbound message', helper_metadata={
            'foo': {'bar': 'baz'},
            'quux': {},
        })
        self.assert_message_fields(msg, {
            'content': 'inbound message',
            'from_addr': msg_helper.mobile_addr,
            'to_addr': msg_helper.transport_addr,
            'transport_type': msg_helper.transport_type,
            'transport_name': msg_helper.transport_name,
            'helper_metadata': {
                'foo': {'bar': 'baz'},
                'quux': {},
            },
            'transport_metadata': {},
        })

    def test_make_outbound_defaults(self):
        """
        .make_outbound() should build a message with expected default values.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_outbound('outbound message')
        self.assert_message_fields(msg, {
            'content': 'outbound message',
            'from_addr': msg_helper.transport_addr,
            'to_addr': msg_helper.mobile_addr,
            'transport_type': msg_helper.transport_type,
            'transport_name': msg_helper.transport_name,
            'helper_metadata': {},
            'transport_metadata': {},
        })

    def test_make_outbound_with_addresses(self):
        """
        .make_outbound() should build use overridden addresses if provided.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_outbound(
            'outbound message', from_addr='ob_from', to_addr='ob_to')
        self.assert_message_fields(msg, {
            'content': 'outbound message',
            'from_addr': 'ob_from',
            'to_addr': 'ob_to',
            'transport_type': msg_helper.transport_type,
            'transport_name': msg_helper.transport_name,
            'helper_metadata': {},
            'transport_metadata': {},
        })

    def test_make_outbound_with_helper_metadata(self):
        """
        .make_outbound() should use overridden helper_metadata if provided.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_outbound('outbound message', helper_metadata={
            'foo': {'bar': 'baz'},
            'quux': {},
        })
        self.assert_message_fields(msg, {
            'content': 'outbound message',
            'from_addr': msg_helper.transport_addr,
            'to_addr': msg_helper.mobile_addr,
            'transport_type': msg_helper.transport_type,
            'transport_name': msg_helper.transport_name,
            'helper_metadata': {
                'foo': {'bar': 'baz'},
                'quux': {},
            },
            'transport_metadata': {},
        })

    def test_make_user_message_defaults(self):
        """
        .make_user_message() should build a message with expected values.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_user_message('outbound message', 'from', 'to')
        expected_msg = TransportUserMessage(
            content='outbound message', from_addr='from', to_addr='to',
            transport_type=msg_helper.transport_type,
            transport_name=msg_helper.transport_name,
            transport_metadata={}, helper_metadata={},
            # These fields are generated in both messages, so copy them.
            message_id=msg['message_id'], timestamp=msg['timestamp'])
        self.assertEqual(expected_msg, msg)

    def test_make_user_message_all_fields(self):
        """
        .make_user_message() should build a message with all provided fields.
        """
        msg_helper = MessageHelper()
        msg_fields = {
            'content': 'outbound message',
            'from_addr': 'from',
            'to_addr': 'to',
            'group': '#channel',
            'session_event': TransportUserMessage.SESSION_NEW,
            'transport_type': 'irc',
            'transport_name': 'vuminet',
            'transport_metadata': {'foo': 'bar'},
            'helper_metadata': {'foo': {}},
            'in_reply_to': 'ccf9c2b9b1e94433be20d157e82786fe',
            'timestamp': datetime.utcnow(),
            'message_id': 'bbf9c2b9b1e94433be20d157e82786ed',
            'endpoint': 'foo_ep',
        }
        msg = msg_helper.make_user_message(**msg_fields)
        expected_fields = msg_fields.copy()
        expected_fields.update({
            'message_type': TransportUserMessage.MESSAGE_TYPE,
            'message_version': TransportUserMessage.MESSAGE_VERSION,
            'routing_metadata': {
                'endpoint_name': expected_fields.pop('endpoint'),
            }
        })
        self.assertEqual(expected_fields, msg.payload)

    def test_make_user_message_extra_fields(self):
        """
        .make_user_message() should build a message with extra fields.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_user_message(
            'outbound message', 'from', 'to', foo='bar', baz='quux')
        self.assert_message_fields(msg, {'foo': 'bar', 'baz': 'quux'})

    def test_make_event_defaults_ack(self):
        """
        .make_event() should build an ack event with expected values.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_event('ack', 'abc123', sent_message_id='sent')
        expected_event = TransportEvent(
            event_type='ack', user_message_id='abc123', sent_message_id='sent',
            transport_type=msg_helper.transport_type,
            transport_name=msg_helper.transport_name,
            transport_metadata={}, helper_metadata={},
            # These fields are generated in both messages, so copy them.
            event_id=event['event_id'], timestamp=event['timestamp'])
        self.assertEqual(expected_event, event)

    def test_make_event_defaults_nack(self):
        """
        .make_event() should build a nack event with expected values.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_event('nack', 'abc123', nack_reason='elves')
        expected_event = TransportEvent(
            event_type='nack', user_message_id='abc123', nack_reason='elves',
            transport_type=msg_helper.transport_type,
            transport_name=msg_helper.transport_name,
            transport_metadata={}, helper_metadata={},
            # These fields are generated in both messages, so copy them.
            event_id=event['event_id'], timestamp=event['timestamp'])
        self.assertEqual(expected_event, event)

    def test_make_event_defaults_dr(self):
        """
        .make_event() should build a delivery report with expected values.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_event(
            'delivery_report', 'abc123', delivery_status='pending')
        expected_event = TransportEvent(
            event_type='delivery_report', user_message_id='abc123',
            delivery_status='pending',
            transport_type=msg_helper.transport_type,
            transport_name=msg_helper.transport_name,
            transport_metadata={}, helper_metadata={},
            # These fields are generated in both messages, so copy them.
            event_id=event['event_id'], timestamp=event['timestamp'])
        self.assertEqual(expected_event, event)

    def test_make_event_all_fields(self):
        """
        .make_event() should build an event with all provided fields.
        """
        msg_helper = MessageHelper()
        event_fields = {
            'event_type': 'ack',
            'user_message_id': 'abc123',
            'sent_message_id': '123abc',
            'transport_type': 'irc',
            'transport_name': 'vuminet',
            'transport_metadata': {'foo': 'bar'},
            'helper_metadata': {'foo': {}},

            'timestamp': datetime.utcnow(),
            'event_id': 'e6b7efecda8e42988b1e6905ad40fae1',
            'endpoint': 'foo_ep',
        }
        event = msg_helper.make_event(**event_fields)
        expected_fields = event_fields.copy()
        expected_fields.update({
            'message_type': TransportEvent.MESSAGE_TYPE,
            'message_version': TransportEvent.MESSAGE_VERSION,
            'routing_metadata': {
                'endpoint_name': expected_fields.pop('endpoint'),
            }
        })
        self.assertEqual(expected_fields, event.payload)

    def test_make_event_extra_fields(self):
        """
        .make_event() should build an event with extra fields.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_event(
            'ack', 'abc123', sent_message_id='sent', foo='bar', baz='quux')
        self.assert_message_fields(event, {'foo': 'bar', 'baz': 'quux'})

    def test_make_ack_default(self):
        """
        .make_ack() should build an ack event with expected values.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_ack()
        self.assert_message_fields(event, {
            'event_type': 'ack',
            'sent_message_id': event['user_message_id'],
        })

    def test_make_ack_with_sent_message_id(self):
        """
        .make_ack() should build an ack with the provided sent_message_id.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_ack(sent_message_id='abc123')
        self.assert_message_fields(event, {
            'event_type': 'ack',
            'sent_message_id': 'abc123',
        })

    def test_make_ack_with_message(self):
        """
        .make_ack() should build an ack event for the provided message.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_outbound('test message')
        event = msg_helper.make_ack(msg)
        self.assert_message_fields(event, {
            'event_type': 'ack',
            'user_message_id': msg['message_id'],
            'sent_message_id': msg['message_id'],
        })

    def test_make_nack_default(self):
        """
        .make_nack() should build a nack event with expected values.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_nack()
        self.assert_message_fields(event, {
            'event_type': 'nack',
            'nack_reason': 'sunspots',
        })

    def test_make_nack_with_nack_reason(self):
        """
        .make_nack() should build a nack with the provided nack_reason.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_nack(nack_reason='bogon emissions')
        self.assert_message_fields(event, {
            'event_type': 'nack',
            'nack_reason': 'bogon emissions',
        })

    def test_make_nack_with_message(self):
        """
        .make_nack() should build a nack event for the provided message.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_outbound('test message')
        event = msg_helper.make_nack(msg)
        self.assert_message_fields(event, {
            'event_type': 'nack',
            'user_message_id': msg['message_id'],
            'nack_reason': 'sunspots',
        })

    def test_make_delivery_report_default(self):
        """
        .make_delivery_report() should build an event with expected values.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_delivery_report()
        self.assert_message_fields(event, {
            'event_type': 'delivery_report',
            'delivery_status': 'delivered',
        })

    def test_make_delivery_report_with_delivery_statuss(self):
        """
        .make_delivery_report() should build an event with the provided
        delivery_status.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_delivery_report(delivery_status='pending')
        self.assert_message_fields(event, {
            'event_type': 'delivery_report',
            'delivery_status': 'pending',
        })

    def test_make_delivery_report_with_message(self):
        """
        .make_delivery_report() should build an event for the provided message.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_outbound('test message')
        event = msg_helper.make_delivery_report(msg)
        self.assert_message_fields(event, {
            'event_type': 'delivery_report',
            'user_message_id': msg['message_id'],
            'delivery_status': 'delivered',
        })
