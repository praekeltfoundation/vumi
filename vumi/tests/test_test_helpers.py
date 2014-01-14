from twisted.trial.unittest import TestCase

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
