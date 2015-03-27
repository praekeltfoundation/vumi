"""Tests for vumi.middleware.provider_setter."""

from vumi.middleware.provider_setter import (
    StaticProviderSettingMiddleware, AddressPrefixProviderSettingMiddleware,
    ProviderSettingMiddlewareError)
from vumi.tests.helpers import VumiTestCase, MessageHelper


class TestStaticProviderSettingMiddleware(VumiTestCase):
    def setUp(self):
        self.msg_helper = self.add_helper(MessageHelper())

    def mk_middleware(self, config):
        dummy_worker = object()
        mw = StaticProviderSettingMiddleware(
            "static_provider_setter", config, dummy_worker)
        mw.setup_middleware()
        return mw

    def test_set_provider_on_inbound_if_unset(self):
        """
        The statically configured provider value is set on inbound messages
        that have no provider.
        """
        mw = self.mk_middleware({"provider": "MY-MNO"})
        msg = self.msg_helper.make_inbound(None)
        self.assertEqual(msg.get("provider"), None)
        processed_msg = mw.handle_inbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), "MY-MNO")

    def test_replace_provider_on_inbound_if_set(self):
        """
        The statically configured provider value replaces any existing provider
        a message may already have set.
        """
        mw = self.mk_middleware({"provider": "MY-MNO"})
        msg = self.msg_helper.make_inbound(None, provider="YOUR-MNO")
        self.assertEqual(msg.get("provider"), "YOUR-MNO")
        processed_msg = mw.handle_inbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), "MY-MNO")

    def test_set_provider_on_outbound_if_unset(self):
        """
        Outbound messages are left as they are.
        """
        mw = self.mk_middleware({"provider": "MY-MNO"})
        msg = self.msg_helper.make_outbound(None)
        self.assertEqual(msg.get("provider"), None)
        processed_msg = mw.handle_outbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), "MY-MNO")


class TestAddressPrefixProviderSettingMiddleware(VumiTestCase):
    def setUp(self):
        self.msg_helper = self.add_helper(MessageHelper())

    def mk_middleware(self, config):
        dummy_worker = object()
        mw = AddressPrefixProviderSettingMiddleware(
            "address_prefix_provider_setter", config, dummy_worker)
        mw.setup_middleware()
        return mw

    def assert_middleware_error(self, msg):
        [err] = self.flushLoggedErrors(ProviderSettingMiddlewareError)
        self.assertEqual(str(err.value), msg)

    def test_set_provider_unique_matching_prefix(self):
        """
        If exactly one prefix matches the address, its corresponding provider
        value is set on the inbound message.
        """
        mw = self.mk_middleware({"provider_prefixes": {
            "+123": "MY-MNO",
            "+124": "YOUR-MNO",
        }})
        msg = self.msg_helper.make_inbound(None, from_addr="+12345")
        self.assertEqual(msg.get("provider"), None)
        processed_msg = mw.handle_inbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), "MY-MNO")

    def test_set_provider_longest_matching_prefix(self):
        """
        If more than one prefix matches the address, the provider value for
        the longest matching prefix is set on the inbound message.
        """
        mw = self.mk_middleware({"provider_prefixes": {
            "+12": "YOUR-MNO",
            "+123": "YOUR-MNO",
            "+1234": "YOUR-MNO",
            "+12345": "MY-MNO",
            "+123456": "YOUR-MNO",
        }})
        msg = self.msg_helper.make_inbound(None, from_addr="+12345")
        self.assertEqual(msg.get("provider"), None)
        processed_msg = mw.handle_inbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), "MY-MNO")

    def test_no_provider_for_no_matching_prefix(self):
        """
        If no prefix matches the address, the provider value will be set to
        ``None`` on the inbound message.
        """
        mw = self.mk_middleware({"provider_prefixes": {
            "+124": "YOUR-MNO",
            "+125": "YOUR-MNO",
        }})
        msg = self.msg_helper.make_inbound(None, from_addr="+12345")
        self.assertEqual(msg.get("provider"), None)
        processed_msg = mw.handle_inbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), None)

    def test_set_provider_no_normalize_msisdn(self):
        """
        If exactly one prefix matches the address, its corresponding provider
        value is set on the inbound message.
        """
        mw = self.mk_middleware({
            "provider_prefixes": {
                "083": "MY-MNO",
                "+2783": "YOUR-MNO",
            },
        })
        msg = self.msg_helper.make_inbound(None, from_addr="0831234567")
        self.assertEqual(msg.get("provider"), None)
        processed_msg = mw.handle_inbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), "MY-MNO")

    def test_set_provider_normalize_msisdn(self):
        """
        If exactly one prefix matches the address, its corresponding provider
        value is set on the inbound message.
        """
        mw = self.mk_middleware({
            "normalize_msisdn": {"country_code": "27"},
            "provider_prefixes": {
                "083": "YOUR-MNO",
                "+2783": "MY-MNO",
            },
        })
        msg = self.msg_helper.make_inbound(None, from_addr="0831234567")
        self.assertEqual(msg.get("provider"), None)
        processed_msg = mw.handle_inbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), "MY-MNO")

    def test_set_provider_normalize_msisdn_strip_plus(self):
        """
        If exactly one prefix matches the address, its corresponding provider
        value is set on the inbound message.
        """
        mw = self.mk_middleware({
            "normalize_msisdn": {"country_code": "27", "strip_plus": True},
            "provider_prefixes": {
                "083": "YOUR-MNO",
                "+2783": "YOUR-MNO",
                "2783": "MY-MNO",
            },
        })
        msg = self.msg_helper.make_inbound(None, from_addr="0831234567")
        self.assertEqual(msg.get("provider"), None)
        processed_msg = mw.handle_inbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), "MY-MNO")

    def test_set_provider_on_outbound(self):
        """
        Outbound messages are left as they are.
        """
        mw = self.mk_middleware({"provider_prefixes": {"+123": "MY-MNO"}})
        msg = self.msg_helper.make_outbound(
            None, to_addr="+1234567", from_addr="+12345")
        self.assertEqual(msg.get("provider"), None)
        processed_msg = mw.handle_outbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), "MY-MNO")

    def test_provider_not_overwritten_for_inbound(self):
        """
        If a provider already exists for an inbound message, it isn't
        overwritten.
        """
        mw = self.mk_middleware({"provider_prefixes": {"+123": "MY-MNO"}})
        msg = self.msg_helper.make_inbound(
            None, to_addr="+345", from_addr="+12345", provider="OTHER-MNO")
        processed_msg = mw.handle_inbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), "OTHER-MNO")

    def test_provider_not_overwritten_for_outbound(self):
        """
        If a provider already exists for an outbound message, it isn't
        overwritten.
        """
        mw = self.mk_middleware({"provider_prefixes": {"+123": "MY-MNO"}})
        msg = self.msg_helper.make_outbound(
            None, to_addr="+1234567", from_addr="+345", provider="OTHER-MNO")
        processed_msg = mw.handle_outbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), "OTHER-MNO")

    def test_provider_logs_no_address_error_for_inbound(self):
        """
        If the from_addr of an inbound message is None, an error should be
        logged and the message returned.
        """
        mw = self.mk_middleware({"provider_prefixes": {"+123": "MY-MNO"}})
        msg = self.msg_helper.make_inbound(
            None, to_addr="+1234567", from_addr=None)
        processed_msg = mw.handle_inbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), None)
        self.assert_middleware_error(
            "Address for determining message provider cannot be None,"
            " skipping message")

    def test_provider_logs_no_address_error_for_outbound(self):
        """
        If the to_addr of an outbound message is None, an error should be
        logged and the message returned.
        """
        mw = self.mk_middleware({"provider_prefixes": {"+123": "MY-MNO"}})
        msg = self.msg_helper.make_outbound(
            None, to_addr=None, from_addr="+345")
        processed_msg = mw.handle_outbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), None)
        self.assert_middleware_error(
            "Address for determining message provider cannot be None,"
            " skipping message")
