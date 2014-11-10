"""Tests for vumi.middleware.provider_setter."""

from vumi.middleware.provider_setter import StaticProviderSettingMiddleware
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

    def test_do_not_set_provider_on_outbound(self):
        """
        Outbound messages are left as they are.
        """
        mw = self.mk_middleware({"provider": "MY-MNO"})
        msg = self.msg_helper.make_outbound(None)
        self.assertEqual(msg.get("provider"), None)
        processed_msg = mw.handle_outbound(msg, "dummy_connector")
        self.assertEqual(processed_msg.get("provider"), None)
