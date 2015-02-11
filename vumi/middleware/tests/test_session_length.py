"""Tests for vumi.middleware.session_length."""

from vumi.middleware.session_length import SessionLengthMiddleware
from vumi.tests.helpers import VumiTestCase, MessageHelper


class TestStaticProviderSettingMiddleware(VumiTestCase):
    def setUp(self):
        self.msg_helper = self.add_helper(MessageHelper())

    def mk_middleware(self, config):
        dummy_worker = object()
        mw = SessionLengthMiddleware(
            "static_provider_setter", config, dummy_worker)
        mw.setup_middleware()
        return mw