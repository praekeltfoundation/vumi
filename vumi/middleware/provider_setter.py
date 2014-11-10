# -*- test-case-name: vumi.middleware.tests.test_provider_setter -*-

from vumi.middleware.base import TransportMiddleware


class StaticProviderSettingMiddleware(TransportMiddleware):
    """
    Transport middleware that sets a static `provider` on each inbound message.
    Outbound messages are ignored.

    Configuration options:

    :param str provider: Value to set the `provider` field to.
    """
    def setup_middleware(self):
        self.provider_value = self.config['provider']

    def handle_inbound(self, message, connector_name):
        message["provider"] = self.provider_value
        return message

    def handle_outbound(self, message, connector_name):
        return message
