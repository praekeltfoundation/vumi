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


class AddressPrefixProviderSettingMiddleware(TransportMiddleware):
    """
    Transport middleware that sets a `provider` on each inbound message based
    on configured ``from_addr`` prefixes. Outbound messages are ignored.

    Configuration options:

    :param dict provider_prefixes:
        Mapping from address prefix to provider value. Longer prefixes are
        checked first to avoid ambiguity. If no prefix matches, the provider
        value will be set to ``None``.
    """
    def setup_middleware(self):
        self.provider_prefixes = []
        prefix_mapping = self.config['provider_prefixes']
        sorted_prefixes = sorted(prefix_mapping.keys(), key=lambda p: -len(p))
        for prefix in sorted_prefixes:
            self.provider_prefixes.append((prefix, prefix_mapping[prefix]))

    def get_prefix(self, from_addr):
        for prefix, provider in self.provider_prefixes:
            if from_addr.startswith(prefix):
                return provider
        return None

    def handle_inbound(self, message, connector_name):
        message["provider"] = self.get_prefix(message["from_addr"])
        return message

    def handle_outbound(self, message, connector_name):
        return message
