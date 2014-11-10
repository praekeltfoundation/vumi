# -*- test-case-name: vumi.middleware.tests.test_provider_setter -*-

from vumi.middleware.base import TransportMiddleware
from vumi.utils import normalize_msisdn


class StaticProviderSettingMiddleware(TransportMiddleware):
    """
    Transport middleware that sets a static ``provider`` on each inbound
    message. Outbound messages are ignored.

    Configuration options:

    :param str provider: Value to set the ``provider`` field to.
    """
    def setup_middleware(self):
        self.provider_value = self.config["provider"]

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

    :param dict normalize_msisdn:
        Optional MSISDN normalization config. If present, this dict should
        contain a (mandatory) ``country_code`` field and an optional boolean
        ``strip_plus`` field (default ``False``). If absent, the ``from_addr``
        field will not be normalized prior to the prefix check. (This
        normalization is only used for the prefix check. The ``from_addr``
        field on the message is not modified.)
    """
    def setup_middleware(self):
        prefixes = self.config["provider_prefixes"].items()
        self.provider_prefixes = sorted(
            prefixes, key=lambda item: -len(item[0]))
        self.normalize_config = self.config.get("normalize_msisdn", {}).copy()
        if self.normalize_config:
            assert "country_code" in self.normalize_config

    def process_from_addr(self, from_addr):
        if self.normalize_config:
            from_addr = normalize_msisdn(
                from_addr, country_code=self.normalize_config["country_code"])
            if self.normalize_config.get("strip_plus"):
                from_addr = from_addr.lstrip("+")
        return from_addr

    def get_provider(self, from_addr):
        from_addr = self.process_from_addr(from_addr)
        for prefix, provider in self.provider_prefixes:
            if from_addr.startswith(prefix):
                return provider
        return None

    def handle_inbound(self, message, connector_name):
        message["provider"] = self.get_provider(message["from_addr"])
        return message

    def handle_outbound(self, message, connector_name):
        return message
