# -*- test-case-name: vumi.middleware.tests.test_provider_setter -*-

from confmodel.fields import ConfigDict, ConfigText

from vumi import log
from vumi.errors import VumiError
from vumi.middleware.base import TransportMiddleware, BaseMiddlewareConfig
from vumi.utils import normalize_msisdn


class ProviderSettingMiddlewareError(VumiError):
    """
    Raised when provider setting middleware encounters an error.
    """


class StaticProviderSetterMiddlewareConfig(BaseMiddlewareConfig):
    """
    Config class for the static provider setting middleware.
    """
    provider = ConfigText(
        "Value to set the ``provider`` field to", required=True, static=True)


class StaticProviderSettingMiddleware(TransportMiddleware):
    """
    Transport middleware that sets a static ``provider`` on each inbound
    message and outbound message.

    Configuration options:

    :param str provider: Value to set the ``provider`` field to.

    .. note::

       If you rely on the provider value in other middleware, please
       order your middleware carefully. If another middleware requires the
       provider for both inbound and outbound messages, you might need two
       copies of this middleware (one on either side of the other middleware).
    """
    CONFIG_CLASS = StaticProviderSetterMiddlewareConfig

    def setup_middleware(self):
        self.provider_value = self.config.provider

    def handle_inbound(self, message, connector_name):
        message["provider"] = self.provider_value
        return message

    def handle_outbound(self, message, connector_name):
        message["provider"] = self.provider_value
        return message


class AddressPrefixProviderSettingMiddlewareConfig(BaseMiddlewareConfig):
    """
    Config for the address prefix provider setting middleware.
    """

    class ConfigNormalizeMsisdn(ConfigDict):
        def clean(self, value):
            if 'country_code' not in value:
                self.raise_config_error(
                    "does not contain the `country_code` key.")
            if 'strip_plus' not in value:
                value['strip_plus'] = False
            return super(self.__class__, self).clean(value)

    provider_prefixes = ConfigDict(
        "Mapping from address prefix to provider value. Longer prefixes are "
        "checked first to avoid ambiguity. If no prefix matches, the provider"
        " value will be set to ``None``.", required=True, static=True)
    normalize_msisdn = ConfigNormalizeMsisdn(
        "Optional MSISDN normalization config. If present, this dict should "
        "contain a (mandatory) ``country_code`` field and an optional boolean "
        "``strip_plus`` field (default ``False``). If absent, the "
        "``from_addr`` field will not be normalized prior to the prefix check."
        " (This normalization is only used for the prefix check. The "
        "``from_addr`` field on the message is not modified.)", static=True)


class AddressPrefixProviderSettingMiddleware(TransportMiddleware):
    """
    Transport middleware that sets a ``provider`` on each message based
    on configured address prefixes. Inbound messages have their provider set
    based on their ``from_addr``. Outbound messages have their provider set
    based on their ``to_addr``.

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

    .. note::

       If you rely on the provider value in other middleware, please
       order your middleware carefully. If another middleware requires the
       provider for both inbound and outbound messages, you might need two
       copies of this middleware (one on either side of the other middleware).
    """
    CONFIG_CLASS = AddressPrefixProviderSettingMiddlewareConfig

    def setup_middleware(self):
        prefixes = self.config.provider_prefixes.items()
        self.provider_prefixes = sorted(
            prefixes, key=lambda item: -len(item[0]))
        self.normalize_config = self.config.normalize_msisdn

    def normalize_addr(self, addr):
        if self.normalize_config:
            addr = normalize_msisdn(
                addr, country_code=self.normalize_config["country_code"])
            if self.normalize_config.get("strip_plus"):
                addr = addr.lstrip("+")
        return addr

    def get_provider(self, addr):
        if addr is None:
            log.error(ProviderSettingMiddlewareError(
                "Address for determining message provider cannot be None,"
                " skipping message"))
            return None
        addr = self.normalize_addr(addr)
        for prefix, provider in self.provider_prefixes:
            if addr.startswith(prefix):
                return provider
        return None

    def handle_inbound(self, message, connector_name):
        if message.get("provider") is None:
            message["provider"] = self.get_provider(message["from_addr"])
        return message

    def handle_outbound(self, message, connector_name):
        if message.get("provider") is None:
            message["provider"] = self.get_provider(message["to_addr"])
        return message
