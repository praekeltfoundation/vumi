# -*- test-case-name: vumi.middleware.tests.test_address_translator -*-

from confmodel.fields import ConfigDict

from vumi.middleware import BaseMiddleware
from vumi.middleware.base import BaseMiddlewareConfig


class AddressTranslatorMiddlewareConfig(BaseMiddlewareConfig):
    """
    Configuration class for the address translator middleware.
    """
    outbound_map = ConfigDict(
        "Mapping of old ``to_addr`` values to new ``to_addr`` values",
        required=True, static=True)


class AddressTranslationMiddleware(BaseMiddleware):
    """Address translation middleware.

    Used for mapping a set of `to_addr` values in outbound messages to
    new values. Inbound messages have the inverse mapping applied to
    their `from_addr` values..  This is useful during debugging,
    testing and development.

    For example, you might want to make your Gmail address look like
    an MSISDN to an application to test SMS address handling, for
    instance. Or you might want to have an outgoing SMS end up at your
    Gmail account.

    Configuration options:

    :param dict outbound_map:
        Mapping of old `to_addr` values to new `to_addr` values for
        outbound messages. Inbound messages have the inverse mapping
        applied to `from_addr` values. Addresses not in this dictionary
        are not affected.
    """
    CONFIG_CLASS = AddressTranslatorMiddlewareConfig

    def setup_middleware(self):
        self.outbound_map = self.config.outbound_map
        self.inbound_map = dict((v, k) for k, v in self.outbound_map.items())

    def handle_outbound(self, message, connector_name):
        fake_addr = message['to_addr']
        real_addr = self.outbound_map.get(fake_addr)
        if real_addr is not None:
            message['to_addr'] = real_addr
        return message

    def handle_inbound(self, message, connector_name):
        real_addr = message['from_addr']
        fake_addr = self.inbound_map.get(real_addr)
        if fake_addr is not None:
            message['from_addr'] = fake_addr
        return message
