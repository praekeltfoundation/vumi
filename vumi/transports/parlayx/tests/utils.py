from collections import namedtuple

from vumi.transports.parlayx.client import format_address
from vumi.transports.parlayx.soaputil import soap_envelope
from vumi.transports.parlayx.server import NOTIFICATION_NS, normalize_address
from vumi.transports.parlayx.xmlutil import LocalNamespace as L, tostring



class MockResponse(namedtuple('MockResponse', ['code', 'delivered_body'])):
    """
    Mock response from ``http_request_full``.
    """
    @classmethod
    def build(cls, code, body, header=None):
        """
        Build a `MockResponse` containing a SOAP envelope.
        """
        return cls(
            code=code,
            delivered_body=tostring(soap_envelope(body, header)))



def create_sms_reception_element(correlator, message, sender_address,
                                 service_activation_number):
    """
    Helper for creating an ``notifySmsReception`` element.
    """
    return NOTIFICATION_NS.notifySmsReception(
        NOTIFICATION_NS.correlator(correlator),
        NOTIFICATION_NS.message(
            L.message(message),
            L.senderAddress(format_address(normalize_address(sender_address))),
            L.smsServiceActivationNumber(service_activation_number)))



def create_sms_delivery_receipt(correlator, address, delivery_status):
    """
    Helper for creating an ``notifySmsDeliveryReceipt`` element.
    """
    return NOTIFICATION_NS.notifySmsDeliveryReceipt(
        NOTIFICATION_NS.correlator(correlator),
        NOTIFICATION_NS.deliveryStatus(
            L.address(format_address(normalize_address(address))),
            L.deliveryStatus(delivery_status.name)))
