# -*- test-case-name: vumi.transports.parlayx.tests.test_server -*-
import iso8601
from collections import namedtuple

from twisted.internet.defer import maybeDeferred, fail
from twisted.python import log
from twisted.python.constants import Values, ValueConstant
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.parlayx.client import PARLAYX_COMMON_NS
from vumi.transports.parlayx.soaputil import (
    soap_envelope, unwrap_soap_envelope, soap_fault, SoapFault)
from vumi.transports.parlayx.xmlutil import (
    Namespace, elemfind, gettext, split_qualified, parse_document, tostring)
from vumi.utils import normalize_msisdn


NOTIFICATION_NS = Namespace(
    'http://www.csapi.org/schema/parlayx/sms/notification/v2_2/local', 'loc')


def normalize_address(address):
    """
    Normalize a ParlayX address.
    """
    if address.startswith('tel:'):
        address = address[4:]
    return normalize_msisdn(address)


class DeliveryStatus(Values):
    """
    ParlayX `DeliveryStatus` enumeration type.
    """
    DeliveredToNetwork = ValueConstant('delivered')
    DeliveryUncertain = ValueConstant('pending')
    DeliveryImpossible = ValueConstant('failed')
    MessageWaiting = ValueConstant('pending')
    DeliveredToTerminal = ValueConstant('delivered')
    DeliveryNotificationNotSupported = ValueConstant('failed')


class SmsMessage(namedtuple('SmsMessage',
                            ['message', 'sender_address',
                             'service_activation_number', 'timestamp'])):
    """
    ParlayX `SmsMessage` complex type.
    """
    @classmethod
    def from_element(cls, root):
        """
        Create an `SmsMessage` instance from an ElementTree element.
        """
        return cls(
            message=gettext(root, 'message'),
            sender_address=gettext(
                root, 'senderAddress', parse=normalize_address),
            service_activation_number=gettext(
                root, 'smsServiceActivationNumber', parse=normalize_address),
            timestamp=gettext(root, 'dateTime', parse=iso8601.parse_date))


class DeliveryInformation(namedtuple('DeliveryInformation',
                                     ['address', 'delivery_status'])):
    """
    ParlayX `DeliveryInformation` complex type.
    """
    @classmethod
    def from_element(cls, root):
        """
        Create a `DeliveryInformation` instance from an ElementTree element.
        """
        try:
            delivery_status = gettext(
                root, 'deliveryStatus', parse=DeliveryStatus.lookupByName)
        except ValueError, e:
            raise ValueError(
                'No such delivery status enumeration value: %r' % (str(e),))
        else:
            return cls(
                address=gettext(root, 'address', parse=normalize_address),
                delivery_status=delivery_status)


class SmsNotificationService(Resource):
    """
    Web resource to handle SOAP requests for ParlayX SMS deliveries and
    delivery receipts.
    """
    isLeaf = True

    def __init__(self, callback_message_received, callback_message_delivered):
        self.callback_message_received = callback_message_received
        self.callback_message_delivered = callback_message_delivered
        Resource.__init__(self)

    def render_POST(self, request):
        """
        Process a SOAP request and convert any exceptions into SOAP faults.
        """
        def _writeResponse(response):
            request.setHeader('Content-Type', 'text/xml; charset="utf-8"')
            request.write(tostring(soap_envelope(response)))
            request.finish()

        def _handleSuccess(result):
            request.setResponseCode(http.OK)
            return result

        def _handleError(f):
            # XXX: Perhaps report this back to the transport somehow???
            log.err(f, 'Failure processing SOAP request')
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            faultcode = u'soapenv:Server'
            if f.check(SoapFault):
                return f.value.to_element()
            return soap_fault(faultcode, f.getErrorMessage())

        try:
            tree = parse_document(request.content)
            body, header = unwrap_soap_envelope(tree)
        except:
            d = fail()
        else:
            d = maybeDeferred(self.process, request, body, header)
            d.addCallback(_handleSuccess)

        d.addErrback(_handleError)
        d.addCallback(_writeResponse)
        return NOT_DONE_YET

    def process(self, request, body, header=None):
        """
        Process a SOAP request.
        """
        for child in body.getchildren():
            # Since there is no SOAPAction header, and these requests are not
            # made to different endpoints, the only way to handle these is to
            # switch on the root element's name. Yuck.
            localname = split_qualified(child.tag)[1]
            meth = getattr(self, 'process_' + localname, self.process_unknown)
            return meth(child, header, localname)

        raise SoapFault(u'soapenv:Client', u'No actionable items')

    def process_unknown(self, root, header, name):
        """
        Process unknown notification deliverables.
        """
        raise SoapFault(u'soapenv:Server', u'No handler for %s' % (name,))

    def process_notifySmsReception(self, root, header, name):
        """
        Process a received text message.
        """
        linkid = None
        if header is not None:
            linkid = gettext(header, './/' + str(PARLAYX_COMMON_NS.linkid))

        correlator = gettext(root, NOTIFICATION_NS.correlator)
        message = SmsMessage.from_element(
            elemfind(root, NOTIFICATION_NS.message))
        d = maybeDeferred(
            self.callback_message_received, correlator, linkid, message)
        d.addCallback(
            lambda ignored: NOTIFICATION_NS.notifySmsReceptionResponse())
        return d

    def process_notifySmsDeliveryReceipt(self, root, header, name):
        """
        Process a text message delivery receipt.
        """
        correlator = gettext(root, NOTIFICATION_NS.correlator)
        delivery_info = DeliveryInformation.from_element(
            elemfind(root, NOTIFICATION_NS.deliveryStatus))
        d = maybeDeferred(self.callback_message_delivered,
            correlator, delivery_info.delivery_status.value)
        d.addCallback(
            lambda ignored: NOTIFICATION_NS.notifySmsDeliveryReceiptResponse())
        return d


# XXX: Only used for debugging with SoapUI:
# twistd web --class=vumi.transports.parlayx.server.Root --port=9080
class Root(Resource):
    def getChild(self, path, request):
        from twisted.internet.defer import succeed
        noop = lambda *a, **kw: succeed(None)
        if request.postpath == ['services', 'SmsNotification']:
            return SmsNotificationService(noop, noop)
        return None


__all__ = [
    'normalize_address', 'DeliveryStatus', 'SmsMessage', 'DeliveryInformation',
    'SmsNotificationService']
