import iso8601
from datetime import datetime
from StringIO import StringIO

from twisted.trial.unittest import TestCase
from twisted.web import http
from twisted.web.test.requesthelper import DummyRequest

from vumi.transports.parlayx.server import (
    NOTIFICATION_NS, PARLAYX_COMMON_NS, normalize_address, SmsMessage,
    DeliveryInformation, DeliveryStatus, SmsNotificationService)
from vumi.transports.parlayx.soaputil import SoapFault, SOAP_ENV, soap_envelope
from vumi.transports.parlayx.xmlutil import (
    ParseError, LocalNamespace as L, tostring, fromstring, element_to_dict)
from vumi.transports.parlayx.tests.utils import (
    create_sms_reception_element, create_sms_delivery_receipt)


class NormalizeAddressTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.server.normalize_address`.
    """
    def test_not_prefixed(self):
        """
        `normalize_address` still normalizes addresses that are not prefixed
        with ``tel:``.
        """
        self.assertEqual(
            '+27117654321', normalize_address('27 11 7654321'))
        self.assertEqual(
            '54321', normalize_address('54321'))

    def test_prefixed(self):
        """
        `normalize_address` strips any ``tel:`` prefix and normalizes the
        address.
        """
        self.assertEqual(
            '+27117654321', normalize_address('tel:27 11 7654321'))
        self.assertEqual(
            '+27117654321', normalize_address('tel:27117654321'))
        self.assertEqual(
            '54321', normalize_address('tel:54321'))


class SmsMessageTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.server.SmsMessage`.
    """
    def test_from_element(self):
        """
        `SmsMessage.from_element` parses a ParlayX ``SmsMessage`` complex type,
        with an ISO8601 timestamp, into an `SmsMessage` instance.
        """
        timestamp = datetime(
            2013, 6, 12, 13, 15, 0, tzinfo=iso8601.iso8601.Utc())
        msg = SmsMessage.from_element(
            NOTIFICATION_NS.message(
                L.message('message'),
                L.senderAddress('tel:27117654321'),
                L.smsServiceActivationNumber('54321'),
                L.dateTime('2013-06-12T13:15:00')))
        self.assertEqual(
            ('message', '+27117654321', '54321', timestamp),
            (msg.message, msg.sender_address, msg.service_activation_number,
             msg.timestamp))

    def test_from_element_missing_timestamp(self):
        """
        `SmsMessage.from_element` parses a ParlayX ``SmsMessage`` complex type,
        without a timestamp, into an `SmsMessage` instance.
        """
        msg = SmsMessage.from_element(
            NOTIFICATION_NS.message(
                L.message('message'),
                L.senderAddress('tel:27117654321'),
                L.smsServiceActivationNumber('54321')))
        self.assertEqual(
            ('message', '+27117654321', '54321', None),
            (msg.message, msg.sender_address, msg.service_activation_number,
             msg.timestamp))


class DeliveryInformationTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.server.DeliveryInformation`.
    """
    def test_from_element(self):
        """
        `DeliveryInformation.from_element` parses a ParlayX
        ``DeliveryInformation`` complex type into a `DeliveryInformation`
        instance. Known ``DeliveryStatus`` enumeration values are parsed into
        `DeliveryStatus` attributes.
        """
        info = DeliveryInformation.from_element(
            NOTIFICATION_NS.deliveryStatus(
                L.address('tel:27117654321'),
                L.deliveryStatus('DeliveredToNetwork')))
        self.assertEqual(
            ('+27117654321', DeliveryStatus.DeliveredToNetwork),
            (info.address, info.delivery_status))

    def test_from_element_unknown_status(self):
        """
        `DeliveryInformation.from_element` raises ``ValueError`` if an unknown
        ``DeliveryStatus`` enumeration value is specified.
        """
        e = self.assertRaises(ValueError,
            DeliveryInformation.from_element,
            NOTIFICATION_NS.deliveryStatus(
                L.address('tel:27117654321'),
                L.deliveryStatus('WhatIsThis')))
        self.assertEqual(
            "No such delivery status enumeration value: 'WhatIsThis'", str(e))


class SmsNotificationServiceTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.server.SmsNotificationService`.
    """
    def test_process_empty(self):
        """
        `SmsNotificationService.process` raises `SoapFault` if there are no
        actionable child elements in the request body.
        """
        service = SmsNotificationService(None, None)
        exc = self.assertRaises(SoapFault,
            service.process, None, L.root())
        self.assertEqual(
            ('soapenv:Client', 'No actionable items'),
            (exc.code, str(exc)))

    def test_process_unknown(self):
        """
        `SmsNotificationService.process` invokes
        `SmsNotificationService.process_unknown`, for handling otherwise
        unknown requests, which raises `SoapFault`.
        """
        service = SmsNotificationService(None, None)
        exc = self.assertRaises(SoapFault,
            service.process, None, L.root(L.WhatIsThis))
        self.assertEqual(
            ('soapenv:Server', 'No handler for WhatIsThis'),
            (exc.code, str(exc)))

    def test_process_notifySmsReception(self):
        """
        `SmsNotificationService.process_notifySmsReception` invokes the
        message delivery callback with the correlator (message identifier) and
        a `SmsMessage` instance containing the details of the delivered
        message.
        """
        def callback(*a):
            self.callbacks.append(a)
        self.callbacks = []
        service = SmsNotificationService(callback, None)
        self.successResultOf(service.process(None,
            SOAP_ENV.Body(
                create_sms_reception_element(
                    '1234', 'message', '+27117654321', '54321')),
            SOAP_ENV.Header(
                PARLAYX_COMMON_NS.NotifySOAPHeader(
                    PARLAYX_COMMON_NS.linkid('linkid')))))

        self.assertEqual(1, len(self.callbacks))
        correlator, linkid, msg = self.callbacks[0]
        self.assertEqual(
            ('1234', 'linkid', 'message', '+27117654321', '54321', None),
            (correlator, linkid, msg.message, msg.sender_address,
             msg.service_activation_number, msg.timestamp))

    def test_process_notifySmsDeliveryReceipt(self):
        """
        `SmsNotificationService.process_notifySmsDeliveryReceipt` invokes the
        delivery receipt callback with the correlator (message identifier) and
        the delivery status (translated into a Vumi-compatible value.)
        """
        def callback(*a):
            self.callbacks.append(a)
        self.callbacks = []
        service = SmsNotificationService(None, callback)
        self.successResultOf(service.process(None,
            SOAP_ENV.Body(
                create_sms_delivery_receipt(
                    '1234',
                    '+27117654321',
                    DeliveryStatus.DeliveryUncertain))))

        self.assertEqual(1, len(self.callbacks))
        correlator, status = self.callbacks[0]
        self.assertEqual(('1234', 'pending'), self.callbacks[0])

    def test_render(self):
        """
        `SmsNotificationService.render_POST` parses a SOAP request and
        dispatches it to `SmsNotificationService.process` for processing.
        """
        service = SmsNotificationService(None, None)
        service.process = lambda *a, **kw: L.done()
        request = DummyRequest([])
        request.content = StringIO(tostring(soap_envelope('hello')))
        d = request.notifyFinish()
        service.render_POST(request)
        self.successResultOf(d)
        self.assertEqual(http.OK, request.responseCode)
        self.assertEqual(
            {str(SOAP_ENV.Envelope): {
                str(SOAP_ENV.Body): {
                    'done': None}}},
            element_to_dict(fromstring(''.join(request.written))))

    def test_render_soap_fault(self):
        """
        `SmsNotificationService.render_POST` logs any exceptions that occur
        during processing and writes a SOAP fault back to the request. If the
        logged exception is a `SoapFault` its ``to_element`` method is invoked
        to serialize the fault.
        """
        service = SmsNotificationService(None, None)
        service.process = lambda *a, **kw: L.done()
        request = DummyRequest([])
        request.content = StringIO(tostring(L.hello()))
        d = request.notifyFinish()

        service.render_POST(request)
        self.successResultOf(d)
        self.assertEqual(http.INTERNAL_SERVER_ERROR, request.responseCode)
        failures = self.flushLoggedErrors(SoapFault)
        self.assertEqual(1, len(failures))
        self.assertEqual(
            {str(SOAP_ENV.Envelope): {
                str(SOAP_ENV.Body): {
                    str(SOAP_ENV.Fault): {
                        'faultcode': 'soapenv:Client',
                        'faultstring': 'Malformed SOAP request'}}}},
            element_to_dict(fromstring(''.join(request.written))))

    def test_render_exceptions(self):
        """
        `SmsNotificationService.render_POST` logs any exceptions that occur
        during processing and writes a SOAP fault back to the request.
        """
        def process(*a, **kw):
            raise ValueError('What is this')
        service = SmsNotificationService(None, None)
        service.process = process
        request = DummyRequest([])
        request.content = StringIO(tostring(soap_envelope('hello')))
        d = request.notifyFinish()

        service.render_POST(request)
        self.successResultOf(d)
        self.assertEqual(http.INTERNAL_SERVER_ERROR, request.responseCode)
        failures = self.flushLoggedErrors(ValueError)
        self.assertEqual(1, len(failures))
        self.assertEqual(
            {str(SOAP_ENV.Envelope): {
                str(SOAP_ENV.Body): {
                    str(SOAP_ENV.Fault): {
                        'faultcode': 'soapenv:Server',
                        'faultstring': 'What is this'}}}},
            element_to_dict(fromstring(''.join(request.written))))

    def test_render_invalid_xml(self):
        """
        `SmsNotificationService.render_POST` does not accept invalid XML body
        content.
        """
        service = SmsNotificationService(None, None)
        request = DummyRequest([])
        request.content = StringIO('sup')
        d = request.notifyFinish()

        service.render_POST(request)
        self.successResultOf(d)
        self.assertEqual(http.INTERNAL_SERVER_ERROR, request.responseCode)
        failures = self.flushLoggedErrors(ParseError)
        self.assertEqual(1, len(failures))
