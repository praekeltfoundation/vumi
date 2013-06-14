from functools import partial

from twisted.internet.defer import succeed
from twisted.trial.unittest import TestCase
from twisted.web import http

from vumi.transports.parlayx.client import (
    PARLAYX_COMMON_NS, NOTIFICATION_MANAGER_NS, SEND_NS, format_address,
    ServiceExceptionDetail, ServiceException, PolicyExceptionDetail,
    PolicyException, ParlayXClient)
from vumi.transports.parlayx.soaputil import (
    perform_soap_request, unwrap_soap_envelope, soap_fault)
from vumi.transports.parlayx.xmlutil import (
    LocalNamespace as L, elemfind, fromstring, element_to_dict)
from vumi.transports.parlayx.tests.utils import (
    MockResponse, _FailureResultOfMixin)



class FormatAddressTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.client.format_address`.
    """
    def test_invalid(self):
        """
        `format_address` raises ``ValueError` for invalid MSISDNs.
        """
        self.assertRaises(ValueError, format_address, '12345')
        self.assertRaises(ValueError, format_address, 'nope')


    def test_format(self):
        """
        `format_address` formats MSISDNs in a way that ParlayX services will
        accept.
        """
        self.assertEqual(
            'tel:27117654321', format_address('+27117654321'))
        self.assertEqual(
            'tel:264117654321', format_address('+264117654321'))



class ServiceExceptionDetailTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.client.ServiceExceptionDetail`.
    """
    def test_unmatched(self):
        """
        `ServiceExceptionDetail.from_element` returns ``None`` if the element's
        tag is not a service exception detail.
        """
        elem = L.WhatIsThis(
            L.foo('a'),
            L.bar('b'))
        self.assertIdentical(None, ServiceExceptionDetail.from_element(elem))


    def test_from_element(self):
        """
        `ServiceExceptionDetail.from_element` returns
        a `ServiceExceptionDetail` instance by parsing
        a ``ServiceExceptionDetail`` detail element.
        """
        elem = PARLAYX_COMMON_NS.ServiceExceptionDetail(
            L.messageId('a'),
            L.text('b'),
            L.variables('c'),
            L.variables('d'))
        detail = ServiceExceptionDetail.from_element(elem)
        self.assertEqual(
            ('a', 'b', ['c', 'd']),
            (detail.message_id, detail.text, detail.variables))



class PolicyExceptionDetailTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.client.PolicyExceptionDetail`.
    """
    def test_unmatched(self):
        """
        `PolicyExceptionDetail.from_element` returns ``None`` if the element's
        tag is not a policy exception detail.
        """
        elem = L.WhatIsThis(
            L.foo('a'),
            L.bar('b'))
        self.assertIdentical(None, PolicyExceptionDetail.from_element(elem))


    def test_from_element(self):
        """
        `PolicyExceptionDetail.from_element` returns a `PolicyExceptionDetail`
        instance by parsing a ``PolicyExceptionDetail`` detail element.
        """
        elem = PARLAYX_COMMON_NS.PolicyExceptionDetail(
            L.messageId('a'),
            L.text('b'),
            L.variables('c'),
            L.variables('d'))
        detail = PolicyExceptionDetail.from_element(elem)
        self.assertEqual(
            ('a', 'b', ['c', 'd']),
            (detail.message_id, detail.text, detail.variables))



class ParlayXClientTests(TestCase, _FailureResultOfMixin):
    """
    Tests for `vumi.transports.parlayx.client.ParlayXClient`.
    """
    def setUp(self):
        self.requests = []


    def _http_request_full(self, response, uri, body, headers):
        """
        A mock for `vumi.utils.http_request_full`.

        Store an HTTP request's information and return a canned response.
        """
        self.requests.append((uri, body, headers))
        return succeed(response)


    def _perform_soap_request(self, response, *a, **kw):
        """
        Perform a SOAP request with a canned response.
        """
        return perform_soap_request(
            http_request_full=partial(
                self._http_request_full, response), *a, **kw)


    def _make_client(self, response=''):
        """
        Create a `ParlayXClient` instance that uses a stubbed
        `perform_soap_request` function.
        """
        return ParlayXClient(
            'short', 'endpoint', 'send', 'notification',
            perform_soap_request=partial(self._perform_soap_request, response))


    def test_start_sms_notification(self):
        """
        `ParlayXClient.start_sms_notification` performs a SOAP request to the
        remote ParlayX notification endpoint indicating where delivery and
        receipt notifications for a particular service activation number can be
        delivered.
        """
        client = self._make_client(
            MockResponse.build(
                http.OK, NOTIFICATION_MANAGER_NS.startSmsNotificationResponse))
        self.successResultOf(client.start_sms_notification())
        self.assertEqual(1, len(self.requests))
        self.assertEqual('notification', self.requests[0][0])
        body, header = unwrap_soap_envelope(fromstring(self.requests[0][1]))
        self.assertEqual(
            {str(NOTIFICATION_MANAGER_NS.startSmsNotification): {
                str(NOTIFICATION_MANAGER_NS.reference): {
                    'correlator': client._service_correlator,
                    'endpoint': 'endpoint',
                    'interfaceName': 'notifySmsReception'},
                str(NOTIFICATION_MANAGER_NS.smsServiceActivationNumber):
                    'short'}},
            element_to_dict(
                elemfind(body, NOTIFICATION_MANAGER_NS.startSmsNotification)))


    def test_start_sms_notification_service_fault(self):
        """
        `ParlayXClient.start_sms_notification` expects `ServiceExceptionDetail`
        fault details in SOAP requests that fail for remote service-related
        reasons.
        """
        detail = PARLAYX_COMMON_NS.ServiceExceptionDetail(
            L.messageId('a'),
            L.text('b'),
            L.variables('c'),
            L.variables('d'))
        client = self._make_client(
            MockResponse.build(
                http.INTERNAL_SERVER_ERROR,
                soap_fault('soapenv:Server', 'Whoops', detail=detail)))
        f = self.failureResultOf(
            client.start_sms_notification(), ServiceException)
        detail = f.value.parsed_detail
        self.assertEqual(
            ('a', 'b', ['c', 'd']),
            (detail.message_id, detail.text, detail.variables))


    def test_stop_sms_notification(self):
        """
        `ParlayXClient.stop_sms_notification` performs a SOAP request to the
        remote ParlayX notification endpoint indicating that delivery and
        receipt notifications for a particular service activation number can be
        deactivated.
        """
        client = self._make_client(
            MockResponse.build(
                http.OK, NOTIFICATION_MANAGER_NS.stopSmsNotificationResponse))
        self.successResultOf(client.stop_sms_notification())
        self.assertEqual(1, len(self.requests))
        self.assertEqual('notification', self.requests[0][0])
        body, header = unwrap_soap_envelope(fromstring(self.requests[0][1]))
        self.assertEqual(
            {str(NOTIFICATION_MANAGER_NS.stopSmsNotification): {
                'correlator': client._service_correlator}},
            element_to_dict(
                elemfind(body, NOTIFICATION_MANAGER_NS.stopSmsNotification)))


    def test_stop_sms_notification_service_fault(self):
        """
        `ParlayXClient.stop_sms_notification` expects `ServiceExceptionDetail`
        fault details in SOAP requests that fail for remote service-related
        reasons.
        """
        detail = PARLAYX_COMMON_NS.ServiceExceptionDetail(
            L.messageId('a'),
            L.text('b'),
            L.variables('c'),
            L.variables('d'))
        client = self._make_client(
            MockResponse.build(
                http.INTERNAL_SERVER_ERROR,
                soap_fault('soapenv:Server', 'Whoops', detail=detail)))
        f = self.failureResultOf(
            client.stop_sms_notification(), ServiceException)
        detail = f.value.parsed_detail
        self.assertEqual(
            ('a', 'b', ['c', 'd']),
            (detail.message_id, detail.text, detail.variables))


    def test_send_sms(self):
        """
        `ParlayXClient.send_sms` performs a SOAP request to the
        remote ParlayX send endpoint to deliver a message via SMS.
        """
        client = self._make_client(
            MockResponse.build(
                http.OK, SEND_NS.sendSmsResponse(SEND_NS.result('reference'))))
        response = self.successResultOf(
            client.send_sms('+27117654321', 'content', 'message_id'))
        self.assertEqual('reference', response)
        self.assertEqual(1, len(self.requests))
        self.assertEqual('send', self.requests[0][0])

        body, header = unwrap_soap_envelope(fromstring(self.requests[0][1]))
        self.assertEqual(
            {str(SEND_NS.sendSms): {
                str(SEND_NS.addresses): 'tel:27117654321',
                str(SEND_NS.message): 'content',
                str(SEND_NS.receiptRequest): {
                    'correlator': 'message_id',
                    'endpoint': 'endpoint',
                    'interfaceName': 'SmsNotification'}}},
            element_to_dict(elemfind(body, SEND_NS.sendSms)))


    def test_send_sms_service_fault(self):
        """
        `ParlayXClient.send_sms` expects `ServiceExceptionDetail` fault details
        in SOAP requests that fail for remote service-related reasons.
        """
        detail = PARLAYX_COMMON_NS.ServiceExceptionDetail(
            L.messageId('a'),
            L.text('b'),
            L.variables('c'),
            L.variables('d'))
        client = self._make_client(
            MockResponse.build(
                http.INTERNAL_SERVER_ERROR,
                soap_fault('soapenv:Server', 'Whoops', detail=detail)))
        f = self.failureResultOf(
            client.send_sms('+27117654321', 'content', 'message_id'),
            ServiceException)
        detail = f.value.parsed_detail
        self.assertEqual(
            ('a', 'b', ['c', 'd']),
            (detail.message_id, detail.text, detail.variables))


    def test_send_sms_policy_fault(self):
        """
        `ParlayXClient.send_sms` expects `PolicyExceptionDetail` fault details
        in SOAP requests that fail for remote policy-related reasons.
        """
        detail = PARLAYX_COMMON_NS.PolicyExceptionDetail(
            L.messageId('a'),
            L.text('b'),
            L.variables('c'),
            L.variables('d'))
        client = self._make_client(
            MockResponse.build(
                http.INTERNAL_SERVER_ERROR,
                soap_fault('soapenv:Server', 'Whoops', detail=detail)))
        f = self.failureResultOf(
            client.send_sms('+27117654321', 'content', 'message_id'),
            PolicyException)
        detail = f.value.parsed_detail
        self.assertEqual(
            ('a', 'b', ['c', 'd']),
            (detail.message_id, detail.text, detail.variables))
