from collections import namedtuple
from functools import partial

from twisted.internet.defer import succeed
from twisted.trial.unittest import TestCase
from twisted.web import http

from vumi.transports.parlayx.soaputil import (
    SOAP_ENV, soap_envelope, unwrap_soap_envelope, soap_fault, tostring,
    perform_soap_request, SoapFault)
from vumi.transports.parlayx.xmlutil import (
    ParseError, gettext, Element, LocalNamespace as L, element_to_dict)
from vumi.transports.parlayx.tests.utils import (
    MockResponse, _FailureResultOfMixin)


class SoapWrapperTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.soaputil.soap_envelope`,
    `vumi.transports.parlayx.soaputil.unwrap_soap_envelope` and
    `vumi.transports.parlayx.soaputil.soap_fault`.
    """
    def test_soap_envelope(self):
        """
        `soap_envelope` wraps content in a SOAP envelope element.
        """
        self.assertEqual(
            '<soapenv:Envelope xmlns:soapenv='
            '"http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body>'
            'hello</soapenv:Body></soapenv:Envelope>',
            tostring(soap_envelope('hello')))
        self.assertEqual(
            '<soapenv:Envelope xmlns:soapenv='
            '"http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body>'
            '<tag>hello</tag></soapenv:Body></soapenv:Envelope>',
            tostring(soap_envelope(Element('tag', 'hello'))))

    def test_unwrap_soap_envelope(self):
        """
        `unwrap_soap_envelope` unwraps a SOAP envelope element, with no header,
        to a tuple of the SOAP body element and ``None``.
        """
        body, header = unwrap_soap_envelope(
            soap_envelope(Element('tag', 'hello')))
        self.assertIdentical(None, header)
        self.assertEqual(
            '<soapenv:Body xmlns:soapenv='
            '"http://schemas.xmlsoap.org/soap/envelope/"><tag>hello</tag>'
            '</soapenv:Body>',
            tostring(body))

    def test_unwrap_soap_envelope_header(self):
        """
        `unwrap_soap_envelope` unwraps a SOAP envelope element, with a header,
        to a tuple of the SOAP body and header elements.
        """
        body, header = unwrap_soap_envelope(
            soap_envelope(
                Element('tag', 'hello'),
                Element('header', 'value')))
        self.assertEqual(
            '<soapenv:Header xmlns:soapenv='
            '"http://schemas.xmlsoap.org/soap/envelope/">'
            '<header>value</header></soapenv:Header>',
            tostring(header))
        self.assertEqual(
            '<soapenv:Body xmlns:soapenv='
            '"http://schemas.xmlsoap.org/soap/envelope/">'
            '<tag>hello</tag></soapenv:Body>',
            tostring(body))

    def test_soap_fault(self):
        """
        `soap_fault` constructs a SOAP fault element from a code and
        description.
        """
        self.assertEqual(
            '<soapenv:Fault xmlns:soapenv='
            '"http://schemas.xmlsoap.org/soap/envelope/">'
            '<faultcode>soapenv:Client</faultcode>'
            '<faultstring>Oops.</faultstring></soapenv:Fault>',
            tostring(soap_fault('soapenv:Client', 'Oops.')))


class ToyFaultDetail(namedtuple('ToyFaultDetail', ['foo', 'bar'])):
    """
    A SOAP fault detail used for tests.
    """
    @classmethod
    def from_element(cls, elem):
        if elem.tag != L.ToyFaultDetail.text:
            return None
        return cls(gettext(elem, 'foo'), gettext(elem, 'bar'))


class TestFault(SoapFault):
    """
    A SOAP fault used for tests.
    """
    detail_type = ToyFaultDetail


def _make_fault(*a, **kw):
    """
    Create a SOAP body containing a SOAP fault.
    """
    return SOAP_ENV.Body(soap_fault(*a, **kw))


class SoapFaultTests(TestCase):
    """
    Tests for `vumi.transports.parlayx.soaputil.SoapFault`.
    """
    def test_missing_fault(self):
        """
        `SoapFault.from_element` raises `ValueError` if the element contains no
        SOAP fault.
        """
        self.assertRaises(ValueError,
            SoapFault.from_element, Element('tag'))

    def test_from_element(self):
        """
        `SoapFault.from_element` creates a `SoapFault` instance from an
        ElementTree element and parses known SOAP fault details.
        """
        detail = L.ToyFaultDetail(
                L.foo('a'),
                L.bar('b'))

        fault = SoapFault.from_element(_make_fault(
            'soapenv:Client', 'message', 'actor', detail=detail))
        self.assertEqual(
            ('soapenv:Client', 'message', 'actor'),
            (fault.code, fault.string, fault.actor))
        self.assertIdentical(None, fault.parsed_detail)

    def test_to_element(self):
        """
        `SoapFault.to_element` serializes the fault to a SOAP ``Fault``
        ElementTree element.
        """
        detail = L.ToyFaultDetail(
                L.foo('a'),
                L.bar('b'))

        fault = SoapFault.from_element(_make_fault(
            'soapenv:Client', 'message', 'actor', detail=detail))
        self.assertEqual(
            {str(SOAP_ENV.Fault): {
                'faultcode': fault.code,
                'faultstring': fault.string,
                'faultactor': fault.actor,
                'detail': {
                    'ToyFaultDetail': {'foo': 'a',
                                        'bar': 'b'}}}},
            element_to_dict(fault.to_element()))

    def test_to_element_no_detail(self):
        """
        `SoapFault.to_element` serializes the fault to a SOAP ``Fault``
        ElementTree element, omitting the ``detail`` element if
        `SoapFault.detail` is None.
        """
        fault = SoapFault.from_element(_make_fault(
            'soapenv:Client', 'message', 'actor'))
        self.assertEqual(
            {str(SOAP_ENV.Fault): {
                'faultcode': fault.code,
                'faultstring': fault.string,
                'faultactor': fault.actor}},
            element_to_dict(fault.to_element()))

    def test_expected_faults(self):
        """
        `SoapFault.from_element` creates an instance of a specified `SoapFault`
        subclass if a fault detail of a recognised type occurs.
        """
        detail = [
            L.WhatIsThis(
                L.foo('a'),
                L.bar('b')),
            L.ToyFaultDetail(
                L.foo('c'),
                L.bar('d'))]

        fault = SoapFault.from_element(_make_fault(
            'soapenv:Client', 'message', 'actor', detail=detail),
            expected_faults=[TestFault])
        self.assertEqual(
            ('soapenv:Client', 'message', 'actor'),
            (fault.code, fault.string, fault.actor))
        parsed_detail = fault.parsed_detail
        self.assertEqual(
            ('c', 'd'),
            (parsed_detail.foo, parsed_detail.bar))


class PerformSoapRequestTests(_FailureResultOfMixin, TestCase):
    """
    Tests for `vumi.transports.parlayx.soaputil.perform_soap_request`.
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

    def test_success(self):
        """
        `perform_soap_request` issues a SOAP request, over HTTP, to a URI, sets
        the ``SOAPAction`` header and parses the response as a SOAP envelope.
        """
        response = MockResponse.build(http.OK, 'response', 'response_header')
        body, header = self.successResultOf(
            self._perform_soap_request(response, 'uri', 'action', 'request'))
        self.assertEqual([
            ('uri',
             tostring(soap_envelope('request')),
             {'SOAPAction': 'action',
              'Content-Type': 'text/xml; charset="utf-8"'})],
            self.requests)
        self.assertEqual(SOAP_ENV.Body.text, body.tag)
        self.assertEqual('response', body.text)
        self.assertEqual(SOAP_ENV.Header.text, header.tag)
        self.assertEqual('response_header', header.text)

    def test_response_not_xml(self):
        """
        `perform_soap_request` raises `xml.etree.ElementTree.ParseError` if the
        response is not valid XML.
        """
        response = MockResponse(http.OK, 'hello')
        self.failureResultOf(
            self._perform_soap_request(response, 'uri', 'action', 'request'),
            ParseError)

    def test_response_no_body(self):
        """
        `perform_soap_request` raises `SoapFault` if the response contains no
        SOAP body element..
        """
        response = MockResponse(http.OK, tostring(SOAP_ENV.Envelope('hello')))
        f = self.failureResultOf(
            self._perform_soap_request(response, 'uri', 'action', 'request'),
            SoapFault)
        self.assertEqual('soapenv:Client', f.value.code)
        self.assertEqual('Malformed SOAP request', f.getErrorMessage())

    def test_fault(self):
        """
        `perform_soap_request` raises `SoapFault`, parsed from the ``Fault``
        element in the response, if the response HTTP status is ``500 Internal
        server error``.
        """
        response = MockResponse.build(
            http.INTERNAL_SERVER_ERROR, soap_fault('soapenv:Server', 'Whoops'))
        f = self.failureResultOf(
            self._perform_soap_request(response, 'uri', 'action', 'request'),
            SoapFault)
        self.assertEqual(
            ('soapenv:Server', 'Whoops'),
            (f.value.code, f.getErrorMessage()))

    def test_expected_fault(self):
        """
        `perform_soap_request` raises a `SoapFault` subclass when a SOAP fault
        detail matches one of the expected fault types.
        """
        detail = L.ToyFaultDetail(
                L.foo('a'),
                L.bar('b'))
        response = MockResponse.build(
            http.INTERNAL_SERVER_ERROR,
            soap_fault('soapenv:Server', 'Whoops', detail=detail))
        f = self.failureResultOf(
            self._perform_soap_request(
                response, 'uri', 'action', 'request',
                expected_faults=[TestFault]),
            TestFault)
        self.assertEqual(
            ('soapenv:Server', 'Whoops'),
            (f.value.code, f.getErrorMessage()))
        parsed_detail = f.value.parsed_detail
        self.assertEqual(
            ('a', 'b'),
            (parsed_detail.foo, parsed_detail.bar))
