# -*- test-case-name: vumi.transports.parlayx.tests.test_soaputil -*-
"""
Utility functions for performing and processing SOAP requests, constructing
SOAP responses and parsing and constructing SOAP faults.
"""
from twisted.web import http
from twisted.python import log

from vumi.utils import http_request_full
from vumi.transports.parlayx.xmlutil import (
    Namespace, LocalNamespace, elemfind, gettext, fromstring, tostring)


SOAP_ENV = Namespace('http://schemas.xmlsoap.org/soap/envelope/', 'soapenv')


def perform_soap_request(uri, action, body, header=None,
                         expected_faults=None,
                         http_request_full=http_request_full):
    """
    Perform a SOAP request.

    If the remote server responds with an HTTP 500 status, then it is assumed
    that the body contains a SOAP fault, which is then parsed and a `SoapFault`
    exception raised.

    :param uri: SOAP endpoint URI.
    :param action: SOAP action.
    :param body:
        SOAP body that will appear in an envelope ``Body`` element.
    :param header:
        SOAP header that will appear in an envelope ``Header`` element, or
        ``None`` so omit the header.
    :param expected_faults:
        A `list` of `SoapFault` subclasses to be used to extract fault details
        from SOAP faults.
    :param http_request_full:
        Callable to perform an HTTP request, see
        `vumi.utils.http_request_full`.
    :return:
        `Deferred` that fires with the response, in the case of success, or
        a `SoapFault` in the case of failure.
    """
    def _parse_soap_response(response):
        root = fromstring(response.delivered_body)
        body, header = unwrap_soap_envelope(root)
        if response.code == http.INTERNAL_SERVER_ERROR:
            raise SoapFault.from_element(body, expected_faults)
        return body, header

    envelope = soap_envelope(body, header)
    headers = {
        'SOAPAction': action,
        'Content-Type': 'text/xml; charset="utf-8"'}
    d = http_request_full(uri, tostring(envelope), headers)
    d.addCallback(_parse_soap_response)
    return d


def soap_envelope(body, header=None):
    """
    Wrap an element or text in a SOAP envelope.
    """
    parts = [SOAP_ENV.Body(body)]
    if header is not None:
        parts.insert(0, SOAP_ENV.Header(header))
    return SOAP_ENV.Envelope(*parts)


def unwrap_soap_envelope(root):
    """
    Unwrap a SOAP request and return the SOAP header and body elements.
    """
    header = elemfind(root, SOAP_ENV.Header)
    body = elemfind(root, SOAP_ENV.Body)
    if body is None:
        raise SoapFault(u'soapenv:Client', u'Malformed SOAP request')
    return body, header


def soap_fault(faultcode, faultstring=None, faultactor=None, detail=None):
    """
    Create a SOAP fault response.
    """
    def _maybe(f, value):
        if value is not None:
            return f(value)
        return None

    xs = [
        LocalNamespace.faultcode(faultcode),
        _maybe(LocalNamespace.faultstring, faultstring),
        _maybe(LocalNamespace.faultactor, faultactor),
        _maybe(LocalNamespace.detail, detail)]
    # filter(None, xs) doesn't do what we want because of weird implicit
    # truthiness with ElementTree elements.
    return SOAP_ENV.Fault(*[x for x in xs if x is not None])


def _parse_expected_faults(detail, expected_faults):
    """
    Parse expected SOAP faults from a SOAP fault ``detail`` element.

    :param detail:
        ElementTree element containing SOAP fault detail elements that will
        attempt to be matched against the expected SOAP faults.
    :param expected_faults:
        A `list` of `SoapFault` subclasses whose ``detail_type`` attribute will
        be used to determine a match against each top-level SOAP fault detail
        element.
    :return:
        A 2-tuple of the matching exception type and an instance of it's
        ``detail_type``, or ``None`` if there were no matches.
    """
    if detail is None:
        return None

    for child in detail.getchildren():
        for exc_type in expected_faults:
            try:
                if exc_type.detail_type is not None:
                    det = exc_type.detail_type.from_element(child)
                    if det is not None:
                        return exc_type, det
            except:
                log.err(
                    None, 'Error parsing SOAP fault element (%r) with %r' % (
                        child, exc_type.detail_type))

    return None


def parse_soap_fault(body, expected_faults=None):
    """
    Parse a SOAP fault element and its details.

    :param body: SOAP ``Body`` element.
    :param expected_faults:
        A `list` of `SoapFault` subclasses whose ``detail_type`` attribute will
        be used to determine a match against each top-level SOAP fault detail
        element.
    :return:
        A 2-tuple of: matching exception type and an instance of it's
        ``detail_type``; and SOAP fault information (code, string, actor,
        detail). ``None`` if there is no SOAP fault.
    """
    fault = elemfind(body, SOAP_ENV.Fault)
    if fault is None:
        return None
    faultcode = gettext(fault, u'faultcode')
    faultstring = gettext(fault, u'faultstring')
    faultactor = gettext(fault, u'faultactor')
    detail = elemfind(fault, u'detail')

    if expected_faults is None:
        expected_faults = []
    parsed = _parse_expected_faults(detail, expected_faults)
    return parsed, (faultcode, faultstring, faultactor, detail)


class SoapFault(RuntimeError):
    """
    An exception that constitutes a SOAP fault.

    :cvar detail_type:
        A type with a ``from_element`` callable that takes a top-level detail
        element and attempts to parse it, returning an instance of itself if
        successful or ``None``.

    :ivar code: SOAP fault code.
    :ivar string: SOAP fault string.
    :ivar actor: SOAP fault actor.
    :ivar detail: SOAP fault detail ElementTree element.
    :ivar parsed_detail: ``detail_type`` instance, or ``None``.
    """
    detail_type = None

    def __init__(self, code, string, actor=None, detail=None,
                 parsed_detail=None):
        self.code = code
        self.string = string
        self.actor = actor
        self.detail = detail
        self.parsed_detail = parsed_detail
        RuntimeError.__init__(self, string)

    def __repr__(self):
        return '<%s code=%r string=%r actor=%r parsed_detail=%r>' % (
            type(self).__name__,
            self.code,
            self.string,
            self.actor,
            self.parsed_detail)

    @classmethod
    def from_element(cls, root, expected_faults=None):
        """
        Parse a SOAP fault from an ElementTree element.

        :param expected_faults:
            A `list` of `SoapFault` subclasses whose ``detail_type`` attribute
            will be used to determine a match against each top-level SOAP fault
            detail element.
        :return: A `SoapFault` subclass.
        """
        faultinfo = parse_soap_fault(root, expected_faults)
        if faultinfo is None:
            raise ValueError(
                'Element (%r) does not contain a SOAP fault' % (root,))

        parsed_fault, faultinfo = faultinfo
        if parsed_fault is None:
            parsed_fault = SoapFault, None
        exc_type, parsed_detail = parsed_fault

        faultcode, faultstring, faultactor, detail = faultinfo
        return exc_type(
            faultcode, faultstring, faultactor, detail, parsed_detail)

    def to_element(self):
        """
        Serialize this SOAP fault to an ElementTree element.
        """
        detail = self.detail
        if detail is not None:
            detail = self.detail.getchildren()
        return soap_fault(
            self.code, self.string, self.actor, detail)


__all__ = [
    'perform_soap_request', 'soap_envelope', 'unwrap_soap_envelope',
    'soap_fault', 'parse_soap_fault', 'SoapFault']
