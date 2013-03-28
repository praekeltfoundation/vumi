# -*- test-case-name: vumi.transports.mxit.tests.test_mxit -*-
from HTMLParser import HTMLParser

from vumi.transports.httprpc import HttpRpcTransport


class MxitTransport(HttpRpcTransport):
    """
    HTTP Transport for MXit, implemented using the MXit Mobi Portal
    API Specification, see: http://dev.mxit.com/docs/mobi-portal-api.
    """

    content_type = 'text/html; charset=utf-8'

    def is_mxit_request(self, request):
        return request.requestHeaders.hasHeader('X-Mxit-Contact')

    def noop(self, key):
        return key

    def parse_location(self, location):
        return dict(zip([
            'country_code',
            'country_name',
            'subdivision_code',
            'subdivision_name',
            'city_code',
            'city',
            'network_operator_id',
            'client_features_bitset',
            'cell_id'
        ], location.split(',')))

    def parse_profile(self, profile):
        return dict(zip([
            'language_code',
            'country_code',
            'date_of_birth',
            'gender',
            'tariff_plan',
        ], profile.split(',')))

    def html_decode(self, html):
        return HTMLParser().unescape(html)

    def get_request_data(self, request):
        headers = request.requestHeaders
        header_ops = [
            ('X-Device-User-Agent', self.noop),
            ('X-Mxit-Contact', self.noop),
            ('X-Mxit-USERID-R', self.noop),
            ('X-Mxit-Nick', self.noop),
            ('X-Mxit-Location', self.parse_location),
            ('X-Mxit-Profile', self.parse_profile),
            ('X-Mxit-User-Input', self.html_decode),
        ]
        data = {}
        for header, proc in header_ops:
            if headers.hasHeader(header):
                [value] = headers.getRawHeaders(header)
                data[header] = proc(value)
        return data

    def handle_raw_inbound_message(self, msgid, request):
        data = self.get_request_data(data)
