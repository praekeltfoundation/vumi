# -*- test-case-name: vumi.transports.mxit.tests.test_mxit -*-
from HTMLParser import HTMLParser

from twisted.web import http
from twisted.internet.defer import inlineCallbacks
from twisted.web.template import flattenString

from vumi.transports.httprpc import HttpRpcTransport
from vumi.transports.mxit.responses import MxitResponse


class MxitTransport(HttpRpcTransport):
    """
    HTTP Transport for MXit, implemented using the MXit Mobi Portal
    API Specification, see: http://dev.mxit.com/docs/mobi-portal-api.
    """

    content_type = 'text/html; charset=utf-8'
    transport_type = 'mxit'

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
        """
        Turns '&lt;b&gt;foo&lt;/b&gt;' into u'<b>foo</b>'
        """
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

    def get_request_content(self, request):
        if request.args and 'input' in request.args:
            content = request.args['input']
        else:
            headers = request.requestHeaders
            content = headers.getRawHeaders('X-Mxit-User-Input', None)
        return content[0]

    def handle_raw_inbound_message(self, msg_id, request):
        if not self.is_mxit_request(request):
            return self.finish_request(
                msg_id, data=http.RESPONSES[http.BAD_REQUEST],
                code=http.BAD_REQUEST)

        data = self.get_request_data(request)
        content = self.get_request_content(request)
        return self.publish_message(
            message_id=msg_id,
            content=content,
            to_addr=data['X-Mxit-Contact'],
            from_addr=data['X-Mxit-USERID-R'],
            provider='mxit',
            transport_type=self.transport_type,
            helper_metadata={
                'mxit_info': data,
            })

    @inlineCallbacks
    def handle_outbound_message(self, message):
        self.emit("MxitTransport consuming %s" % (message))
        missing_fields = self.ensure_message_values(
            message, ['in_reply_to', 'content'])
        if missing_fields:
            yield self.reject_message(message, missing_fields)
        else:
            yield self.render_response(message)
            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'])

    @inlineCallbacks
    def render_response(self, message):
        msg_id = message['in_reply_to']
        request = self.get_request(msg_id)
        if request:
            data = yield flattenString(None, MxitResponse(message))
            super(MxitTransport, self).finish_request(
                msg_id, data, code=http.OK)
