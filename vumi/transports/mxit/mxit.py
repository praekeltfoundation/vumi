# -*- test-case-name: vumi.transports.mxit.tests.test_mxit -*-
import json
import base64
from urllib import urlencode, unquote_plus
from HTMLParser import HTMLParser

from twisted.web import http
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.config import ConfigText, ConfigInt, ConfigDict, ConfigList
from vumi.persist.txredis_manager import TxRedisManager
from vumi.transports.httprpc import HttpRpcTransport
from vumi.transports.mxit.responses import MxitResponse
from vumi.utils import http_request_full


class MxitTransportException(Exception):
    """Raised when the Mxit API returns an error"""


class MxitTransportConfig(HttpRpcTransport.CONFIG_CLASS):

    client_id = ConfigText(
        'The OAuth2 ClientID assigned to this transport.', required=True,
        static=True)
    client_secret = ConfigText(
        'The OAuth2 ClientSecret assigned to this transport.', required=True,
        static=True)
    timeout = ConfigInt(
        'Timeout for outbound Mxit HTTP API calls.', required=False,
        default=30, static=True)
    redis_manager = ConfigDict(
        'How to connect to Redis', required=True, static=True)
    api_send_url = ConfigText(
        'The URL for the Mxit message sending API.',
        required=False, default="https://api.mxit.com/message/send/",
        static=True)
    api_auth_url = ConfigText(
        'The URL for the Mxit authentication API.',
        required=False, default='https://auth.mxit.com',
        static=True)
    api_auth_scopes = ConfigList(
        'The list of scopes to request access to.',
        required=False, static=True, default=['message/send'])


class MxitTransport(HttpRpcTransport):
    """
    HTTP Transport for MXit, implemented using the MXit Mobi Portal
    (for inbound messages and replies) and the Messaging API (for sends
    that aren't replies).

    * Mobi Portal API specification:
      http://dev.mxit.com/docs/mobi-portal-api
    * Message API specification:
      https://dev.mxit.com/docs/restapi/messaging/post-message-send
    """

    CONFIG_CLASS = MxitTransportConfig
    content_type = 'text/html; charset=utf-8'
    transport_type = 'mxit'
    access_token_key = 'access_token'
    access_token_auto_decay = 0.95

    @inlineCallbacks
    def setup_transport(self):
        yield super(MxitTransport, self).setup_transport()
        config = self.get_static_config()
        self.redis = yield TxRedisManager.from_config(config.redis_manager)

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
        headers = request.requestHeaders
        [content] = headers.getRawHeaders('X-Mxit-User-Input', [None])
        if content:
            return unquote_plus(content)

        if request.args and 'input' in request.args:
            [content] = request.args['input']
            return content

        return None

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

    def handle_outbound_message(self, message):
        self.emit("MxitTransport consuming %s" % (message))
        if message["in_reply_to"] is None:
            return self.handle_outbound_send(message)
        else:
            return self.handle_outbound_reply(message)

    @inlineCallbacks
    def handle_outbound_reply(self, message):
        missing_fields = self.ensure_message_values(
            message, ['in_reply_to'])
        if missing_fields:
            yield self.reject_message(message, missing_fields)
        else:
            yield self.render_response(message)
            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'])

    @inlineCallbacks
    def get_access_token(self):
        access_token = yield self.redis.get(self.access_token_key)
        if access_token is None:
            access_token, expiry = yield self.request_new_access_token()
            # always make sure we expire before the token actually does
            safe_expiry = expiry * self.access_token_auto_decay
            yield self.redis.setex(
                self.access_token_key, int(safe_expiry), access_token)
        returnValue(access_token)

    @inlineCallbacks
    def request_new_access_token(self):
        config = self.get_static_config()
        url = '%s/token' % (config.api_auth_url)
        auth = base64.b64encode(
            '%s:%s' % (config.client_id, config.client_secret))
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic %s' % (auth,)
        }
        data = urlencode({
            'grant_type': 'client_credentials',
            'scope': ' '.join(config.api_auth_scopes)
        })
        response = yield http_request_full(url=url, method='POST',
                                           headers=headers, data=data)
        data = json.loads(response.delivered_body)
        if 'error' in data:
            raise MxitTransportException(
                '%(error)s: %(error_description)s.' % data)

        returnValue(
            (data['access_token'].encode('utf8'), int(data['expires_in'])))

    @inlineCallbacks
    def handle_outbound_send(self, message):
        config = self.get_static_config()
        body = message['content']
        access_token = yield self.get_access_token()
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer %s" % (access_token,)
        }
        data = {
            "Body": body,
            "ContainsMarkup": "true",
            "From": message["from_addr"],
            "To": message["to_addr"],
            "Spool": "true",
        }
        context_factory = None
        resp = yield http_request_full(
            config.api_send_url, data=json.dumps(data), headers=headers,
            method="POST", timeout=config.timeout,
            context_factory=context_factory)

    @inlineCallbacks
    def render_response(self, message):
        msg_id = message['in_reply_to']
        request = self.get_request(msg_id)
        if request:
            data = yield MxitResponse(message).flatten()
            super(MxitTransport, self).finish_request(
                msg_id, data, code=http.OK)
