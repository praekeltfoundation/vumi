# -*- test-case-name: vumi.transports.wechat.tests.test_wechat -*-

import hashlib
import urllib
import json
from datetime import datetime
from functools import partial

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue
from twisted.web.resource import Resource
from twisted.web import http
from twisted.web.server import NOT_DONE_YET

from vumi import log
from vumi.config import (
    ConfigText, ConfigServerEndpoint, ConfigDict, ConfigInt)
from vumi.transports import Transport
from vumi.transports.httprpc.httprpc import HttpRpcHealthResource
from vumi.transports.wechat.errors import WeChatException, WeChatApiException
from vumi.transports.wechat.message_types import (
    TextMessage, EventMessage, NewsMessage, WeChatXMLParser)
from vumi.utils import build_web_site, http_request_full
from vumi.message import TransportUserMessage
from vumi.persist.txredis_manager import TxRedisManager


def is_verifiable(request):
    params = ['signature', 'timestamp', 'nonce']
    return all([(key in request.args) for key in params])


def http_ok(request):
    return 200 <= request.code < 300


def verify(token, request):
    signature = request.args['signature'][0]
    timestamp = request.args['timestamp'][0]
    nonce = request.args['nonce'][0]

    hash_ = hashlib.sha1(''.join(sorted([timestamp, nonce, token])))

    return hash_.hexdigest() == signature


class WeChatConfig(Transport.CONFIG_CLASS):

    api_url = ConfigText(
        'The URL the WeChat API is accessible at.',
        default='https://api.wechat.com/cgi-bin/',
        required=False, static=True)
    auth_token = ConfigText(
        'This WeChat app\'s auth token. '
        'Used for initial message authentication.',
        required=True, static=True)
    twisted_endpoint = ConfigServerEndpoint(
        'The endpoint to listen on.',
        required=True, static=True)
    web_path = ConfigText(
        "The path to serve this resource on.",
        default='/api/v1/wechat/', static=True)
    health_path = ConfigText(
        "The path to serve the health resource on.",
        default='/health/', static=True)
    redis_manager = ConfigDict('Parameters to connect to Redis with.',
                               default={}, required=False, static=True)
    wechat_appid = ConfigText(
        'The WeChat app_id. Issued by WeChat for developer accounts '
        'to allow push API access.', required=True, static=True)
    wechat_secret = ConfigText(
        'The WeChat secret. Issued by WeChat for developer accounts '
        'to allow push API access.', required=True, static=True)
    wechat_menu = ConfigDict(
        'The menu structure to create at boot.', required=False, static=True)
    wechat_mask_lifetime = ConfigInt(
        'How long, in seconds, to maintain an address mask for. '
        '(default 1 hour)', default=60 * 60 * 1, static=True)


class WeChatResource(Resource):

    isLeaf = True

    def __init__(self, transport):
        Resource.__init__(self)
        self.transport = transport
        self.config = transport.get_static_config()

    def render_GET(self, request):
        if is_verifiable(request) and verify(self.config.auth_token, request):
            return request.args['echostr'][0]
        request.setResponseCode(http.BAD_REQUEST)
        return ''

    def render_POST(self, request):
        if not (is_verifiable(request)
                and verify(self.config.auth_token, request)):
            request.setResponseCode(http.BAD_REQUEST)
            return ''

        d = Deferred()
        d.addCallback(self.handle_request)
        d.addCallback(self.transport.queue_request, request)
        d.addErrback(self.handle_error, request)
        reactor.callLater(0, d.callback, request)
        return NOT_DONE_YET

    def handle_error(self, failure, request):
        if not failure.trap(WeChatException):
            raise failure

        request.setResponseCode(http.BAD_REQUEST)
        request.write(failure.getErrorMessage())
        request.finish()

    def handle_request(self, request):
        wc_msg = WeChatXMLParser.parse(request.content.read())
        return self.transport.handle_raw_inbound_message(wc_msg)


class WeChatTransport(Transport):
    """

    A Transport for the WeChat API.

    API documentation
    ~~~~~~~~~~~~~~~~~

    http://admin.wechat.com/wiki/index.php?title=Main_Page


    Inbound Messaging
    ~~~~~~~~~~~~~~~~~

    Supported Common Message types:

        - Text Message

    Supported Event Message types:

        - Following / subscribe
        - Unfollowing / unsubscribe
        - Text Message (in response to Menu keypress events)


    Outbound Messaging
    ~~~~~~~~~~~~~~~~~~

    Supported Callback Message types:

        - Text Message
        - News Message

    Supported Customer Service Message types:

        - Text Message
        - News Message

    """

    CONFIG_CLASS = WeChatConfig
    DEFAULT_MASK = 'default'
    MESSAGE_TYPES = [
        NewsMessage,
    ]
    DEFAULT_MESSAGE_TYPE = TextMessage
    # What key to store the `access_token` under in Redis
    ACCESS_TOKEN_KEY = 'access_token'
    # What key to store the `addr_mask` under in Redis
    ADDR_MASK_KEY = 'addr_mask'

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        self.request_queue = {}
        self.endpoint = config.twisted_endpoint
        self.resource = WeChatResource(self)
        self.factory = build_web_site({
            config.health_path: HttpRpcHealthResource(self),
            config.web_path: self.resource,
        })

        self.redis = yield TxRedisManager.from_config(config.redis_manager)
        self.server = yield self.endpoint.listen(self.factory)

        if config.wechat_menu:
            # not yielding because this shouldn't block startup
            d = self.get_access_token()
            d.addCallback(self.create_wechat_menu, config.wechat_menu)

    @inlineCallbacks
    def create_wechat_menu(self, access_token, menu_structure):
        url = self.make_url('menu/create', {'access_token': access_token})
        response = yield http_request_full(
            url, method='POST', data=json.dumps(menu_structure),
            headers={'Content-Type': ['application/json']})
        if not http_ok(response):
            raise WeChatApiException(
                'Received HTTP code: %r when creating the menu.' % (
                    response.code,))
        data = json.loads(response.delivered_body)
        if data['errcode'] != 0:
            raise WeChatApiException(
                'Received errcode: %(errcode)s, errmsg: %(errmsg)s '
                'when creating WeChat Menu.' % data)
        log.info('WeChat Menu created succesfully.')

    def mask_addr(self, to_addr, mask):
        return '@'.join([to_addr, mask])

    def cache_addr_mask(self, mask):
        config = self.get_static_config()
        d = self.redis.setex(
            self.ADDR_MASK_KEY, config.wechat_mask_lifetime, mask)
        d.addCallback(lambda *a: mask)
        return d

    def get_addr_mask(self):
        d = self.redis.get(self.ADDR_MASK_KEY)
        d.addCallback(lambda mask: mask or self.DEFAULT_MASK)
        return d

    def clear_addr_mask(self):
        return self.redis.delete(self.ADDR_MASK_KEY)

    def handle_raw_inbound_message(self, wc_msg):
        return {
            TextMessage: self.handle_inbound_text_message,
            EventMessage: self.handle_inbound_event_message,
        }.get(wc_msg.__class__)(wc_msg)

    @inlineCallbacks
    def handle_inbound_text_message(self, wc_msg):
        mask = yield self.get_addr_mask()
        msg = yield self.publish_message(
            content=wc_msg.content,
            from_addr=wc_msg.from_user_name,
            to_addr=self.mask_addr(wc_msg.to_user_name, mask),
            timestamp=datetime.fromtimestamp(int(wc_msg.create_time)),
            transport_type='wechat',
            transport_metadata={
                'wechat': {
                    'FromUserName': wc_msg.from_user_name,
                    'ToUserName': wc_msg.to_user_name,
                    'MsgType': 'text',
                    'MsgId': wc_msg.msg_id,
                }
            })
        returnValue(msg)

    @inlineCallbacks
    def handle_inbound_event_message(self, wc_msg):
        if wc_msg.event_key:
            mask = yield self.cache_addr_mask(wc_msg.event_key)
        else:
            mask = yield self.get_addr_mask()

        if wc_msg.event.lower() in ('subscribe', 'click'):
            session_event = TransportUserMessage.SESSION_NEW
        elif wc_msg.event.lower() == 'unsubscribe':
            session_event = TransportUserMessage.SESSION_CLOSE
        else:
            session_event = TransportUserMessage.SESSION_NONE

        msg = yield self.publish_message(
            content=None,
            from_addr=wc_msg.from_user_name,
            to_addr=self.mask_addr(wc_msg.to_user_name, mask),
            timestamp=datetime.fromtimestamp(int(wc_msg.create_time)),
            session_event=session_event,
            transport_type='wechat',
            transport_metadata={
                'wechat': {
                    'FromUserName': wc_msg.from_user_name,
                    'ToUserName': wc_msg.to_user_name,
                    'MsgType': 'event',
                    'Event': wc_msg.event,
                    'EventKey': wc_msg.event_key
                }
            })
        returnValue(msg)

    def force_close(self, message):
        request = self.pop_request(message['message_id'])
        request.setResponseCode(http.INTERNAL_SERVER_ERROR)
        request.finish()

    def queue_request(self, message, request):
        self.request_queue[message['message_id']] = request

    def pop_request(self, message_id):
        return self.request_queue.pop(message_id, None)

    def infer_message_type(self, message):
        for message_type in self.MESSAGE_TYPES:
            result = message_type.accepts(message)
            if result is not None:
                return partial(message_type.from_vumi_message, result)
        return self.DEFAULT_MESSAGE_TYPE.from_vumi_message

    def handle_outbound_message(self, message):
        """
        Read outbound message and do what needs to be done with them.
        """
        request_id = message['in_reply_to']
        request = self.pop_request(request_id)

        builder = self.infer_message_type(message)
        wc_msg = builder(message)

        if request is None:
            # There's no pending request object for this message which
            # means we need to treat this as a customer service message
            # and hit WeChat's Push API (window available for 24hrs)
            return self.push_message(wc_msg, message)

        request.write(wc_msg.to_xml())
        request.finish()

        d = self.publish_ack(user_message_id=message['message_id'],
                             sent_message_id=message['message_id'])
        if message['session_event'] == TransportUserMessage.SESSION_CLOSE:
            d.addCallback(lambda ack: self.clear_addr_mask())
        return d

    def push_message(self, wc_message, vumi_message):
        d = self.get_access_token()
        d.addCallback(
            lambda access_token: self.make_url('message/custom/send', {
                'access_token': access_token
            }))
        d.addCallback(
            lambda url: http_request_full(
                url, method='POST', data=wc_message.to_json(), headers={
                    'Content-Type': ['application/json']
                }))
        d.addCallback(self.handle_api_response, vumi_message)
        if vumi_message['session_event'] == TransportUserMessage.SESSION_CLOSE:
            d.addCallback(lambda ack: self.clear_addr_mask())
        return d

    def handle_api_response(self, response, message):
        if http_ok(response):
            return self.publish_ack(user_message_id=message['message_id'],
                                    sent_message_id=message['message_id'])
        return self.publish_nack(
            message['message_id'],
            reason='Received status code: %s' % (response.code,))

    @inlineCallbacks
    def get_access_token(self):
        access_token = yield self.redis.get(self.ACCESS_TOKEN_KEY)
        if access_token is None:
            access_token = yield self.request_new_access_token()
        returnValue(access_token)

    @inlineCallbacks
    def request_new_access_token(self):
        config = self.get_static_config()
        response = yield http_request_full(self.make_url('token', {
            'grant_type': 'client_credential',
            'appid': config.wechat_appid,
            'secret': config.wechat_secret,
        }), method='GET')
        if not http_ok(response):
            raise WeChatApiException(
                ('Received HTTP status code %r when '
                 'requesting access token.') % (response.code,))

        data = json.loads(response.delivered_body)
        if 'errcode' in data:
            raise WeChatApiException(
                'Error when requesting access token. '
                'Errcode: %(errcode)s, Errmsg: %(errmsg)s.' % data)

        # make sure we're always ahead of the WeChat expiry
        access_token = data['access_token']
        expiry = int(data['expires_in']) * 0.90
        yield self.redis.setex(
            self.ACCESS_TOKEN_KEY, int(expiry), access_token)
        returnValue(access_token)

    def make_url(self, path, params):
        config = self.get_static_config()
        return '%s%s?%s' % (
            config.api_url, path, urllib.urlencode(params))

    def teardown_transport(self):
        return self.server.stopListening()

    def get_health_response(self):
        return "OK"
