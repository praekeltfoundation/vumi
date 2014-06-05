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
    ConfigText, ConfigServerEndpoint, ConfigDict, ConfigInt, ConfigBool,
    ServerEndpointFallback)
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
        required=True, static=True, fallbacks=[ServerEndpointFallback()])
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
    embed_user_profile = ConfigBool(
        'Whether or not to embed the WeChat User Profile info in '
        'messages received.', required=True, default=False, static=True)
    embed_user_profile_lang = ConfigText(
        'What language to request User Profile as.', required=False,
        default='en', static=True)
    embed_user_profile_lifetime = ConfigInt(
        'How long to cache User Profiles for.', default=(60 * 60),
        required=False, static=True)
    double_delivery_lifetime = ConfigInt(
        'How long to keep track of Message IDs and responses for double '
        'delivery tracking.', default=(60 * 60), required=False, static=True)

    # TODO: Deprecate these fields when confmodel#5 is done.
    host = ConfigText(
        "*DEPRECATED* 'host' and 'port' fields may be used in place of the"
        " 'twisted_endpoint' field.", static=True)
    port = ConfigInt(
        "*DEPRECATED* 'host' and 'port' fields may be used in place of the"
        " 'twisted_endpoint' field.", static=True)


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
        d = request.notifyFinish()
        d.addBoth(
            lambda _: self.transport.handle_finished_request(request))

        wc_msg = WeChatXMLParser.parse(request.content.read())
        return self.transport.handle_raw_inbound_message(request, wc_msg)


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
    # What key to use when constructing the User Profile key
    USER_PROFILE_KEY = 'user_profile'
    # What key to use when constructing the cached reply key
    CACHED_REPLY_KEY = 'cached_reply'

    transport_type = 'wechat'

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        self.request_dict = {}
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

    def user_profile_key(self, open_id):
        return '@'.join([
            self.USER_PROFILE_KEY,
            open_id,
        ])

    def mask_key(self, user):
        return '@'.join([
            self.ADDR_MASK_KEY,
            user,
        ])

    def cached_reply_key(self, *parts):
        key_parts = [self.CACHED_REPLY_KEY]
        key_parts.extend(parts)
        return '@'.join(key_parts)

    def mask_addr(self, to_addr, mask):
        return '@'.join([to_addr, mask])

    def cache_addr_mask(self, user, mask):
        config = self.get_static_config()
        d = self.redis.setex(
            self.mask_key(user), config.wechat_mask_lifetime, mask)
        d.addCallback(lambda *a: mask)
        return d

    def get_addr_mask(self, user):
        d = self.redis.get(self.mask_key(user))
        d.addCallback(lambda mask: mask or self.DEFAULT_MASK)
        return d

    def clear_addr_mask(self, user):
        return self.redis.delete(self.mask_key(user))

    def handle_raw_inbound_message(self, request, wc_msg):
        return {
            TextMessage: self.handle_inbound_text_message,
            EventMessage: self.handle_inbound_event_message,
        }.get(wc_msg.__class__)(request, wc_msg)

    def wrap_expire(self, result, key, ttl):
        d = self.redis.expire(key, ttl)
        d.addCallback(lambda _: result)
        return d

    def mark_as_seen_recently(self, wc_msg_id):
        config = self.get_static_config()
        key = self.cached_reply_key(wc_msg_id)
        d = self.redis.setnx(key, 1)
        d.addCallback(
            lambda result: (
                self.wrap_expire(result, key, config.double_delivery_lifetime)
                if result else False))
        return d

    def was_seen_recently(self, wc_msg_id):
        return self.redis.exists(self.cached_reply_key(wc_msg_id))

    def get_cached_reply(self, wc_msg_id):
        return self.redis.get(self.cached_reply_key(wc_msg_id, 'reply'))

    def set_cached_reply(self, wc_msg_id, reply):
        config = self.get_static_config()
        return self.redis.setex(
            self.cached_reply_key(wc_msg_id, 'reply'),
            config.double_delivery_lifetime, reply)

    @inlineCallbacks
    def check_for_double_delivery(self, request, wc_msg_id):
        seen_recently = yield self.was_seen_recently(wc_msg_id)
        if not seen_recently:
            returnValue(False)

        cached_reply = yield self.get_cached_reply(wc_msg_id)
        if cached_reply:
            # we've got a reply still lying around, just parrot that instead.
            request.write(cached_reply)

        request.finish()
        returnValue(True)

    @inlineCallbacks
    def handle_inbound_text_message(self, request, wc_msg):
        double_delivery = yield self.check_for_double_delivery(
            request, wc_msg.msg_id)
        if double_delivery:
            log.msg('WeChat double delivery of message: %s' % (wc_msg.msg_id,))
            return

        lock = yield self.mark_as_seen_recently(wc_msg.msg_id)
        if not lock:
            log.msg('Unable to get lock for message id: %s' % (wc_msg.msg_id,))
            return

        config = self.get_static_config()
        if config.embed_user_profile:
            user_profile = yield self.get_user_profile(wc_msg.from_user_name)
        else:
            user_profile = {}

        mask = yield self.get_addr_mask(wc_msg.from_user_name)
        msg = yield self.publish_message(
            content=wc_msg.content,
            from_addr=wc_msg.from_user_name,
            to_addr=self.mask_addr(wc_msg.to_user_name, mask),
            timestamp=datetime.fromtimestamp(int(wc_msg.create_time)),
            transport_type=self.transport_type,
            transport_metadata={
                'wechat': {
                    'FromUserName': wc_msg.from_user_name,
                    'ToUserName': wc_msg.to_user_name,
                    'MsgType': 'text',
                    'MsgId': wc_msg.msg_id,
                    'UserProfile': user_profile,
                }
            })
        returnValue(msg)

    @inlineCallbacks
    def handle_inbound_event_message(self, request, wc_msg):
        if wc_msg.event.lower() in ('view', 'unsubscribe'):
            log.msg("%s clicked on %s" % (
                wc_msg.from_user_name, wc_msg.event_key))
            request.finish()
            yield self.clear_addr_mask(wc_msg.from_user_name)
            return

        if wc_msg.event_key:
            mask = yield self.cache_addr_mask(
                wc_msg.from_user_name, wc_msg.event_key)
        else:
            mask = yield self.get_addr_mask(wc_msg.from_user_name)

        if wc_msg.event.lower() in ('subscribe', 'click'):
            session_event = TransportUserMessage.SESSION_NEW
        else:
            session_event = TransportUserMessage.SESSION_NONE

        msg = yield self.publish_message(
            content=None,
            from_addr=wc_msg.from_user_name,
            to_addr=self.mask_addr(wc_msg.to_user_name, mask),
            timestamp=datetime.fromtimestamp(int(wc_msg.create_time)),
            transport_type=self.transport_type,
            session_event=session_event,
            transport_metadata={
                'wechat': {
                    'FromUserName': wc_msg.from_user_name,
                    'ToUserName': wc_msg.to_user_name,
                    'MsgType': 'event',
                    'Event': wc_msg.event,
                    'EventKey': wc_msg.event_key
                }
            })
        # Close the request to ensure we fire a push message on reply.
        request.finish()
        returnValue(msg)

    def force_close(self, message):
        request = self.get_request(message['message_id'])
        request.setResponseCode(http.INTERNAL_SERVER_ERROR)
        request.finish()

    def handle_finished_request(self, request):
        for message_id, request_ in self.request_dict.items():
            if request_ == request:
                self.request_dict.pop(message_id)

    def queue_request(self, message, request):
        if message is not None:
            self.request_dict[message['message_id']] = request

    def get_request(self, message_id):
        return self.request_dict.get(message_id, None)

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
        request = self.get_request(request_id)

        builder = self.infer_message_type(message)
        wc_msg = builder(message)

        if request is None or request.finished:
            # There's no pending request object for this message which
            # means we need to treat this as a customer service message
            # and hit WeChat's Push API (window available for 24hrs)
            return self.push_message(wc_msg, message)

        request.write(wc_msg.to_xml())
        request.finish()

        d = self.publish_ack(user_message_id=message['message_id'],
                             sent_message_id=message['message_id'])
        wc_metadata = message["transport_metadata"].get('wechat', {})
        if wc_metadata:
            d.addCallback(lambda _: self.set_cached_reply(
                wc_metadata['MsgId'], wc_msg.to_xml()))

        if message['session_event'] == TransportUserMessage.SESSION_CLOSE:
            d.addCallback(
                lambda _: self.clear_addr_mask(wc_msg.to_user_name))
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
            d.addCallback(
                lambda ack: self.clear_addr_mask(wc_message.from_user_name))
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
    def get_user_profile(self, open_id):
        config = self.get_static_config()
        up_key = self.user_profile_key(open_id)
        cached_up = yield self.redis.get(open_id)
        if cached_up:
            returnValue(json.loads(cached_up))

        access_token = yield self.get_access_token()
        response = yield http_request_full(self.make_url('user/info', {
            'access_token': access_token,
            'openid': open_id,
            'lang': config.embed_user_profile_lang,
        }), method='GET')
        user_profile = response.delivered_body
        yield self.redis.setex(up_key, config.embed_user_profile_lifetime,
                               user_profile)
        returnValue(json.loads(user_profile))

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
