# -*- test-case-name: vumi.transports.wechat.tests.test_wechat -*-

import hashlib

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.web.resource import Resource
from twisted.web import http
from twisted.web.server import NOT_DONE_YET

from vumi.config import ConfigText, ConfigServerEndpoint
from vumi.transports import Transport
from vumi.transports.httprpc.httprpc import HttpRpcHealthResource
from vumi.transports.wechat.parser import WeChatParser
from vumi.transports.wechat.message_types import TextMessage, EventMessage
from vumi.utils import build_web_site
from vumi.message import TransportUserMessage


def is_verifiable(request):
    params = ['signature', 'timestamp', 'nonce']
    return all([(key in request.args) for key in params])


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
        reactor.callLater(0, d.callback, request)
        return NOT_DONE_YET

    def handle_request(self, request):
        wc_msg = WeChatParser.parse(request.content.read())
        return self.transport.handle_raw_inbound_message(wc_msg)


class WeChatTransport(Transport):

    CONFIG_CLASS = WeChatConfig

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

        self.server = yield self.endpoint.listen(self.factory)

    def handle_raw_inbound_message(self, wc_msg):
        return {
            TextMessage: self.handle_inbound_text_message,
            EventMessage: self.handle_inbound_event_message,
        }.get(wc_msg.__class__)(wc_msg)

    def handle_inbound_text_message(self, wc_msg):
        return self.publish_message(
            content=wc_msg.Content,
            from_addr=wc_msg.FromUserName,
            to_addr=wc_msg.ToUserName,
            timestamp=wc_msg.CreateTime,
            transport_type='wechat',
            transport_metadata={
                'wechat': {
                    'FromUserName': wc_msg.FromUserName,
                    'ToUserName': wc_msg.ToUserName,
                    'MsgType': wc_msg.MsgType,
                    'MsgId': wc_msg.MsgId,
                }
            })

    def handle_inbound_event_message(self, wc_msg):

        return self.publish_message(
            content=None,
            from_addr=wc_msg.FromUserName,
            to_addr=wc_msg.ToUserName,
            timestamp=wc_msg.CreateTime,
            session_event=(
                TransportUserMessage.SESSION_NEW
                if wc_msg.Event == 'subscribe'
                else TransportUserMessage.SESSION_CLOSE),
            transport_type='wechat',
            transport_metadata={
                'wechat': {
                    'FromUserName': wc_msg.FromUserName,
                    'ToUserName': wc_msg.ToUserName,
                    'MsgType': wc_msg.msg_type,
                    'Event': wc_msg.Event,
                    'EventKey': wc_msg.EventKey
                }
            })

    def force_close(self, message):
        request = self.pop_request(message['message_id'])
        request.setResponseCode(http.INTERNAL_SERVER_ERROR)
        request.finish()

    def queue_request(self, message, request):
        self.request_queue[message['message_id']] = request

    def pop_request(self, message_id):
        return self.request_queue.pop(message_id)

    def handle_outbound_message(self, message):
        """
        Read outbound message and do what needs to be done with them.
        """
        request_id = message['in_reply_to']
        request = self.pop_request(request_id)

        metadata = message['transport_metadata']['wechat']

        wc_msg = TextMessage(
            {
                'ToUserName': metadata['FromUserName'],
                'FromUserName': metadata['ToUserName'],
                'CreateTime': message['timestamp'],
                'Content': message['content']
            })

        request.write(wc_msg.to_xml().encode('utf-8'))
        request.finish()

    def teardown_transport(self):
        return self.server.stopListening()

    def get_health_response(self):
        return "OK"
