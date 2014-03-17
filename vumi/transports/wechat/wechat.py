# -*- test-case-name: vumi.transports.wechat.tests.test_wechat -*-

import hashlib

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.config import ConfigText, ConfigServerEndpoint
from vumi.transports import Transport
from vumi.transports.httprpc.httprpc import HttpRpcHealthResource
from vumi.transports.wechat.parser import WeChatParser
from vumi.utils import build_web_site
from vumi import log


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
        if all([lambda key: key in request.args,
                ['signature', 'timestamp', 'nonce', 'echostr']]):
            return self.verify(request)
        return ''

    def render_POST(self, request):
        d = Deferred()
        d.addCallback(self.handle_request)
        reactor.callLater(0, d.callback, request)
        return NOT_DONE_YET

    def verify(self, request):
        signature = request.args['signature'][0]
        timestamp = request.args['timestamp'][0]
        nonce = request.args['nonce'][0]
        echostr = request.args['echostr'][0]
        token = self.config.auth_token

        hash_ = hashlib.sha1(''.join(sorted([timestamp, nonce, token])))

        if hash_.hexdigest() == signature:
            return echostr
        return ''

    def handle_request(self, request):
        wc_msg = WeChatParser.parse(request.content.read())
        request.write(wc_msg.to_xml())
        request.finish()


class WeChatTransport(Transport):

    CONFIG_CLASS = WeChatConfig

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()

        self.endpoint = config.twisted_endpoint
        self.factory = build_web_site({
            config.health_path: HttpRpcHealthResource(self),
            config.web_path: WeChatResource(self),
        })

        self.server = yield self.endpoint.listen(self.factory)

    def teardown_transport(self):
        return self.server.stopListening()

    def get_health_response(self):
        return "OK"
