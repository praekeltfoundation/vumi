# -*- test-case-name: vumi.transports.wechat.tests.test_wechat -*-

from vumi.config import ConfigText, ConfigServerEndpoint
from vumi.transports import Transport
from vumi.transports.httprpc.httprpc import HttpRpcHealthResource
from vumi.utils import build_web_site

from twisted.internet.defer import inlineCallbacks
from twisted.web.resource import Resource


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

    def __init__(self, transport):
        Resource.__init__(self)
        self.transport = transport

    isLeaf = True


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
        print self.server

    def get_health_response(self):
        return "OK"
