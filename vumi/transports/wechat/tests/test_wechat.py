from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import Clock
from twisted.web.server import NOT_DONE_YET

from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import MockHttpServer
from vumi.transports.tests.helpers import TransportHelper
from vumi.transports.wechat import WeChatTransport


class WeChatTestCase(VumiTestCase):

    transport_class = WeChatTransport

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.mock_server = MockHttpServer(self.handle_inbound_request)
        self.add_cleanup(self.mock_server.stop)
        self.clock = Clock()

        yield self.mock_server.start()

    def get_transport(self, **config):
        defaults = {
            'api_url': self.mock_server.url,
            'auth_token': 'token',
            'twisted_endpoint': 'tcp:0'
        }
        defaults.update(config)
        return self.tx_helper.get_transport(defaults)

    def handle_inbound_request(self, request):
        reactor.callLater(0, request.finish)
        return NOT_DONE_YET

    @inlineCallbacks
    def test_auth(self):
        transport = yield self.get_transport()
        print transport
