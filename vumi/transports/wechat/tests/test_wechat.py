import hashlib
from urllib import urlencode

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import Clock
from twisted.web.server import NOT_DONE_YET

from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import MockHttpServer
from vumi.transports.tests.helpers import TransportHelper
from vumi.transports.wechat import WeChatTransport
from vumi.utils import http_request_full


def request(transport, method, path='', params={}, data=None):
    addr = transport.server.getHost()
    if params:
        path += '?%s' % (urlencode(params),)
    url = 'http://%s:%s%s%s' % (
        addr.host,
        addr.port,
        transport.get_static_config().web_path,
        path)
    return http_request_full(url, method=method, data=data)


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
    def test_auth_success(self):
        transport = yield self.get_transport()

        nonce = '1234'
        timestamp = '2014-01-01T00:00:00'
        good_signature = hashlib.sha1(
            ''.join(sorted([timestamp, nonce, 'token']))).hexdigest()

        resp = yield request(
            transport, "GET", params={
                'signature': good_signature,
                'timestamp': timestamp,
                'nonce': nonce,
                'echostr': 'success'
            })
        self.assertEqual(resp.delivered_body, 'success')

    @inlineCallbacks
    def test_auth_fail(self):
        transport = yield self.get_transport()

        nonce = '1234'
        timestamp = '2014-01-01T00:00:00'
        bad_signature = hashlib.sha1(
            ''.join(sorted([timestamp, nonce, 'bad_token']))).hexdigest()

        resp = yield request(
            transport, "GET", params={
                'signature': bad_signature,
                'timestamp': timestamp,
                'nonce': nonce,
                'echostr': 'success'
            })
        self.assertNotEqual(resp.delivered_body, 'success')
