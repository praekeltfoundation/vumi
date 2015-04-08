from urllib import urlencode

from twisted.internet.defer import inlineCallbacks
from twisted.web import http

from vumi.tests.helpers import VumiTestCase
from vumi.transports.netcore import NetcoreTransport
from vumi.transports.tests.helpers import TransportHelper
from vumi.utils import http_request_full


def request(transport, method, params={}, path=None):
    if path is None:
        path = transport.get_static_config().web_path

    addr = transport.server.getHost()
    url = 'http://%s:%s%s' % (addr.host,
                              addr.port,
                              path)
    return http_request_full(
        url, method=method, data=urlencode(params), headers={
            'Content-Type': ['application/x-www-form-urlencoded'],
        })


class NetCoreTestCase(VumiTestCase):

    transport_class = NetcoreTransport

    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))

    def get_transport(self, **config):
        defaults = {
            'twisted_endpoint': 'tcp:0',
        }
        defaults.update(config)
        return self.tx_helper.get_transport(defaults)

    @inlineCallbacks
    def test_inbound_sms_failure(self):
        transport = yield self.get_transport()
        resp = yield request(transport, 'POST', {
            'foo': 'bar'
        })
        self.assertEqual(resp.code, http.BAD_REQUEST)
        self.assertEqual(resp.delivered_body, (
            "Not all expected parameters received. Only allowing: "
            "['to_addr', 'from_addr', 'content', 'circle', 'source'], "
            "received: ['foo']"))
        self.assertEqual(
            [], self.tx_helper.get_dispatched_inbound())

    @inlineCallbacks
    def test_inbound_missing_values(self):
        transport = yield self.get_transport()
        resp = yield request(transport, 'POST', {
            'to_addr': '10010',
            'from_addr': '8800000000',
            'content': '',  # Intentionally empty!
            'source': 'sms',
            'circle': 'of life',
        })
        self.assertEqual(resp.code, http.BAD_REQUEST)
        self.assertEqual(resp.delivered_body, (
            "Not all parameters have values. "
            "Received: %r" % (sorted(
                ['', 'sms', 'of life', '10010', '8800000000']),)))
        self.assertEqual(
            [], self.tx_helper.get_dispatched_inbound())

    @inlineCallbacks
    def test_inbound_content_none_string_literal(self):
        transport = yield self.get_transport()
        resp = yield request(transport, 'POST', {
            'to_addr': '10010',
            'from_addr': '8800000000',
            'content': 'None',  # Python str(None) on netcore's side
            'source': 'sms',
            'circle': 'of life',
        })
        self.assertEqual(resp.code, http.BAD_REQUEST)
        self.assertEqual(resp.delivered_body, (
            '"None" string literal not allowed for content parameter.'))
        self.assertEqual(
            [], self.tx_helper.get_dispatched_inbound())

    @inlineCallbacks
    def test_inbound_sms_success(self):
        transport = yield self.get_transport()
        resp = yield request(transport, 'POST', {
            'to_addr': '10010',
            'from_addr': '8800000000',
            'content': 'foo',
            'source': 'sms',
            'circle': 'of life',
        })
        self.assertEqual(resp.code, http.OK)
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['to_addr'], '10010')
        self.assertEqual(msg['from_addr'], '08800000000')
        self.assertEqual(msg['content'], 'foo')
        self.assertEqual(msg['transport_metadata'], {
            'netcore': {
                'source': 'sms',
                'circle': 'of life',
            }
        })

    @inlineCallbacks
    def test_health_resource(self):
        transport = yield self.get_transport()
        health_path = transport.get_static_config().health_path
        resp = yield request(transport, 'GET', path=health_path)
        self.assertEqual(resp.delivered_body, 'OK')
        self.assertEqual(resp.code, http.OK)
