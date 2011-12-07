import json
from urllib import urlencode
from twisted.internet.defer import inlineCallbacks

from vumi.utils import http_request
from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.api import HttpApiTransport


class TestHttpApiTransport(TransportTestCase):

    transport_name = 'test_http_api_transport'
    transport_class = HttpApiTransport

    @inlineCallbacks
    def setUp(self):
        super(TestHttpApiTransport, self).setUp()
        self.config = {
            'transport_name': self.transport_name,
            'web_path': "foo",
            'web_port': 0,
        }

        self.transport = yield self.get_transport(self.config)
        addr = self.transport.web_resource.getHost()
        self.transport_url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(self.transport_url + "health", "",
                                    method='GET')
        self.assertEqual(json.loads(result), {})

    @inlineCallbacks
    def test_inbound(self):
        url = '%s%s?%s' % (
            self.transport_url,
            self.config['web_path'],
            urlencode({
                'to_addr': 555,
                'from_addr': 123,
                'content': 'hello',
            })
        )
        response = yield http_request(url, '', method='GET')
        [msg] = self.get_dispatched_messages()
        payload = msg.payload
        self.assertEqual(payload['transport_name'], self.transport_name)
        self.assertEqual(payload['to_addr'], "555")
        self.assertEqual(payload['from_addr'], "123")
        self.assertEqual(payload['content'], "hello")
        self.assertEqual(json.loads(response), {
            'message_id': payload['message_id'],
        })
