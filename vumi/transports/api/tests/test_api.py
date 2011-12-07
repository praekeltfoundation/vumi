# -*- encoding: utf-8 -*-

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

    def mkurl(self, content, from_addr=123, to_addr=555):
        return '%s%s?%s' % (
            self.transport_url,
            self.config['web_path'],
            urlencode({
                'to_addr': to_addr,
                'from_addr': from_addr,
                'content': content,
            })
        )

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(self.transport_url + "health", "",
                                    method='GET')
        self.assertEqual(json.loads(result), {})

    @inlineCallbacks
    def test_inbound(self):
        url = self.mkurl('hello')
        response = yield http_request(url, '', method='GET')
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['to_addr'], "555")
        self.assertEqual(msg['from_addr'], "123")
        self.assertEqual(msg['content'], "hello")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    def test_handle_non_ascii_input(self):
        url = self.mkurl(u"öæł".encode("utf-8"))
        response = yield http_request(url, '', method='GET')
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['to_addr'], "555")
        self.assertEqual(msg['from_addr'], "123")
        self.assertEqual(msg['content'], u"öæł")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})
