# -*- encoding: utf-8 -*-

import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http

from vumi.utils import http_request, http_request_full
from vumi.tests.utils import MockHttpServer
from vumi.tests.helpers import VumiTestCase
from vumi.transports.mediafonemc import MediafoneTransport
from vumi.transports.tests.helpers import TransportHelper


class TestMediafoneTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.mediafone_calls = DeferredQueue()
        self.mock_mediafone = MockHttpServer(self.handle_request)
        yield self.mock_mediafone.start()
        self.add_cleanup(self.mock_mediafone.stop)

        self.config = {
            'web_path': "foo",
            'web_port': 0,
            'username': 'user',
            'password': 'pass',
            'outbound_url': self.mock_mediafone.url,
        }
        self.tx_helper = self.add_helper(TransportHelper(MediafoneTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url()
        self.mediafonemc_response = ''
        self.mediafonemc_response_code = http.OK

    def handle_request(self, request):
        self.mediafone_calls.put(request)
        request.setResponseCode(self.mediafonemc_response_code)
        return self.mediafonemc_response

    def mkurl(self, content, from_addr="2371234567", **kw):
        params = {
            'to': '12345',
            'from': from_addr,
            'sms': content,
            }
        params.update(kw)
        return self.mkurl_raw(**params)

    def mkurl_raw(self, **params):
        return '%s%s?%s' % (
            self.transport_url,
            self.config['web_path'],
            urlencode(params)
        )

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(
            self.transport_url + "health", "", method='GET')
        self.assertEqual(json.loads(result), {'pending_requests': 0})

    @inlineCallbacks
    def test_inbound(self):
        url = self.mkurl('hello')
        response = yield http_request(url, '', method='GET')
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['to_addr'], "12345")
        self.assertEqual(msg['from_addr'], "2371234567")
        self.assertEqual(msg['content'], "hello")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    def test_outbound(self):
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", to_addr="2371234567")
        req = yield self.mediafone_calls.get()
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'GET')
        self.assertEqual({
                'username': ['user'],
                'phone': ['2371234567'],
                'password': ['pass'],
                'msg': ['hello world'],
                }, req.args)

    @inlineCallbacks
    def test_nack(self):
        self.mediafonemc_response_code = http.NOT_FOUND
        self.mediafonemc_response = 'Not Found'

        msg = yield self.tx_helper.make_dispatch_outbound(
            "outbound", to_addr="2371234567")

        yield self.mediafone_calls.get()
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'],
            'Unexpected response code: 404')

    @inlineCallbacks
    def test_handle_non_ascii_input(self):
        url = self.mkurl(u"öæł".encode("utf-8"))
        response = yield http_request(url, '', method='GET')
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['to_addr'], "12345")
        self.assertEqual(msg['from_addr'], "2371234567")
        self.assertEqual(msg['content'], u"öæł")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    def test_bad_parameter(self):
        url = self.mkurl('hello', foo='bar')
        response = yield http_request_full(url, '', method='GET')
        self.assertEqual(400, response.code)
        self.assertEqual(json.loads(response.delivered_body),
                         {'unexpected_parameter': ['foo']})

    @inlineCallbacks
    def test_missing_parameters(self):
        url = self.mkurl_raw(to='12345', sms='hello')
        response = yield http_request_full(url, '', method='GET')
        self.assertEqual(400, response.code)
        self.assertEqual(json.loads(response.delivered_body),
                         {'missing_parameter': ['from']})
