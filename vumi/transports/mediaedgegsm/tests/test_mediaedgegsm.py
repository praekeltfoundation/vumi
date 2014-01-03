# -*- encoding: utf-8 -*-

import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http

from vumi.utils import http_request, http_request_full
from vumi.tests.utils import MockHttpServer
from vumi.tests.helpers import VumiTestCase
from vumi.transports.mediaedgegsm import MediaEdgeGSMTransport
from vumi.transports.tests.helpers import TransportHelper


class TestMediaEdgeGSMTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.mediaedgegsm_calls = DeferredQueue()
        self.mock_mediaedgegsm = MockHttpServer(self.handle_request)
        self.add_cleanup(self.mock_mediaedgegsm.stop)
        yield self.mock_mediaedgegsm.start()

        self.config = {
            'web_path': "foo",
            'web_port': 0,
            'username': 'user',
            'password': 'pass',
            'outbound_url': self.mock_mediaedgegsm.url,
            'outbound_username': 'username',
            'outbound_password': 'password',
            'operator_mappings': {
                '417': {
                    '417912': 'VODA',
                    '417913': 'TIGO',
                    '417914': 'UNKNOWN',
                }
            }
        }
        self.tx_helper = self.add_helper(
            TransportHelper(MediaEdgeGSMTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url()
        self.mediaedgegsm_response = ''
        self.mediaedgegsm_response_code = http.OK

    def handle_request(self, request):
        self.mediaedgegsm_calls.put(request)
        request.setResponseCode(self.mediaedgegsm_response_code)
        return self.mediaedgegsm_response

    def mkurl(self, content, from_addr="2371234567", **kw):
        params = {
            'ServiceNumber': '12345',
            'PhoneNumber': from_addr,
            'SMSBODY': content,
            'USN': 'user',
            'PWD': 'pass',
            'Operator': 'foo',
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
        deferred = http_request(url, '', method='GET')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['to_addr'], "12345")
        self.assertEqual(msg['from_addr'], "2371234567")
        self.assertEqual(msg['content'], "hello")

        yield self.tx_helper.make_dispatch_reply(msg, 'message received')
        response = yield deferred
        self.assertEqual(response, 'message received')

    @inlineCallbacks
    def test_outbound(self):
        msisdns = ['+41791200000', '+41791300000', '+41791400000']
        operators = ['VODA', 'TIGO', 'UNKNOWN']

        sent_messages = []
        for msisdn in msisdns:
            msg = yield self.tx_helper.make_dispatch_outbound(
                "outbound", to_addr=msisdn)
            sent_messages.append(msg)

        req1 = yield self.mediaedgegsm_calls.get()
        req2 = yield self.mediaedgegsm_calls.get()
        req3 = yield self.mediaedgegsm_calls.get()
        requests = [req1, req2, req3]

        for req in requests:
            self.assertEqual(req.path, '/')
            self.assertEqual(req.method, 'GET')

        collections = zip(msisdns, operators, sent_messages, requests)
        for msisdn, operator, msg, req in collections:
            self.assertEqual({
                    'USN': ['username'],
                    'PWD': ['password'],
                    'SmsID': [msg['message_id']],
                    'PhoneNumber': [msisdn.lstrip('+')],
                    'Operator': [operator],
                    'SmsBody': [msg['content']],
                    }, req.args)

    @inlineCallbacks
    def test_nack(self):
        self.mediaedgegsm_response_code = http.NOT_FOUND
        self.mediaedgegsm_response = 'Not Found'

        msg = yield self.tx_helper.make_dispatch_outbound(
            "outbound", to_addr='+41791200000')

        yield self.mediaedgegsm_calls.get()
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'],
            'Unexpected response code: 404')

    @inlineCallbacks
    def test_bad_parameter(self):
        url = self.mkurl('hello', foo='bar')
        response = yield http_request_full(url, '', method='GET')
        self.assertEqual(400, response.code)
        self.assertEqual(json.loads(response.delivered_body),
                         {'unexpected_parameter': ['foo']})

    @inlineCallbacks
    def test_missing_parameters(self):
        url = self.mkurl_raw(ServiceNumber='12345', SMSBODY='hello',
            USN='user', PWD='pass', Operator='foo')
        response = yield http_request_full(url, '', method='GET')
        self.assertEqual(400, response.code)
        self.assertEqual(json.loads(response.delivered_body),
                         {'missing_parameter': ['PhoneNumber']})

    @inlineCallbacks
    def test_invalid_credentials(self):
        url = self.mkurl_raw(ServiceNumber='12345', SMSBODY='hello',
            USN='something', PWD='wrong', Operator='foo', PhoneNumber='1234')
        response = yield http_request_full(url, '', method='GET')
        self.assertEqual(400, response.code)
        self.assertEqual(json.loads(response.delivered_body),
                         {'credentials': 'invalid'})

    @inlineCallbacks
    def test_handle_non_ascii_input(self):
        url = self.mkurl(u"öæł".encode("utf-8"))
        deferred = http_request_full(url, '', method='GET')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['to_addr'], "12345")
        self.assertEqual(msg['from_addr'], "2371234567")
        self.assertEqual(msg['content'], u"öæł")

        yield self.tx_helper.make_dispatch_reply(msg, u'Zoë says hi')

        response = yield deferred
        self.assertEqual(response.headers.getRawHeaders('Content-Type'),
            ['text/plain; charset=utf-8'])
        self.assertEqual(response.delivered_body,
            u'Zoë says hi'.encode('utf-8'))
