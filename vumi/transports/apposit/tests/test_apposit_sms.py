# -*- encoding: utf-8 -*-

import json
from urllib import urlencode

from twisted.web import http
from twisted.internet.defer import inlineCallbacks, DeferredQueue

from vumi.tests.utils import MockHttpServer
from vumi.utils import http_request_full
from vumi.transports.apposit import AppositSmsTransport
from vumi.transports.tests.utils import TransportTestCase


class TestAppositSmsTransport(TransportTestCase):
    transport_name = 'test_apposit_sms_transport'
    transport_class = AppositSmsTransport

    @inlineCallbacks
    def setUp(self):
        super(TestAppositSmsTransport, self).setUp()

        self.mock_server = MockHttpServer(self.handle_inbound_request)
        self.outbound_requests = DeferredQueue()
        self.mock_server_response = ''
        self.mock_server_response_code = http.OK
        yield self.mock_server.start()

        config = {
            'transport_name': self.transport_name,
            'web_path': 'api/v1/apposit/sms',
            'web_port': 0,
            'credentials': {
                '8123': {
                    'username': 'root',
                    'password': 'toor',
                    'service_id': 'service-id-1',
                },
                '8124': {
                    'username': 'admin',
                    'password': 'nimda',
                    'service_id': 'service-id-2',
                }
            },
            'outbound_url': self.mock_server.url,
        }
        self.transport = yield self.get_transport(config)
        self.transport_url = self.transport.get_transport_url()
        self.web_path = config['web_path']

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_server.stop()
        yield super(TestAppositSmsTransport, self).tearDown()

    def send_full_inbound_request(self, **params):
        return http_request_full(
            '%s%s?%s' % (self.transport_url, self.web_path, urlencode(params)),
            '', method='POST')

    def send_inbound_request(self, **kwargs):
        params = {
            'fromAddress': '251911223344',
            'toAddress': '8123',
            'channel': 'sms',
            'content': 'never odd or even',
        }
        params.update(kwargs)
        return self.send_full_inbound_request(**params)

    def handle_inbound_request(self, request):
        self.outbound_requests.put(request)
        request.setResponseCode(self.mock_server_response_code)
        return self.mock_server_response

    def set_mock_server_response(self, code=http.OK, body=''):
        self.mock_server_response_code = code
        self.mock_server_response = body

    def mk_outbound_message(self, **kwargs):
        args = {
            'from_addr': '8123',
            'to_addr': '251911223344',
            'content': 'so many dynamos',
        }
        args.update(kwargs)
        return self.mkmsg_out(**args)

    def assert_outbound_request(self, request, **kwargs):
        expected_args = {
            'username': ['root'],
            'password': ['toor'],
            'serviceId': ['service-id-1'],
            'fromAddress': ['8123'],
            'toAddress': ['251911223344'],
            'content': ['so many dynamos'],
            'channel': ['sms'],
        }
        expected_args.update(kwargs)

        self.assertEqual(request.path, '/')
        self.assertEqual(request.method, 'POST')
        self.assertEqual(expected_args, request.args)

    def assert_message_fields(self, msg, **kwargs):
        fields = {
            'transport_name': self.transport_name,
            'from_addr': '251911223344',
            'to_addr': '8123',
            'content': 'so many dynamos',
            'provider': 'apposit',
            'transport_metadata': {}
        }
        fields.update(kwargs)

        for field, expected_value in fields.iteritems():
            self.assertEqual(msg[field], expected_value)

    def assert_ack(self, ack, msg):
        self.assertEqual(ack.payload['event_type'], 'ack')
        self.assertEqual(ack.payload['user_message_id'], msg['message_id'])
        self.assertEqual(ack.payload['sent_message_id'], msg['message_id'])

    def assert_nack(self, nack, msg, reason):
        self.assertEqual(nack.payload['event_type'], 'nack')
        self.assertEqual(nack.payload['user_message_id'], msg['message_id'])
        self.assertEqual(nack.payload['nack_reason'], reason)

    @inlineCallbacks
    def test_inbound(self):
        response = yield self.send_inbound_request(content='so many dynamos')

        [msg] = self.get_dispatched_messages()
        self.assert_message_fields(msg,
            transport_name=self.transport_name,
            from_addr='251911223344',
            to_addr='8123',
            content='so many dynamos',
            provider='apposit',
            transport_metadata={})

        self.assertEqual(response.code, http.OK)
        self.assertEqual(json.loads(response.delivered_body),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    def test_outbound(self):
        msg = self.mk_outbound_message(
            from_addr='8123',
            to_addr='251911223344',
            content='racecar')
        yield self.dispatch(msg)

        request = yield self.outbound_requests.get()
        self.assert_outbound_request(request,
            username=['root'],
            password=['toor'],
            serviceId=['service-id-1'],
            content=['racecar'],
            fromAddress=['8123'],
            toAddress=['251911223344'],
            channel=['sms'])

        [ack] = yield self.wait_for_dispatched_events(1)
        self.assert_ack(ack, msg)

    @inlineCallbacks
    def test_inbound_requests_for_non_ascii_content(self):
        response = yield self.send_inbound_request(content='Hliðskjálf')
        [msg] = self.get_dispatched_messages()
        self.assert_message_fields(msg, content=u'Hliðskjálf')

        self.assertEqual(response.code, http.OK)
        self.assertEqual(json.loads(response.delivered_body),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    def test_inbound_requests_for_invalid_channel(self):
        response = yield self.send_full_inbound_request(
            fromAddress='251911223344',
            toAddress='8123',
            channel='steven',
            content='never odd or even')

        self.assertEqual(response.code, 400)
        self.assertEqual(json.loads(response.delivered_body),
                         {'invalid_channel': 'steven'})

    @inlineCallbacks
    def test_inbound_requests_for_unexpected_param(self):
        response = yield self.send_full_inbound_request(
            fromAddress='251911223344',
            toAddress='8123',
            channel='sms',
            steven='its a trap',
            content='never odd or even')

        self.assertEqual(response.code, 400)
        self.assertEqual(json.loads(response.delivered_body),
                         {'unexpected_parameter': ['steven']})

    @inlineCallbacks
    def test_inbound_requests_for_missing_param(self):
        response = yield self.send_full_inbound_request(
            fromAddress='251911223344',
            toAddress='8123',
            content='never odd or even')

        self.assertEqual(response.code, 400)
        self.assertEqual(json.loads(response.delivered_body),
                         {'missing_parameter': ['channel']})

    @inlineCallbacks
    def test_outbound_request_credential_selection(self):
        msg1 = self.mk_outbound_message(from_addr='8123')
        yield self.dispatch(msg1)
        request1 = yield self.outbound_requests.get()
        self.assert_outbound_request(request1,
            fromAddress=['8123'],
            username=['root'],
            password=['toor'],
            serviceId=['service-id-1'])

        msg2 = self.mk_outbound_message(from_addr='8124')
        yield self.dispatch(msg2)
        request2 = yield self.outbound_requests.get()
        self.assert_outbound_request(request2,
            fromAddress=['8124'],
            username=['admin'],
            password=['nimda'],
            serviceId=['service-id-2'])

        [ack1, ack2] = yield self.wait_for_dispatched_events(2)
        self.assert_ack(ack1, msg1)
        self.assert_ack(ack2, msg2)

    @inlineCallbacks
    def test_outbound_requests_for_non_ascii_content(self):
        msg = self.mk_outbound_message(content=u'Hliðskjálf')
        yield self.dispatch(msg)
        request = yield self.outbound_requests.get()
        self.assert_outbound_request(request, content=['Hliðskjálf'])

        [ack] = yield self.wait_for_dispatched_events(1)
        self.assert_ack(ack, msg)

    @inlineCallbacks
    def test_outbound_requests_for_known_error_responses(self):
        self.set_mock_server_response(http.BAD_REQUEST, '102999')

        msg = self.mkmsg_out(
            from_addr='8123',
            to_addr='251911223344',
            content='racecar')
        yield self.dispatch(msg)

        [nack] = yield self.wait_for_dispatched_events(1)
        self.assert_nack(
            nack, msg, self.transport.KNOWN_ERROR_RESPONSE_CODES['102999'])

    @inlineCallbacks
    def test_outbound_requests_for_unknown_error_responses(self):
        self.set_mock_server_response(http.BAD_REQUEST, '103000')

        msg = self.mk_outbound_message()
        yield self.dispatch(msg)

        [nack] = yield self.wait_for_dispatched_events(1)
        self.assert_nack(
            nack, msg, self.transport.UNKNOWN_ERROR_RESPONSE % 103000)
