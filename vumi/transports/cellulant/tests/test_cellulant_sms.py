# -*- encoding: utf-8 -*-

import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks, DeferredQueue, returnValue

from vumi.utils import http_request, http_request_full
from vumi.tests.utils import MockHttpServer
from vumi.tests.helpers import VumiTestCase
from vumi.transports.cellulant import CellulantSmsTransport
from vumi.transports.tests.helpers import TransportHelper


class TestCellulantSmsTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.cellulant_sms_calls = DeferredQueue()
        self.mock_cellulant_sms = MockHttpServer(self.handle_request)
        yield self.mock_cellulant_sms.start()
        self.add_cleanup(self.mock_cellulant_sms.stop)

        self.config = {
            'web_path': "foo",
            'web_port': 0,
            'credentials': {
                '2371234567': {
                    'username': 'user',
                    'password': 'pass',
                },
                '9292': {
                    'username': 'other-user',
                    'password': 'other-pass',
                }
            },
            'outbound_url': self.mock_cellulant_sms.url,
        }
        self.tx_helper = self.add_helper(
            TransportHelper(CellulantSmsTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url()

    def handle_request(self, request):
        self.cellulant_sms_calls.put(request)
        return ''

    def mkurl(self, content, from_addr="2371234567", **kw):
        params = {
            'SOURCEADDR': from_addr,
            'DESTADDR': '12345',
            'MESSAGE': content,
            'ID': '1234567',
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
        req = yield self.cellulant_sms_calls.get()
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'GET')
        self.assertEqual({
                'username': ['other-user'],
                'password': ['other-pass'],
                'source': ['9292'],
                'destination': ['2371234567'],
                'message': ['hello world'],
                }, req.args)

    @inlineCallbacks
    def test_outbound_creds_selection(self):
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", to_addr="2371234567", from_addr='2371234567')
        req = yield self.cellulant_sms_calls.get()
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'GET')
        self.assertEqual({
                'username': ['user'],
                'password': ['pass'],
                'source': ['2371234567'],
                'destination': ['2371234567'],
                'message': ['hello world'],
                }, req.args)

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", to_addr="2371234567", from_addr='9292')
        req = yield self.cellulant_sms_calls.get()
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'GET')
        self.assertEqual({
                'username': ['other-user'],
                'password': ['other-pass'],
                'source': ['9292'],
                'destination': ['2371234567'],
                'message': ['hello world'],
                }, req.args)

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
        url = self.mkurl_raw(ID='12345678', DESTADDR='12345', MESSAGE='hello')
        response = yield http_request_full(url, '', method='GET')
        self.assertEqual(400, response.code)
        self.assertEqual(json.loads(response.delivered_body),
                         {'missing_parameter': ['SOURCEADDR']})

    @inlineCallbacks
    def test_ignored_parameters(self):
        url = self.mkurl('hello', channelID='a', keyword='b', CHANNELID='c',
                         serviceID='d', SERVICEID='e', unsub='f')
        response = yield http_request(url, '', method='GET')
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['content'], "hello")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})


class TestAcksCellulantSmsTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.cellulant_sms_calls = DeferredQueue()
        self.mock_cellulant_sms = MockHttpServer(self.handle_request)
        self._mock_response = ''
        yield self.mock_cellulant_sms.start()
        self.add_cleanup(self.mock_cellulant_sms.stop)

        self.config = {
            'web_path': "foo",
            'web_port': 0,
            'credentials': {
                '2371234567': {
                    'username': 'user',
                    'password': 'pass',
                },
                '9292': {
                    'username': 'other-user',
                    'password': 'other-pass',
                }
            },
            'outbound_url': self.mock_cellulant_sms.url,
            'validation_mode': 'permissive',
        }
        self.tx_helper = self.add_helper(
            TransportHelper(CellulantSmsTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url()

    def mock_response(self, response):
        self._mock_response = response

    def handle_request(self, request):
        self.cellulant_sms_calls.put(request)
        return self._mock_response

    @inlineCallbacks
    def mock_event(self, msg, nr_events):
        self.mock_response(msg)
        yield self.tx_helper.make_dispatch_outbound(
            "foo", to_addr='2371234567', message_id='id_%s' % (msg,))
        yield self.cellulant_sms_calls.get()
        events = yield self.tx_helper.wait_for_dispatched_events(nr_events)
        returnValue(events)

    @inlineCallbacks
    def test_nack_param_error_E0(self):
        [nack] = yield self.mock_event('E0', 1)
        self.assertEqual(nack['event_type'], 'nack')
        self.assertEqual(nack['user_message_id'], 'id_E0')
        self.assertEqual(nack['nack_reason'],
            self.transport.KNOWN_ERROR_RESPONSE_CODES['E0'])

    @inlineCallbacks
    def test_nack_login_error_E1(self):
        [nack] = yield self.mock_event('E1', 1)
        self.assertEqual(nack['event_type'], 'nack')
        self.assertEqual(nack['user_message_id'], 'id_E1')
        self.assertEqual(nack['nack_reason'],
            self.transport.KNOWN_ERROR_RESPONSE_CODES['E1'])

    @inlineCallbacks
    def test_nack_credits_error_E2(self):
        [nack] = yield self.mock_event('E2', 1)
        self.assertEqual(nack['event_type'], 'nack')
        self.assertEqual(nack['user_message_id'], 'id_E2')
        self.assertEqual(nack['nack_reason'],
            self.transport.KNOWN_ERROR_RESPONSE_CODES['E2'])

    @inlineCallbacks
    def test_nack_delivery_failed_1005(self):
        [nack] = yield self.mock_event('1005', 1)
        self.assertEqual(nack['event_type'], 'nack')
        self.assertEqual(nack['user_message_id'], 'id_1005')
        self.assertEqual(nack['nack_reason'],
            self.transport.KNOWN_ERROR_RESPONSE_CODES['1005'])

    @inlineCallbacks
    def test_unknown_response(self):
        [nack] = yield self.mock_event('something_unexpected', 1)
        self.assertEqual(nack['event_type'], 'nack')
        self.assertEqual(nack['user_message_id'], 'id_something_unexpected')
        self.assertEqual(nack['nack_reason'],
            'Unknown response code: something_unexpected')

    @inlineCallbacks
    def test_ack_success(self):
        [event] = yield self.mock_event('1', 1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], 'id_1')


class TestPermissiveCellulantSmsTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.cellulant_sms_calls = DeferredQueue()
        self.mock_cellulant_sms = MockHttpServer(self.handle_request)
        yield self.mock_cellulant_sms.start()
        self.add_cleanup(self.mock_cellulant_sms.stop)

        self.config = {
            'web_path': "foo",
            'web_port': 0,
            'credentials': {
                '2371234567': {
                    'username': 'user',
                    'password': 'pass',
                },
                '9292': {
                    'username': 'other-user',
                    'password': 'other-pass',
                }
            },
            'outbound_url': self.mock_cellulant_sms.url,
            'validation_mode': 'permissive',
        }
        self.tx_helper = self.add_helper(
            TransportHelper(CellulantSmsTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url()

    def handle_request(self, request):
        self.cellulant_sms_calls.put(request)
        return ''

    def mkurl(self, content, from_addr="2371234567", **kw):
        params = {
            'SOURCEADDR': from_addr,
            'DESTADDR': '12345',
            'MESSAGE': content,
            'ID': '1234567',
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
    def test_bad_parameter_in_permissive_mode(self):
        url = self.mkurl('hello', foo='bar')
        response = yield http_request_full(url, '', method='GET')
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(200, response.code)
        self.assertEqual(json.loads(response.delivered_body),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    def test_missing_parameters(self):
        url = self.mkurl_raw(ID='12345678', DESTADDR='12345', MESSAGE='hello')
        response = yield http_request_full(url, '', method='GET')
        self.assertEqual(400, response.code)
        self.assertEqual(json.loads(response.delivered_body),
                         {'missing_parameter': ['SOURCEADDR']})

    @inlineCallbacks
    def test_ignored_parameters(self):
        url = self.mkurl('hello', channelID='a', keyword='b', CHANNELID='c',
                         serviceID='d', SERVICEID='e', unsub='f')
        response = yield http_request(url, '', method='GET')
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['content'], "hello")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})
