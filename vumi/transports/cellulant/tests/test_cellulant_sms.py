# -*- encoding: utf-8 -*-

import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks, DeferredQueue

from vumi.utils import http_request, http_request_full
from vumi.tests.utils import MockHttpServer
from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.cellulant import CellulantSmsTransport


class TestCellulantSmsTransport(TransportTestCase):

    transport_name = 'test_cellulant_sms_transport'
    transport_class = CellulantSmsTransport

    @inlineCallbacks
    def setUp(self):
        super(TestCellulantSmsTransport, self).setUp()

        self.cellulant_sms_calls = DeferredQueue()
        self.mock_cellulant_sms = MockHttpServer(self.handle_request)
        yield self.mock_cellulant_sms.start()

        self.config = {
            'transport_name': self.transport_name,
            'web_path': "foo",
            'web_port': 0,
            'username': 'user',
            'password': 'pass',
            'outbound_url': self.mock_cellulant_sms.url,
        }
        self.transport = yield self.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url()

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_cellulant_sms.stop()
        yield super(TestCellulantSmsTransport, self).tearDown()

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
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['to_addr'], "12345")
        self.assertEqual(msg['from_addr'], "2371234567")
        self.assertEqual(msg['content'], "hello")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    def test_outbound(self):
        yield self.dispatch(self.mkmsg_out(to_addr="2371234567"))
        req = yield self.cellulant_sms_calls.get()
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'GET')
        self.assertEqual({
                'username': ['user'],
                'password': ['pass'],
                'source': ['9292'],
                'destination': ['2371234567'],
                'message': ['hello world'],
                }, req.args)

    @inlineCallbacks
    def test_handle_non_ascii_input(self):
        url = self.mkurl(u"öæł".encode("utf-8"))
        response = yield http_request(url, '', method='GET')
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['transport_name'], self.transport_name)
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
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['content'], "hello")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})


class TestPermissiveCellulantSmsTransport(TransportTestCase):

    transport_name = 'test_cellulant_sms_transport'
    transport_class = CellulantSmsTransport

    @inlineCallbacks
    def setUp(self):
        super(TestPermissiveCellulantSmsTransport, self).setUp()

        self.cellulant_sms_calls = DeferredQueue()
        self.mock_cellulant_sms = MockHttpServer(self.handle_request)
        yield self.mock_cellulant_sms.start()

        self.config = {
            'transport_name': self.transport_name,
            'web_path': "foo",
            'web_port': 0,
            'username': 'user',
            'password': 'pass',
            'outbound_url': self.mock_cellulant_sms.url,
            'validation_mode': 'permissive',
        }
        self.transport = yield self.get_transport(self.config)
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
    def tearDown(self):
        yield self.mock_cellulant_sms.stop()
        yield super(TestPermissiveCellulantSmsTransport, self).tearDown()

    @inlineCallbacks
    def test_bad_parameter_in_permissive_mode(self):
        url = self.mkurl('hello', foo='bar')
        response = yield http_request_full(url, '', method='GET')
        [msg] = self.get_dispatched_messages()
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
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['content'], "hello")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})
