# -*- encoding: utf-8 -*-

import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks, DeferredQueue

from vumi.utils import http_request, http_request_full
from vumi.tests.utils import MockHttpServer
from vumi.transports.tests.utils import TransportTestCase
from vumi.transports.mtech_kenya import MTechKenyaTransport


class TestMTechKenyaTransport(TransportTestCase):

    transport_name = 'test_mtech_kenya_transport'
    transport_class = MTechKenyaTransport

    @inlineCallbacks
    def setUp(self):
        super(TestMTechKenyaTransport, self).setUp()

        self.cellulant_sms_calls = DeferredQueue()
        self.mock_mtech_sms = MockHttpServer(self.handle_request)
        yield self.mock_mtech_sms.start()

        self.valid_creds = {
            'mt_username': 'testuser',
            'mt_password': 'testpass',
        }
        self.config = {
            'transport_name': self.transport_name,
            'web_path': "foo",
            'web_port': 0,
            'outbound_url': self.mock_mtech_sms.url,
        }
        self.config.update(self.valid_creds)
        self.transport = yield self.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url()

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_mtech_sms.stop()
        yield super(TestMTechKenyaTransport, self).tearDown()

    def handle_request(self, request):
        if request.args.get('user') != [self.valid_creds['mt_username']]:
            request.setResponseCode(401)
        elif request.args.get('MSISDN') != ['2371234567']:
            request.setResponseCode(403)
        self.cellulant_sms_calls.put(request)
        return ''

    def mkurl(self, content, from_addr="2371234567", **kw):
        params = {
            'shortCode': '12345',
            'MSISDN': from_addr,
            'MESSAGE': content,
            'messageID': '1234567',
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
        response = yield http_request(url, '', method='POST')
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['to_addr'], "12345")
        self.assertEqual(msg['from_addr'], "2371234567")
        self.assertEqual(msg['content'], "hello")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    def test_handle_non_ascii_input(self):
        url = self.mkurl(u"öæł".encode("utf-8"))
        response = yield http_request(url, '', method='POST')
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
        response = yield http_request_full(url, '', method='POST')
        self.assertEqual(400, response.code)
        self.assertEqual(json.loads(response.delivered_body),
                         {'unexpected_parameter': ['foo']})

    @inlineCallbacks
    def test_outbound(self):
        msg = self.mkmsg_out(to_addr="2371234567")
        yield self.dispatch(msg)
        req = yield self.cellulant_sms_calls.get()
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'POST')
        self.assertEqual({
            'user': ['testuser'],
            'pass': ['testpass'],
            'messageID': [msg['message_id']],
            'shortCode': ['9292'],
            'MSISDN': ['2371234567'],
            'MESSAGE': ['hello world'],
        }, req.args)
        [ack] = yield self.wait_for_dispatched_events(1)
        self.assertEqual('ack', ack['event_type'])

    @inlineCallbacks
    def test_outbound_bad_creds(self):
        self.valid_creds['mt_username'] = 'other_user'
        msg = self.mkmsg_out(to_addr="2371234567")
        yield self.dispatch(msg)
        req = yield self.cellulant_sms_calls.get()
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'POST')
        self.assertEqual({
            'user': ['testuser'],
            'pass': ['testpass'],
            'messageID': [msg['message_id']],
            'shortCode': ['9292'],
            'MSISDN': ['2371234567'],
            'MESSAGE': ['hello world'],
        }, req.args)
        [nack] = yield self.wait_for_dispatched_events(1)
        self.assertEqual('nack', nack['event_type'])
        self.assertEqual('Invalid username or password', nack['nack_reason'])

    @inlineCallbacks
    def test_outbound_bad_msisdn(self):
        msg = self.mkmsg_out(to_addr="4471234567")
        yield self.dispatch(msg)
        req = yield self.cellulant_sms_calls.get()
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'POST')
        self.assertEqual({
            'user': ['testuser'],
            'pass': ['testpass'],
            'messageID': [msg['message_id']],
            'shortCode': ['9292'],
            'MSISDN': ['4471234567'],
            'MESSAGE': ['hello world'],
        }, req.args)
        [nack] = yield self.wait_for_dispatched_events(1)
        self.assertEqual('nack', nack['event_type'])
        self.assertEqual('Invalid mobile number', nack['nack_reason'])
