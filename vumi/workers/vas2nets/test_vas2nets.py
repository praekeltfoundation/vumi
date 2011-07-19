# encoding: utf-8
from twisted.web import http
from twisted.web.resource import Resource
from twisted.trial import unittest
from twisted.python import log, failure
from twisted.internet import defer, reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web.test.test_web import DummyRequest
from twisted.web import http

from vumi.tests.utils import TestPublisher, TestWorker, TestQueue
from vumi.message import Message

from StringIO import StringIO
from urllib import urlencode
from uuid import uuid1
from datetime import datetime
from .transport import (ReceiveSMSResource, DeliveryReceiptResource, 
                        Vas2NetsTransport, validate_characters, 
                        Vas2NetsEncodingError, Vas2NetsTransportError,
                        normalize_outbound_msisdn)
import string

def create_request(dictionary={}, path='/', method='POST'):
    """Creates a dummy Vas2Nets request for testing our 
    resources with"""
    request = DummyRequest(path)
    request.method = method
    args = {
        'messageid': [str(uuid1())],
        'time': [datetime.utcnow().strftime('%Y.%m.%d %H:%M:%S')],
        'sender': ['0041791234567'],
        'destination': ['9292'],
        'provider': ['provider'],
        'keyword': [''],
        'header': [''],
        'text': [''],
        'keyword': [''],
    }
    args.update(dictionary)
    request.args = args
    return request

class TestResource(Resource):
    isLeaf = True
    def __init__(self, message_id, message):
        self.message_id = message_id
        self.message = message
    
    def render_POST(self, request):
        log.msg(request.content.read())
        request.setResponseCode(http.OK)
        required_fields = [
            'username', 'password', 'call-number', 'origin', 'text',
            'messageid', 'provider', 'tariff', 'owner', 'service',
            'subservice'
        ]
        log.msg('request.args', request.args)
        for key in required_fields:
            assert key in request.args
        
        if self.message_id:
            request.setHeader('X-VAS2Nets-SmsId', self.message_id)
        return self.message

class Vas2NetsTestWorker(TestWorker):
    
    def __init__(self, path, port, message_id, message, queue):
        TestWorker.__init__(self, queue)
        self.path = path
        self.port = port
        self.message_id = message_id
        self.message = message
    
    @inlineCallbacks
    def startWorker(self):
        self.receipt_resource = yield self.start_web_resources(
            [
                (TestResource(self.message_id, self.message), self.path),
            ],
            self.port
        )
    
    def stopWorker(self):
        self.receipt_resource.stopListening()

class TestVas2NetsTransport(Vas2NetsTransport):
    def __init__(self, config):
        self.publisher = TestPublisher()
        self.config = config
    

class Vas2NetsTransportTestCase(unittest.TestCase):
    
    def setUp(self):
        self.config = {
            'transport_name': 'vas2nets'
        }
        self.publisher = TestPublisher()
        self.today = datetime.utcnow().date()
    
    def tearDown(self):
        pass
    
    def test_receive_sms(self):
        resource = ReceiveSMSResource(self.config, self.publisher)
        request = create_request({
            'messageid': ['1'],
            'time': [self.today.strftime('%Y.%m.%d %H:%M:%S')],
            'text': ['hello world']
        })
        response = resource.render(request)
        self.assertEquals(response, '')
        self.assertEquals(request.outgoingHeaders['content-type'], 'text/plain')
        self.assertEquals(request.responseCode, http.OK)
        msg = Message(**{
            'transport_message_id': '1',
            'transport_timestamp': self.today.strftime('%Y-%m-%dT%H:%M:%S'),
            'transport_network_id': 'provider',
            'transport_keyword': '',
            'to_msisdn': '9292',
            'from_msisdn': '+41791234567',
            'message': 'hello world'
        })
        
        self.assertEquals(self.publisher.queue, [(msg, {'routing_key': 'sms.inbound.vas2nets.9292'})])
    
    def test_delivery_receipt(self):
        request = create_request({
            'smsid': ['1'],
            'messageid': ['internal id'],
            'sender': ['+41791234567'],
            'time': [self.today.strftime('%Y.%m.%d %H:%M:%S')],
            'status': ['2'],
            'text': ['Message delivered to MSISDN.']
        })
        
        resource = DeliveryReceiptResource(self.config, self.publisher)
        response = resource.render(request)
        self.assertEquals(response, '')
        self.assertEquals(request.outgoingHeaders['content-type'], 'text/plain')
        self.assertEquals(request.responseCode, http.OK)
        msg = Message(**{
            'transport_message_id': '1',
            'transport_status': '2',
            'transport_network_id': 'provider',
            'to_msisdn': '+41791234567',
            'id': 'internal id',
            'transport_timestamp': self.today.strftime('%Y-%m-%dT%H:%M:%S'),
            'transport_status_message': 'Message delivered to MSISDN.'
        })
        self.assertEquals(self.publisher.queue, [(msg, 
            {'routing_key': 'sms.receipt.vas2nets'})])
    
    def test_validate_characters(self):
        self.assertRaises(Vas2NetsEncodingError, validate_characters, 
                            u"ïøéå¬∆˚")
        self.assertTrue(validate_characters(string.ascii_lowercase))
        self.assertTrue(validate_characters(string.ascii_uppercase))
        self.assertTrue(validate_characters('0123456789'))
        self.assertTrue(validate_characters(u'äöü ÄÖÜ àùò ìèé §Ññ £$@'))
        self.assertTrue(validate_characters(u'/?!#%&()*+,-:;<=>.'))
        self.assertTrue(validate_characters(u'testing\ncarriage\rreturns'))
    
    @inlineCallbacks
    def test_send_sms_success(self):
        """no clue yet how I'm going to test this."""
        path = '/api/v1/sms/vas2nets/receive/'
        port = 9999
        mocked_message_id = str(uuid1())
        mocked_message = "Result_code: 00, Message OK"
        stubbed_worker = Vas2NetsTestWorker(path, port, mocked_message_id,
                                            mocked_message, TestQueue([]))
        yield stubbed_worker.startWorker()
        
        transport = TestVas2NetsTransport({
            'transport_name': 'vas2nets',
            'url': 'http://localhost:%s%s' % (port, path),
            'username': 'username',
            'password': 'password',
            'owner': 'owner',
            'service': 'service',
            'subservice': 'subservice'
        })
        resp = yield transport.handle_outbound_message(Message(**{
            'to_msisdn': '+27761234567',
            'from_msisdn': '9292',
            'id': '1',
            'reply_to': '',
            'transport_network_id': 'network-id',
            'message': 'hello world'
        }))
        
        msg, kwargs = transport.publisher.queue.pop()
        self.assertEquals(msg.payload, {
            'id': '1',
            'transport_message_id': mocked_message_id
        })
        self.assertEquals(kwargs, {
            'routing_key': 'sms.ack.vas2nets'
        })
        
        stubbed_worker.stopWorker()
        
    @inlineCallbacks
    def test_send_sms_fail(self):
        """no clue yet how I'm going to test this."""
        path = '/api/v1/sms/vas2nets/receive/'
        port = 9999
        mocked_message_id = False
        mocked_message = "Result_code: 04, Internal system error occurred " \
                            "while processing message"
        stubbed_worker = Vas2NetsTestWorker(path, port, mocked_message_id,
                                            mocked_message, TestQueue([]))
        yield stubbed_worker.startWorker()

        transport = TestVas2NetsTransport({
            'transport_name': 'vas2nets',
            'url': 'http://localhost:%s%s' % (port, path),
            'username': 'username',
            'password': 'password',
            'owner': 'owner',
            'service': 'service',
            'subservice': 'subservice'
        })
        
        deferred = transport.handle_outbound_message(Message(**{
            'to_msisdn': '+27761234567',
            'from_msisdn': '9292',
            'id': '1',
            'reply_to': '',
            'transport_network_id': 'network-id',
            'message': 'hello world'
        }))
        self.assertFailure(deferred, Vas2NetsTransportError)
        yield deferred
        self.assertEquals([], transport.publisher.queue)
        stubbed_worker.stopWorker()
    
    def test_normalize_outbound_msisdn(self):
        self.assertEquals(normalize_outbound_msisdn('+27761234567'), '0027761234567')
