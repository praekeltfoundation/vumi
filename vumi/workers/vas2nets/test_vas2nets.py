from twisted.trial import unittest
from twisted.python import log, failure
from twisted.internet import defer, reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web.test.test_web import DummyRequest
from twisted.web import http

from vumi.tests.utils import TestPublisher
from vumi.message import Message

from StringIO import StringIO
from urllib import urlencode
from uuid import uuid1
from datetime import datetime
from .transport import ReceiveSMSResource, DeliveryReceiptResource

def create_request(dictionary={}, path='/', method='POST'):
    """Creates a dummy Vas2Nets request for testing our 
    resources with"""
    request = DummyRequest(path)
    request.method = method
    args = {
        'messageid': str(uuid1()),
        'time': datetime.utcnow().strftime('%Y.%m.%d %H:%M:%S'),
        'sender': '0041791234567',
        'destination': '9292',
        'provider': '22801',
        'keyword': '',
        'header': '',
        'text': ''
    }
    args.update(dictionary)
    request.args = args
    return request

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
            'messageid': '1',
            'time': self.today.strftime('%Y.%m.%d %H:%M:%S'),
            'text': 'hello world'
        })
        response = resource.render(request)
        self.assertEquals(response, '')
        self.assertEquals(request.outgoingHeaders['content-type'], 'text/plain')
        self.assertEquals(request.responseCode, http.OK)
        self.assertEquals(self.publisher.queue, [(Message(**{
            'transport_message_id': '1',
            'transport_timestamp': self.today.strftime('%Y.%m.%d %H:%M:%S'),
            'to_msisdn': '9292',
            'from_msisdn': '0041791234567',
            'message': 'hello world'
        }), {'routing_key': 'sms.inbound.vas2nets.9292'})])
    
    def test_delivery_receipt(self):
        request = create_request({
            'smsid': '1',
            'time': self.today.strftime('%Y.%m.%d %H:%M:%S'),
            'status': '2',
            'text': 'Message delivered to MSISDN.'
        })
        
        resource = DeliveryReceiptResource(self.config, self.publisher)
        response = resource.render(request)
        self.assertEquals(response, '')
        self.assertEquals(request.outgoingHeaders['content-type'], 'text/plain')
        self.assertEquals(request.responseCode, http.OK)
        self.assertEquals(self.publisher.queue, [(Message(**{
            'transport_message_id': '1',
            'transport_status': '2',
            'transport_timestamp': self.today.strftime('%Y.%m.%d %H:%M:%S'),
            'transport_status_message': 'Message delivered to MSISDN.'
        }), {'routing_key': 'sms.receipt.vas2nets'})])
