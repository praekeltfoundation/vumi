# encoding: utf-8
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
from .transport import (ReceiveSMSResource, DeliveryReceiptResource, 
                        validate_characters, Vas2NetsEncodingError)
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
    
    def test_send_sms(self):
        """no clue yet how I'm going to test this."""
        pass