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
from .transport import Vas2NetsResource

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
        self.resource = Vas2NetsResource(self.config, self.publisher)
    
    def tearDown(self):
        pass
    
    def test_receive_sms(self):
        request = create_request({
            'messageid': '1',
            'time': datetime.utcnow().date().strftime('%Y.%m.%d %H:%M:%S'),
            'text': 'hello world'
        })
        response = self.resource.render(request)
        self.assertEquals(request.responseCode, http.OK)
        self.assertEquals(self.publisher.queue, [(Message(**{
            'transport_message_id': '1',
            'to_msisdn': '9292',
            'from_msisdn': '0041791234567',
            'message': 'hello world'
        }), {'routing_key': 'sms.inbound.vas2nets.9292'})])
