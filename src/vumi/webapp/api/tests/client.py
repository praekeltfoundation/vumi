from django.test import TestCase
from django.test.client import Client as HTTPClient
from vumi.webapp.api.client import Client
import base64

class APIClientTestCase(TestCase):
    
    def setUp(self):
        """Setup the client with HTTP Basic authentication"""
        auth_creds = base64.encodestring('vumi:vumi').strip()
        self.http_client = HTTPClient(HTTP_AUTHORIZATION='Basic %s' % auth_creds)
        self.client = Client(connection=self.http_client)
    
    def teardown(self):
        pass
    
    def test_send_sms(self):
        resp = self.client.send_sms(
            to_msisdn='27123456789',
            from_msisdn='27123456789',
            message='hello world')
        self.assertTrue(isinstance(resp, dict))
    