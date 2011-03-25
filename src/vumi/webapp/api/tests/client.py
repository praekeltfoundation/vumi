from django.test import TestCase
from django.test.client import Client as HTTPClient
from django.contrib.auth.models import User
from vumi.webapp.api.models import Transport
from vumi.webapp.api.client import Client
import base64
from datetime import datetime

class APIClientTestCase(TestCase):
    
    def setUp(self):
        """Setup the client with HTTP Basic authentication"""
        
        # create vumi test user
        user, created = User.objects.get_or_create(username='vumi')
        user.set_password('vumi')
        user.save()
        
        # create transport & attach to profile
        transport, created = Transport.objects.get_or_create(name='Test Transport')
        profile = user.get_profile()
        profile.transport = transport
        profile.save()
        
        
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
        self.assertTrue(len(resp), 1)
        self.assertTrue(isinstance(resp, list))
        sms_resp = resp[0]
        self.assertEquals(sms_resp.to_msisdn, '27123456789')
        self.assertEquals(sms_resp.from_msisdn, '27123456789')
        self.assertEquals(sms_resp.message, 'hello world')
        self.assertTrue(isinstance(sms_resp.created_at, datetime))
        self.assertTrue(isinstance(sms_resp.updated_at, datetime))
        self.assertTrue(isinstance(sms_resp.id, int))
    
    def test_send_multiple_sms(self):
        resp = self.client.send_sms(
            to_msisdns = ['27123456788', '27123456789'],
            from_msisdn = '27123456789',
            message = 'hello world')
        self.assertTrue(len(resp), 2)
        self.assertTrue(isinstance(resp, list))
        self.assertEquals(
            sorted(['27123456788', '27123456789']),
            sorted([o.to_msisdn for o in resp])
        )
        self.assertTrue(all(o.from_msisdn == '27123456789' for o in resp))
        self.assertTrue(all(o.message == 'hello world' for o in resp))
    
    def test_send_template_sms(self):
        resp = self.client.send_template_sms(
            template = 'Hello {{ name }} {{ surname }}',
            from_msisdn = '27123456789',
            to_msisdns = {
                '27123456788': {
                    'name': 'Foo',
                    'surname': 'Bar'
                },
                '27123456789': {
                    'name': 'Boo',
                    'surname': 'Far'
                }
            })
        self.assertTrue(len(resp), 2)
        self.assertTrue(isinstance(resp, list))
        self.assertEquals(
            set(['27123456788', '27123456789']),
            set([o.to_msisdn for o in resp])
        )
        self.assertTrue(all(o.from_msisdn == '27123456789' for o in resp))
        self.assertEquals(set(o.message for o in resp), 
                            set([u'Hello Foo Bar', u'Hello Boo Far']))
        
    