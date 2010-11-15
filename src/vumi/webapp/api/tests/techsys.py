from django.test import TestCase
from django.core.urlresolvers import reverse
from django.contrib.auth.models import User
from time import time
from datetime import datetime, timedelta

from vumi.webapp.api.models import *
from vumi.webapp.api.tests.utils import APIClient, mock_sent_messages
from vumi.webapp.api.gateways.techsys.backend import Techsys

class TechsysSMSHandlerTestCase(TestCase):
    
    fixtures = ['user_set']
    
    def setUp(self):
        self.client = APIClient()
        self.client.login(username='api', password='password')
        # create the user we need to be authorized
        self.user = User.objects.get(username='api')
    
    def test_sms_receipts(self):
        """
        E-Scape currently doesn't send us sms receipts, shouldn't respond
        """
        resp = self.client.post(reverse('api:techsys:sms-receipt'))
        self.assertEquals(resp.status_code, 405)
        resp = self.client.get(reverse('api:techsys:sms-receipt'))
        self.assertEquals(resp.status_code, 405)
    
    def test_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:techsys:sms-send'), {
            'to_msisdn': '27123456789',
            'from_msisdn': '27123456789',
            'message': 'yebo',
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 1)
    
    def test_batch_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:techsys:sms-send'), {
            'to_msisdn': ['27123456780','27123456781','27123456782'],
            'from_msisdn': '27123456789',
            'message': 'yebo'
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 3)
    
    def test_template_sms_sending(self):
        # not implemented
        resp = self.client.post(reverse('api:techsys:sms-template-send'))
        self.assertEquals(resp.status_code, 405)
    
    def test_send_sms_response(self):
        """
        We get the following back from E-scape when we send them an SMS, make
        sure we can parse it correctly"""
        
        good_response = """PS Bind: Message queued successfully"""
        bad_response = """Anything else really"""
        e = E_Scape("url", "bind")
        response = e.parse_response(good_response)
        self.assertRaises(Exception, e.parse_response, bad_response)
    
    def test_sms_receiving(self):
        # not implemented
        resp = self.client.post(reverse('api:techsys:sms-receive'))
        self.assertEquals(resp.status_code, 405)
        


