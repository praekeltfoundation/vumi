from django.test import TestCase
from django.core.urlresolvers import reverse
from django.contrib.auth.models import User
from time import time
from datetime import datetime, timedelta

from vumi.webapp.api.models import *
from vumi.webapp.api.tests.utils import APIClient, mock_sent_messages

class ClickatellSMSHandlerTestCase(TestCase):
    
    fixtures = ['user_set']
    
    def setUp(self):
        self.client = APIClient()
        self.client.login(username='api', password='password')
        # create the user we need to be authorized
        self.user = User.objects.get(username='api')
    
    def test_sms_receipts(self):
        """
        Receipts received from clickatell should update the status
        """
        [sms] = mock_sent_messages(self.user, count=1, 
                                    transport_name = 'Clickatell',
                                    transport_msg_id='a' * 32)
        self.assertEquals(sms.transport_status, '')
        resp = self.client.post(reverse('api:clickatell:sms-receipt'), {
            'apiMsgId': 'a' * 32,
            'cliMsgId': sms.pk,
            'status': 8, # OK
            'to': '27123456789',
            'from': '27123456789',
            'timestamp': int(time()),
            'charge': 0.3
        })
        self.assertEquals(resp.status_code, 201)
        sms = SentSMS.objects.get(pk=sms.pk) # reload
        self.assertEquals(sms.transport_status, '8')
    
    def test_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:clickatell:sms-send'), {
            'to_msisdn': '27123456789',
            'from_msisdn': '27123456789',
            'message': 'yebo',
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 1)
    
    def test_batch_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:clickatell:sms-send'), {
            'to_msisdn': ['27123456780','27123456781','27123456782'],
            'from_msisdn': '27123456789',
            'message': 'yebo'
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 3)
    
    def test_template_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:clickatell:sms-template-send'), {
            'to_msisdn': ['27123456780','27123456781','27123456782'],
            'template_first_name': ['Name 1', 'Name 2', 'Name 3'],
            'template_last_name': ['Surname 1', 'Surname 2', 'Surname 3'],
            'from_msisdn': '27123456789',
            'template': 'Hi {{first_name}} {{last_name}}',
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 3)
    
    def test_sms_receiving(self):
        self.assertEquals(ReceivedSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:clickatell:sms-receive'), {
            'to': '27123456789',
            'from': '27123456789',
            'moMsgId': 'a' * 12,
            'api_id': 'b' * 12,
            'text': 'hello world',
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S") # MySQL format
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(ReceivedSMS.objects.count(), 1)

class ClickatellSentSMSStatusTestCase(TestCase):
    
    fixtures = ['user_set', 'clickatell_sentsms_set']
    
    def setUp(self):
        self.client = APIClient()
        self.client.login('api', 'password')
        self.user = User.objects.get(username='api')
    
    def tearDown(self):
        pass
    
    def test_sms_status_list_since(self):
        """
        Sorry this test needs some explanation. In the SentSMS model I'm using
        Django's `auto_now` and `auto_now_add` options to automatically 
        timestamp the `created_at` and `updated_at` values. Downside of this
        is that you now no longer can set these values from the code. The 
        fixture has a `SentSMS` entry from 2009. I'm using that date to make
        sure the `since` parameter gives us back that entry as well instead
        of only the most recent 50 ones (which I create manually in this test).
        """
        january_2009 = datetime(2009,01,01,0,0,0)
        new_smss = mock_sent_messages(self.user, count=50)
        resp = self.client.get(reverse('api:clickatell:sms-status-list'), {
            'since': january_2009
        })
        from django.utils import simplejson
        data = simplejson.loads(resp.content)
        self.assertEquals(len(data), 51) # respects the `since` parameter
                                        # overriding the `limit` parameter.
                                        # On top of the 50 newly created
                                        # entries it should also return the 
                                        # 51st entry which is one from 2009
                                        # in the fixtures file.
        self.assertEquals(resp.status_code, 200)
    
    def test_single_status(self):
        sent_sms = SentSMS.objects.latest('created_at')
        resp = self.client.get(reverse('api:clickatell:sms-status', kwargs={
            "sms_id": sent_sms.pk
        }))
        from django.utils import simplejson
        json_sms = simplejson.loads(resp.content)
        self.assertEquals(json_sms['to_msisdn'], sent_sms.to_msisdn)
        self.assertEquals(json_sms['from_msisdn'], sent_sms.from_msisdn)
        self.assertEquals(json_sms['message'], sent_sms.message)
        self.assertEquals(json_sms['transport_status'], sent_sms.transport_status)
        self.assertEquals(resp.status_code, 200)
    
    def test_multiple_statuses(self):
        smss = mock_sent_messages(self.user, count=10)
        sms_ids = map(lambda sms: int(sms.pk), smss)
        sms_ids.sort()
        resp = self.client.get(reverse('api:clickatell:sms-status-list'), {
            'id': sms_ids
        })
        from django.utils import simplejson
        json_smss = simplejson.loads(resp.content)
        json_sms_ids = map(lambda sms: int(sms['id']), json_smss)
        json_sms_ids.sort()
        self.assertEquals(sms_ids, json_sms_ids)
        self.assertEquals(resp.status_code, 200)
    

