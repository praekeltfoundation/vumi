from django.test import TestCase
from django.core.urlresolvers import reverse
from django.contrib.auth.models import User
from time import time
from datetime import datetime, timedelta

from vumi.webapp.api.models import *
from vumi.webapp.api.tests.utils import APIClient, mock_sent_messages

class OperaSMSHandlerTestCase(TestCase):
    
    fixtures = ['user_set']
    
    def setUp(self):
        self.client = APIClient()
        self.client.login(username='api', password='password')
        # create the user we need to be authorized
        self.user = User.objects.get(username='api')
    
    def test_sms_receipts(self):
        """
        Receipts received from opera should update the status
        """
        [sms] = mock_sent_messages(self.user, count=1,
                                    transport_name="Opera",
                                    transport_msg_id="001efc31")
        self.assertEquals(sms.transport_status, '')
        
        raw_xml_post = """
        <?xml version="1.0"?>
        <!DOCTYPE receipts>
        <receipts>
          <receipt>
            <msgid>26567958</msgid>
            <reference>001efc31</reference>
            <msisdn>+27123456789</msisdn>
            <status>D</status>
            <timestamp>20080831T15:59:24</timestamp>
            <billed>NO</billed>
          </receipt>
        </receipts>
        """
        
        resp = self.client.post(reverse('api:opera:sms-receipt'), 
                                raw_xml_post.strip(), content_type='text/xml')
        sms = SentSMS.objects.get(pk=sms.pk) # reload
        self.assertEquals(sms.transport_status, 'D')
        self.assertEquals(resp.status_code, 201)
    
    
    def test_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:opera:sms-send'), {
            'to_msisdn': '27123456789',
            'from_msisdn': '27123456789',
            'message': 'yebo',
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 1)
    
    def test_batch_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:opera:sms-send'), {
            'to_msisdn': ['27123456780','27123456781','27123456782'],
            'from_msisdn': '27123456789',
            'message': 'yebo'
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 3)
    
    def test_template_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:opera:sms-template-send'), {
            'to_msisdn': ['27123456780','27123456781','27123456782'],
            'template_first_name': ['Name 1', 'Name 2', 'Name 3'],
            'template_last_name': ['Surname 1', 'Surname 2', 'Surname 3'],
            'from_msisdn': '27123456789',
            'template': 'Hi {{first_name}} {{last_name}}',
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 3)
    
    def test_sms_receiving_with_text_plain_headers(self):
        """
        By eavesdropping we got the following log, this is what opera sends.
        DTD is available at https://dragon.sa.operatelecom.com/MEnable/Client/Extra/bspostevent-1_0_0.dtd
        
        
        POST /api/v1/sms/opera/receive.xml HTTP/1.1
        Content-Length: 1798
        Content-Type: text/plain; charset=utf-8
        Authorization: ...
        Host: ....
        """
        receive_sms_doc = """
        <?xml version="1.0"?>
        <!DOCTYPE bspostevent>
        <bspostevent>
          <field name="MOReference" type = "string">282341913</field>
          <field name="IsReceipt" type = "string">NO</field>
          <field name="RemoteNetwork" type = "string">mtn-za</field>
          <field name="BSDate-tomorrow" type = "string">20100605</field>
          <field name="BSDate-today" type = "string">20100604</field>
          <field name="ReceiveDate" type = "date">2010-06-04 15:51:25 +0000</field>
          <field name="Local" type = "string">*32323</field>
          <field name="ClientID" type = "string">4</field>
          <field name="ChannelID" type = "string">111</field>
          <field name="MessageID" type = "string">373736741</field>
          <field name="ReceiptStatus" type = "string"></field>
          <field name="Prefix" type = "string"></field>
          <field name="ClientName" type = "string">Praekelt</field>
          <field name="MobileDevice" type = "string"></field>
          <field name="BSDate-yesterday" type = "string">20100603</field>
          <field name="Remote" type = "string">+27831234567</field>
          <field name="State" type = "string">5</field>
          <field name="MobileNetwork" type = "string">mtn-za</field>
          <field name="MobileNumber" type = "string">+27831234567</field>
          <field name="Text" type = "string">Hello World</field>
          <field name="ServiceID" type = "string">20222</field>
          <field name="RegType" type = "string">1</field>
          <field name="NewSubscriber" type = "string">NO</field>
          <field name="Subscriber" type = "string">+27831234567</field>
          <field name="Parsed" type = "string"></field>
          <field name="ServiceName" type = "string">Prktl Vumi</field>
          <field name="BSDate-thisweek" type = "string">20100531</field>
          <field name="ServiceEndDate" type = "string">2010-06-30 07:47:00 +0200</field>
          <field name="Now" type = "date">2010-06-04 15:51:27 +0000</field>
        </bspostevent>
        """
        
        self.assertEquals(ReceivedSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:opera:sms-receive'), 
                                    receive_sms_doc.strip(),
                                    content_type='text/plain; charset=utf-8')
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(ReceivedSMS.objects.count(), 1)
        sms = ReceivedSMS.objects.latest()
        self.assertEquals(
            sms.received_at.strftime('%Y-%m-%d %H:%M:%S +0000'), 
            '2010-06-04 15:51:25 +0000'
        )
        self.assertEquals(sms.from_msisdn, '+27831234567')
        self.assertEquals(sms.to_msisdn, '*32323')
        self.assertEquals(sms.transport_name, 'Opera')
        

class OperaSentSMSStatusTestCase(TestCase):

    fixtures = ['user_set', 'opera_sentsms_set']

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
        resp = self.client.get(reverse('api:opera:sms-status-list'), {
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
        resp = self.client.get(reverse('api:opera:sms-status', kwargs={
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
        resp = self.client.get(reverse('api:opera:sms-status-list'), {
            'id': sms_ids
        })
        from django.utils import simplejson
        json_smss = simplejson.loads(resp.content)
        json_sms_ids = map(lambda sms: int(sms['id']), json_smss)
        json_sms_ids.sort()
        self.assertEquals(sms_ids, json_sms_ids)
        self.assertEquals(resp.status_code, 200)


