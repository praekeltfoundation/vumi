from django.test import TestCase
from django.core.urlresolvers import reverse
from django.contrib.auth.models import User
from time import time
from datetime import datetime, timedelta

from vumi.webapp.api.models import *
from vumi.webapp.api.tests.utils import APIClient, mock_sent_messages
from vumi.webapp.api.gateways.e_scape.backend import E_Scape

class E_ScapeSMSHandlerTestCase(TestCase):
    
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
        resp = self.client.post(reverse('api:e-scape:sms-receipt'))
        self.assertEquals(resp.status_code, 405)
        resp = self.client.get(reverse('api:e-scape:sms-receipt'))
        self.assertEquals(resp.status_code, 405)
    
    def test_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:e-scape:sms-send'), {
            'to_msisdn': '27123456789',
            'from_msisdn': '27123456789',
            'message': 'yebo',
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 1)
    
    def test_batch_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:e-scape:sms-send'), {
            'to_msisdn': ['27123456780','27123456781','27123456782'],
            'from_msisdn': '27123456789',
            'message': 'yebo'
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 3)
    
    def test_template_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:e-scape:sms-template-send'), {
            'to_msisdn': ['27123456780','27123456781','27123456782'],
            'template_first_name': ['Name 1', 'Name 2', 'Name 3'],
            'template_last_name': ['Surname 1', 'Surname 2', 'Surname 3'],
            'from_msisdn': '27123456789',
            'template': 'Hi {{first_name}} {{last_name}}',
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 3)
    
    def test_send_sms_response(self):
        """
        We get the following back from E-scape when we send them an SMS, make
        sure we can parse it correctly"""
        
        html = """
        <table class="widefat" border="0">
            <thead>
                <tr>
                    <th>from</th>
                    <th>to</th>
                    <th>smsc</th>
                    <th>status</th>
                    <th>text</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>+35566</td>
                    <td>+44778962937</td>
                    <td>ESC-P1Celtel</td>
                    <td>0: Accepted for delivery</td>
                    <td> http://www.mobi-fee.com/link/g.lnk?ID=135</td>
                </tr>
            </tbody>
            <tfoot>
                <tr>
                    <th>from</th>
                    <th>to</th>
                    <th>smsc</th>
                    <th>status</th>
                    <th>text</th>
                </tr>
            </tfoot>
        </table>
        """
        e = E_Scape('api_id')
        [response] = e.parse_response(html)
        self.assertEquals(response.sender, '+35566')
        self.assertEquals(response.recipient, '+44778962937')
        self.assertEquals(response.smsc, 'ESC-P1Celtel')
        self.assertEquals(response.status, ['0', 'Accepted for delivery'])
        self.assertEquals(response.text, ' http://www.mobi-fee.com/link/g.lnk?ID=135')
    
    def test_sms_receiving(self):
        self.assertEquals(ReceivedSMS.objects.count(), 0)
        resp = self.client.get(reverse('api:e-scape:sms-receive'), {
            'smsc': 'id of sms center',
            'svc': 'name of the service',
            's': 'originator',
            'r': 'recipient',
            'text': 'actual message'
        })
        self.assertEquals(resp.status_code, 200)
        # make sure we don't send any headers or content. If we do we'll 
        # automatically send a reply, currently that's not the 
        # desired behaviour.
        self.assertEquals(resp.get('X-Kannel-From', None), None)
        self.assertEquals(resp.get('X-Kannel-To', None), None)
        self.assertEquals(resp.get('X-Kannel-SMSC', None), None)
        self.assertEquals(resp.get('X-Kannel-Service', None), None)
        self.assertEquals(resp.content, '')
        
        self.assertEquals(ReceivedSMS.objects.count(), 1)
        sms = ReceivedSMS.objects.latest()
        import time
        # grmbl messy datetime comparisons
        self.assertAlmostEqual(
            time.mktime(sms.received_at.utctimetuple()),
            time.mktime(datetime.utcnow().utctimetuple()),
            places=1
        )
        self.assertEquals(sms.from_msisdn, 'originator')
        self.assertEquals(sms.to_msisdn, 'recipient')
        self.assertEquals(sms.message, 'actual message')
        self.assertEquals(sms.transport_name, 'E-scape')
        

class E_ScapeSentSMSStatusTestCase(TestCase):

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
        resp = self.client.get(reverse('api:e-scape:sms-status-list'), {
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
        resp = self.client.get(reverse('api:e-scape:sms-status', kwargs={
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
        resp = self.client.get(reverse('api:e-scape:sms-status-list'), {
            'id': sms_ids
        })
        from django.utils import simplejson
        json_smss = simplejson.loads(resp.content)
        json_sms_ids = map(lambda sms: int(sms['id']), json_smss)
        json_sms_ids.sort()
        self.assertEquals(sms_ids, json_sms_ids)
        self.assertEquals(resp.status_code, 200)


