from django.test import TestCase
from django.core.urlresolvers import reverse
from django.contrib.auth.models import User
from time import time
from datetime import datetime

from vumi.webapp.api.models import SentSMS, ReceivedSMS
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
                                    transport_name='Clickatell',
                                    transport_msg_id='a' * 32)
        self.assertEquals(sms.transport_status, '')
        resp = self.client.post(reverse('api:clickatell:sms-receipt'), {
            'apiMsgId': 'a' * 32,
            'cliMsgId': sms.pk,
            'status': 8,  # OK
            'to': '27123456789',
            'from': '27123456789',
            'timestamp': int(time()),
            'charge': 0.3
        })
        self.assertEquals(resp.status_code, 201)
        sms = SentSMS.objects.get(pk=sms.pk)  # reload
        self.assertEquals(sms.transport_status, '8')

    def test_sms_receiving(self):
        self.assertEquals(ReceivedSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:clickatell:sms-receive'), {
            'to': '27123456789',
            'from': '27123456789',
            'moMsgId': 'a' * 12,
            'api_id': 'b' * 12,
            'text': 'hello world',
            # MySQL format
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(ReceivedSMS.objects.count(), 1)
