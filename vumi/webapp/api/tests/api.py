from django.test import TestCase
from django.core.urlresolvers import reverse
from django.contrib.auth.models import User
from django.conf import settings
import os.path
import json
from datetime import datetime

from vumi.webapp.api.models import (SentSMS, SentSMSBatch, URLCallback,
                                    Transport)
from vumi.webapp.api.tests.utils import APIClient, mock_sent_messages


class BaseSMSHandlerTestCase(TestCase):

    fixtures = ['user_set']

    def setUp(self):
        self.client = APIClient()
        self.client.login(username='api', password='password')
        # get the user
        self.user = User.objects.get(username='api')
        # make sure the transport's set
        profile = self.user.get_profile()
        profile.transport = Transport.objects.create(name='Clickatell')
        profile.save()

    def test_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:sms-send'), {
            'to_msisdn': '27123456789',
            'from_msisdn': '27123456789',
            'message': 'yebo',
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 1)
        self.assertEquals(SentSMSBatch.objects.count(), 1)
        batch = SentSMSBatch.objects.all()[0]
        self.assertEquals(batch.sentsms_set.count(), 1)

    def test_batch_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:sms-send'), {
            'to_msisdn': ['27123456780', '27123456781', '27123456782'],
            'from_msisdn': '27123456789',
            'message': 'yebo',
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 3)
        self.assertEquals(SentSMSBatch.objects.count(), 1)
        batch = SentSMSBatch.objects.all()[0]
        self.assertEquals(batch.sentsms_set.count(), 3)

    def test_template_sms_sending(self):
        self.assertEquals(SentSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:sms-template-send'), {
            'to_msisdn': ['27123456780', '27123456781', '27123456782'],
            'template_first_name': ['Name 1', 'Name 2', 'Name 3'],
            'template_last_name': ['Surname 1', 'Surname 2', 'Surname 3'],
            'from_msisdn': '27123456789',
            'template': 'Hi {{first_name}} {{last_name}}',
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(SentSMS.objects.count(), 3)
        self.assertEquals(SentSMSBatch.objects.count(), 1)
        batch = SentSMSBatch.objects.all()[0]
        self.assertEquals(batch.sentsms_set.count(), 3)


class BaseSentSMSStatusTestCase(TestCase):

    fixtures = ['user_set', 'base_sentsms_set']

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
        january_2009 = datetime(2009, 01, 01, 0, 0, 0)
        # write some new SMSs
        mock_sent_messages(self.user, count=50)
        resp = self.client.get(reverse('api:sms-status-list'), {
            'since': january_2009,
        })
        from django.utils import simplejson
        data = simplejson.loads(resp.content)
        self.assertEquals(len(data), 51)  # respects the `since` parameter
                                          # overriding the `limit` parameter.
                                          # On top of the 50 newly created
                                          # entries it should also return the
                                          # 51st entry which is one from 2009
                                          # in the fixtures file.
        self.assertEquals(resp.status_code, 200)

    def test_single_status(self):
        sent_sms = SentSMS.objects.latest('created_at')
        resp = self.client.get(reverse('api:sms-status', kwargs={
            "sms_id": sent_sms.pk,
        }))
        from django.utils import simplejson
        json_sms = simplejson.loads(resp.content)
        self.assertEquals(json_sms['to_msisdn'], sent_sms.to_msisdn)
        self.assertEquals(json_sms['from_msisdn'], sent_sms.from_msisdn)
        self.assertEquals(json_sms['message'], sent_sms.message)
        self.assertEquals(json_sms['transport_status'],
                          sent_sms.transport_status)
        self.assertEquals(resp.status_code, 200)

    def test_multiple_statuses(self):
        smss = mock_sent_messages(self.user, count=10)
        sms_ids = map(lambda sms: int(sms.pk), smss)
        sms_ids.sort()
        resp = self.client.get(reverse('api:sms-status-list'), {
            'id': sms_ids,
        })
        from django.utils import simplejson
        json_smss = simplejson.loads(resp.content)
        json_sms_ids = map(lambda sms: int(sms['id']), json_smss)
        json_sms_ids.sort()
        self.assertEquals(sms_ids, json_sms_ids)
        self.assertEquals(resp.status_code, 200)


class ConversationHandlerTestCase(TestCase):

    fixtures = ['user_set']

    def setUp(self):
        self.client = APIClient()
        self.client.login(username='api', password='password')
        # create the user we need to be authorized
        self.user = User.objects.get(username='api')
        # load the yaml data
        fp = open(os.path.join(settings.APP_ROOT, 'webapp', 'api',
                               'test_data', 'devquiz.yaml'), 'r')
        self.yaml_conversation = ''.join(fp.readlines())

    def tearDown(self):
        pass

    def test_creation_of_conversation(self):
        """
        Conversations should be able to be created by POSTing to the api
        """
        resp = self.client.post(reverse('api:conversation'),
                                self.yaml_conversation,
                                content_type="application/x-yaml")
        self.assertContains(resp, 'Created', status_code=201)
        resp = self.client.get(reverse('api:conversation'))
        self.assertEquals(resp.status_code, 405)


class URLCallbackHandlerTestCase(TestCase):

    fixtures = ['user_set']

    def setUp(self):
        self.client = APIClient()
        self.client.login(username='api', password='password')
        # create the user we need to be authorized
        self.user = User.objects.get(username='api')

    def tearDown(self):
        pass

    def test_setting_callback_url(self):
        self.assertEquals(URLCallback.objects.count(), 0)
        resp = self.client.post(reverse('api:url-callbacks-list'), {
            'name': 'sms_receipt',
            'url': 'http://localhost/url/sms/receipt',
        })
        resp = self.client.post(reverse('api:url-callbacks-list'), {
            'name': 'sms_received',
            'url': 'http://localhost/url/sms/received',
        })
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(URLCallback.objects.count(), 2)
        resp = self.client.post(reverse('api:clickatell:sms-receive'), {
            'to': '27123456789',
            'from': '27123456789',
            'moMsgId': 'a' * 12,
            'api_id': 'b' * 12,
            'text': 'hello world',
            # MySQL format:
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        # this should show up in the testing log because pycurl can't
        # connect to the given host for the callback

    def test_setting_multiple_callback_urls(self):
        self.assertEquals(URLCallback.objects.count(), 0)
        for name, urls in [
            ('sms_received', [
                'http://localhost/url/sms/received/1',
                'http://localhost/url/sms/received/2',
            ]),
            ('sms_receipt', [
                'http://localhost/url/sms/receipt/1',
                'http://localhost/url/sms/receipt/2',
            ])]:
            for url in urls:
                resp = self.client.post(reverse('api:url-callbacks-list'), {
                    'name': name,
                    'url': url,
                })
                self.assertEquals(resp.status_code, 200)

        self.assertEquals(URLCallback.objects.count(), 4)
        resp = self.client.post(reverse('api:clickatell:sms-receive'), {
            'to': '27123456789',
            'from': '27123456789',
            'moMsgId': 'a' * 12,
            'api_id': 'b' * 12,
            'text': 'hello world',
            # MySQL format
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    def test_updating_callback_urls(self):
        self.assertEquals(URLCallback.objects.count(), 0)
        resp = self.client.post(reverse('api:url-callbacks-list'), {
            'name': 'sms_receipt',
            'url': 'http://localhost/url/sms/receipt',
        })
        self.assertEquals(URLCallback.objects.count(), 1)
        data = json.loads(resp.content)
        resp = self.client.put(reverse('api:url-callback', kwargs={
                'callback_id': data['id'],
            }), {
            'url': 'http://localhost/url/sms/receipt1'
        })
        updated_callback = URLCallback.objects.latest('updated_at')
        self.assertEquals(updated_callback.url,
                          'http://localhost/url/sms/receipt1')

    def test_deleting_callback_urls(self):
        self.assertEquals(URLCallback.objects.count(), 0)
        resp = self.client.post(reverse('api:url-callbacks-list'), {
            'name': 'sms_receipt',
            'url': 'http://localhost/url/sms/receipt',
        })
        data = json.loads(resp.content)
        self.assertEquals(URLCallback.objects.count(), 1)
        resp = self.client.delete(reverse('api:url-callback', kwargs={
            'callback_id': data['id'],
        }))
        self.assertEquals(resp.status_code, 204)
        self.assertEquals(resp.content, '')
        self.assertEquals(URLCallback.objects.count(), 0)
