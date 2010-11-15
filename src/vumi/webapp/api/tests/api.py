from django.test import TestCase
from django.core.urlresolvers import reverse
from django.contrib.auth.models import User
from time import time
import json
from datetime import datetime, timedelta

from vumi.webapp.api.signals import *
from vumi.webapp.api.models import *
from vumi.webapp.api.tests.utils import APIClient

import logging
LOG_FILENAME = 'logs/vumi.testing.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

class PyCurlBugTestCase(TestCase):
    
    def test_callback_unicode_warning(self):
        from vumi.webapp.api.utils import callback
        self.assertRaises(RuntimeError, callback, 'http://localhost/', (
            (u'key', 'value'), # unicode keys or values aren't allowed
        ))
        self.assertRaises(RuntimeError, callback, 'http://localhost/', (
            ('key', u'value'), # unicode keys or values aren't allowed
        ))

class ConversationHandlerTestCase(TestCase):
    
    fixtures = ['user_set']
    
    def setUp(self):
        self.client = APIClient()
        self.client.login(username='api', password='password')
        # create the user we need to be authorized
        self.user = User.objects.get(username='api')
        # load the yaml data
        fp = open('src/vumi/webapp/api/test_data/devquiz.yaml', 'r')
        self.yaml_conversation = ''.join(fp.readlines())
    
    def tearDown(self):
        pass
    
    def test_creation_of_conversation(self):
        """
        Conversations should be able to be created by POSTing to the api
        """
        resp = self.client.post(reverse('api:conversation'), self.yaml_conversation,
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
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S") # MySQL format
        })
        # this should show up in the testing log because pycurl can't
        # connect to the given host for the callback
    
    def test_setting_multiple_callback_urls(self):
        self.assertEquals(URLCallback.objects.count(), 0)
        for name, urls in [
            ('sms_received', [
                'http://localhost/url/sms/received/1',
                'http://localhost/url/sms/received/2'
            ]),
            ('sms_receipt', [
                'http://localhost/url/sms/receipt/1',
                'http://localhost/url/sms/receipt/2',
            ])]:
            for url in urls:
                resp = self.client.post(reverse('api:url-callbacks-list'), {
                    'name': name,
                    'url': url
                })
                self.assertEquals(resp.status_code, 200)
        
        self.assertEquals(URLCallback.objects.count(), 4)
        resp = self.client.post(reverse('api:clickatell:sms-receive'), {
            'to': '27123456789',
            'from': '27123456789',
            'moMsgId': 'a' * 12,
            'api_id': 'b' * 12,
            'text': 'hello world',
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S") # MySQL format
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
                'callback_id': data['id']
            }), {
            'url': 'http://localhost/url/sms/receipt1'
        })
        updated_callback = URLCallback.objects.latest('updated_at')
        self.assertEquals(updated_callback.url, 'http://localhost/url/sms/receipt1')
        
    
    def test_deleting_callback_urls(self):
        self.assertEquals(URLCallback.objects.count(), 0)
        resp = self.client.post(reverse('api:url-callbacks-list'), {
            'name': 'sms_receipt',
            'url': 'http://localhost/url/sms/receipt',
        })
        data = json.loads(resp.content)
        self.assertEquals(URLCallback.objects.count(), 1)
        resp = self.client.delete(reverse('api:url-callback', kwargs={
            'callback_id': data['id']
        }))
        self.assertEquals(resp.status_code, 204)
        self.assertEquals(resp.content, '')
        self.assertEquals(URLCallback.objects.count(), 0)