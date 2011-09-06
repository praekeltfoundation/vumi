from django.test import TestCase
from django.test.client import Client as HTTPClient
from django.contrib.auth.models import User
from vumi.webapp.api.models import Transport, SentSMS, SentSMSBatch
from vumi.webapp.api.client import Client
import base64
from datetime import datetime, timedelta


class APIClientTestCase(TestCase):

    def setUp(self):
        """Setup the client with HTTP Basic authentication"""

        # create vumi test user
        user, created = User.objects.get_or_create(username='vumi')
        user.set_password('vumi')
        user.save()

        # create transport & attach to profile
        transport, created = Transport.objects.get_or_create(
            name='Test Transport')
        profile = user.get_profile()
        profile.transport = transport
        profile.save()

        auth_creds = base64.encodestring('vumi:vumi').strip()
        self.http_client = HTTPClient(HTTP_AUTHORIZATION='Basic %s' %
                                      (auth_creds,))
        self.client = Client('vumi', 'vumi', connection=self.http_client)

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
            to_msisdn=['27123456788', '27123456789'],
            from_msisdn='27123456789',
            message='hello world')
        self.assertTrue(len(resp), 2)
        self.assertTrue(isinstance(resp, list))
        self.assertEquals(
            sorted(['27123456788', '27123456789']),
            sorted([o.to_msisdn for o in resp]),
        )
        self.assertTrue(all(o.from_msisdn == '27123456789' for o in resp))
        self.assertTrue(all(o.message == 'hello world' for o in resp))

    def test_send_template_sms(self):
        resp = self.client.send_template_sms(
            template='Hello {{ name }} {{ surname }}',
            from_msisdn='27123456789',
            to_msisdns={
                '27123456788': {
                    'name': 'Foo',
                    'surname': 'Bar',
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
            set([o.to_msisdn for o in resp]),
        )
        self.assertTrue(all(o.from_msisdn == '27123456789' for o in resp))
        self.assertEquals(set(o.message for o in resp),
                            set([u'Hello Foo Bar', u'Hello Boo Far']))

    def test_send_template_sms_batch(self):
        """The order of arguments should be kept"""
        resp = self.client.send_template_sms(
            template='{{name}} {{number}} {{transport_name}}',
            from_msisdn='0',
            to_msisdns={
                '1': {
                    'name': 'David',
                    'number': 1,
                    'transport_name': 'Transport 1',
                },
                '2': {
                    'name': 'Simon',
                    'number': 2,
                    'transport_name': 'Transport 2',
                },
                '3': {
                    'name': 'Morgan',
                    'number': 3,
                    'transport_name': 'Transport 3',
                }
            })
        self.assertTrue(len(resp), 3)
        for sms in resp:
            if sms.to_msisdn == '1':
                self.assertEquals(sms.message, 'David 1 Transport 1')
            elif sms.to_msisdn == '2':
                self.assertEquals(sms.message, 'Simon 2 Transport 2')
            elif sms.to_msisdn == '3':
                self.assertEquals(sms.message, 'Morgan 3 Transport 3')
            else:
                raise Exception('Do not know whats going on here')

    def test_sms_status(self):
        send_resp = self.client.send_sms(
            to_msisdn=['27123456788'],
            from_msisdn='27123456789',
            message='hello world')
        sent_sms = send_resp[0]
        status = self.client.get_status(sent_sms.id)
        self.assertEquals(status.to_msisdn, u'27123456788')
        self.assertEquals(status.from_msisdn, u'27123456789')
        self.assertEquals(status.message, u'hello world')
        self.assertEquals(status.transport_status, u'')
        self.assertEquals(status.transport_status_display, u'')
        self.assertEquals(status.batch_id, sent_sms.batch_id)

    def test_sms_status_since(self):
        # prime the db with old SMSs sent
        user = User.objects.get(username='vumi')
        batch = SentSMSBatch.objects.create(user=user, title='batch')
        # create the sent_sms
        sent_sms = SentSMS.objects.create(
            user=user,
            batch=batch,
            to_msisdn='27123456789',
            from_msisdn='27123456789',
            transport_name='Clickatell',
            message='hello world',
        )
        # reset the date to 1 month back, django automatically timestamps
        # for now() at creation
        SentSMS.objects.filter(pk=sent_sms.pk).update(
            created_at=datetime.now() - timedelta(days=20),
            updated_at=datetime.now() - timedelta(days=20),
        )

        # 10 days before
        self.assertEquals(len(self.client.get_status_since(
            since=datetime.now() - timedelta(days=30))), 1)
        # 10 days after
        self.assertEquals(len(self.client.get_status_since(
            since=datetime.now() - timedelta(days=10))), 0)

    def test_sms_status_by_id(self):
        send_resp = self.client.send_sms(
            to_msisdn=['27123456787', '27123456788', '27123456789'],
            from_msisdn='27123456789',
            message='hello world')

        ids = [sent_sms.id for sent_sms in send_resp[:-1]]
        statuses = self.client.get_status_by_id(id=ids)
        self.assertEquals(len(statuses), 2)
        self.assertTrue(all((status.id in ids) for status in statuses))

    def test_sms_received_callback(self):
        url = 'http://username:password@localhost/received'
        # create
        callback_resp = self.client.set_callback(
            event='sms_received',
            url=url,
        )
        self.assertTrue(callback_resp.url, url)
        # update
        callback_resp = self.client.update_callback(
            id=callback_resp.id,
            url=url + '/update',
        )
        self.assertTrue(callback_resp.url, url + '/update')
        # delete
        callback_resp = self.client.delete_callback(
            id=callback_resp.id,
        )
        self.assertTrue(callback_resp.success)
