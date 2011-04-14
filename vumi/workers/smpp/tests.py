import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'vumi.webapp.settings'


from twisted.trial.unittest import TestCase
from twisted.python import log
from vumi.utils import TestPublisher, mocking
from vumi.message import Message, VUMI_DATE_FORMAT
from vumi.workers.smpp.worker import SMSBatchConsumer, SMSReceiptConsumer

from django.conf import settings

from django.contrib.auth.models import User
from vumi.webapp.api.models import SentSMSBatch, SentSMS, SMPPResp
from vumi.utils import setup_django_test_database, teardown_django_test_database

from datetime import datetime

class SMSBatchConsumerTestCase(TestCase):

    def setUp(self):
        self.publisher = TestPublisher()
        self.debatcher = SMSBatchConsumer(self.publisher)
        self.runner, self.config = setup_django_test_database()
    
    def tearDown(self):
        teardown_django_test_database(self.runner, self.config)
    
    def test_debatching(self):
        """It should publish an incoming batch of 3 into separate
        individual messages"""
        
        user = User.objects.create(username='vumi')
        batch = SentSMSBatch(user=user, title='test batch')
        # create 3 test messages in this batch
        recipients = [u'27123456789%s' % i for i in range(0,3)]
        for recipient in recipients:
            batch.sentsms_set.create(to_msisdn=recipient,
                from_msisdn='27123456789', message='testing message',
                transport_name='Transport', user=user)
            
        message = Message(kwargs={
            "id": batch.pk
        })

        self.debatcher.consume_message(message)
        self.assertEquals(len(self.publisher.queue), 3)
        self.assertEquals(set([m.payload['to_msisdn'] for m,kwargs in
                self.publisher.queue]),
                set(recipients))
        # ensure that the routing key is converted to lower case
        self.assertTrue(all(kwargs['routing_key'] == 'sms.outbound.transport'
            for m,kwargs in self.publisher.queue))

class SMSReceiptConsumerTestCase(TestCase):
    
    def setUp(self):
        self.publisher = TestPublisher()
        self.receipt_consumer = SMSReceiptConsumer()
        self.test_db_args = setup_django_test_database()
    
    def tearDown(self):
        teardown_django_test_database(*self.test_db_args)
    
    def test_find_sent_sms(self):
        """Given a transport and a message_id it should find the SentSMS record
        that they belong to. This is hacky as there really shouldn't be two
        tables to check for this message_id lookup stuff"""
        
        user = User.objects.create(username='vumi')
        sent_sms = SentSMS.objects.create(user=user,
            transport_name='TransportName', transport_msg_id='message_id')
        self.assertEquals(sent_sms,
            self.receipt_consumer.find_sent_sms('TransportName', 'message_id'))

        # if given an SMPPResp message id, it should find it too
        sent_sms = SentSMS.objects.create(user=user,
                transport_name='TransportName')
        smpp_resp = SMPPResp(message_id='smpp_message_id')
        smpp_resp.sent_sms = sent_sms
        smpp_resp.sequence_number = 0
        smpp_resp.save()

        self.assertEquals(sent_sms, 
                self.receipt_consumer.find_sent_sms('TransportName','smpp_message_id'))

    def test_failure_to_find_sent_sms(self):
        """When no SentSMS or SMPPResp can be found it should raise a
        SentSMS.DoesNotExist exception"""
        self.assertRaises(SentSMS.DoesNotExist,
                self.receipt_consumer.find_sent_sms,
                'TransportName','unknown_message_id')


    def test_receipt_processing(self):
        """Receipts can come from an a varying amount of transports, they are
        all processed by the receipt worker."""
    
    def test_callback_handling(self):
        """When consuming a delivery receipt it should fire a bunch of
        callbacks"""
        user = User.objects.create(username='vumi')
        profile = user.get_profile()
        urlcallback = profile.urlcallback_set.create(url='http://test.domain',
                        name='sms_receipt')
        
        sent_sms = user.sentsms_set.create(transport_msg_id='message_id',
                    transport_name='Opera')
        sent_sms = SentSMS.objects.get(pk=sent_sms.pk)

        from vumi.webapp.api import utils
        original_callback = utils.callback
        
        delivery_timestamp = datetime.now()

        # set the expectation
        with mocking(utils.callback).to_return(urlcallback.url,"OK") as mocked:
            message = Message(
                transport_status='D', 
                transport_name='Opera',
                transport_delivered_at=delivery_timestamp,
                transport_msg_id='message_id')
            self.receipt_consumer.consume_message(message) 
            
            # test that it was called
            self.assertTrue(mocked.called)
            
            # test what it was called with
            url, params_list = mocked.args
            params = dict(params_list)
            self.assertEquals(url, urlcallback.url)
            self.assertEquals(params['callback_name'], 'sms_receipt')
            self.assertEquals(params['id'], str(sent_sms.pk))
            self.assertEquals(params['from_msisdn'], sent_sms.from_msisdn)
            self.assertEquals(params['to_msisdn'], sent_sms.to_msisdn)
            self.assertEquals(params['message'], sent_sms.message)
            
        
        
