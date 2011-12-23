import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'vumi.webapp.settings'

from datetime import datetime
import re

from twisted.trial.unittest import TestCase
from twisted.internet import defer
from django.contrib.auth.models import User

from vumi.tests.utils import (
    TestPublisher, mocking, setup_django_test_database,
    teardown_django_test_database)
from vumi.message import TransportUserMessage, TransportEvent, Message
from vumi.webapp.api.models import (SentSMSBatch, SentSMS, Keyword,
                                    ReceivedSMS, Transport)
from vumi.webapp.api import utils
from vumi.transports.smpp.worker import (
    SMSBatchConsumer, SMSReceiptConsumer, dynamically_create_keyword_consumer,
    SMSKeywordWorker, SMSReceiptWorker)


class SMSBatchTestCase(TestCase):

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
        recipients = [u'27123456789%s' % i for i in range(0, 3)]
        for recipient in recipients:
            batch.sentsms_set.create(to_msisdn=recipient,
                from_msisdn='27123456789', message='testing message',
                transport_name='Transport', user=user)

        message = Message(kwargs={
            "id": batch.pk
        })

        self.debatcher.consume_message(message)
        self.assertEquals(len(self.publisher.queue), 3)
        self.assertEquals(set([m.payload['to_addr'] for m, kwargs in
                self.publisher.queue]),
                set(recipients))


class SMSReceiptTestCase(TestCase):

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

    def test_failure_to_find_sent_sms(self):
        """When no SentSMS or SMPPResp can be found it should raise a
        SentSMS.DoesNotExist exception"""
        self.assertRaises(SentSMS.DoesNotExist,
                self.receipt_consumer.find_sent_sms,
                'TransportName', 'unknown_message_id')

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

        delivery_timestamp = datetime.now()

        # set the expectation
        with mocking(utils.callback).to_return(urlcallback.url,
                                               "OK") as mocked:
            message = TransportEvent(
                event_type='delivery_report',
                delivery_status='delivered',
                transport_name='Opera',
                timestamp=delivery_timestamp,
                user_message_id='message_id')
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

    @defer.inlineCallbacks
    def test_dynamic_receipt_consumer_creation(self):
        """
        Receipt consumers should be automatically created based on
        whatever transports are registered in the db
        """
        for transport_name in ['TransportName%s' % i for i in range(0, 5)]:
            Transport.objects.create(name=transport_name)

        class TestingSMSReceiptWorker(SMSReceiptWorker):
            def __init__(self):
                self.log = []

            def start_consumer(self, consumer_class):
                def _cb(result):
                    self.log.append(result)

                d = defer.Deferred()
                d.addCallback(_cb)
                d.callback(consumer_class)
                return d

        worker = TestingSMSReceiptWorker()
        yield worker.startWorker()

        self.assertEquals(len(worker.log), 5)
        # transport name should be the part of the class name
        self.assertTrue(all([
            re.match(r'TransportName[0-5]{1}_SMSReceiptConsumer',
                     klass.__name__)
            for klass in worker.log]))
        # queue_name & routing_key should include the lowercased transport name
        self.assertTrue(all([
            re.match(r'sms.receipt.transportname[0-5]{1}', klass.queue_name)
            for klass in worker.log]))
        self.assertTrue(all([
            re.match(r'sms.receipt.transportname[0-5]{1}', klass.routing_key)
            for klass in worker.log]))


class SMSKeywordTestCase(TestCase):

    def setUp(self):
        self.test_db_args = setup_django_test_database()
        consumer_class = dynamically_create_keyword_consumer('Testing',
                            queue_name='sms.inbound.testing.27123456780',
                            routing_key='sms.inbound.testing.27123456780')
        self.keyword_consumer = consumer_class()

    def tearDown(self):
        teardown_django_test_database(*self.test_db_args)

    def test_callback_handling(self):
        """If an SMS arrives with a keyword it should be forwarded to a
        specific url"""

        user = User.objects.create(username='vumi')
        profile = user.get_profile()
        # create two callbacks, each should be called
        urlcallback = profile.urlcallback_set.create(
                            url='http://test.domain/1', name='sms_received')
        urlcallback = profile.urlcallback_set.create(
                            url='http://test.domain/2', name='sms_received')
        Keyword.objects.create(keyword='keyword', user=user)
        message = TransportUserMessage(
            transport_name='sphex',
            transport_type='sms',
            transport_metadata={},
            content="keyword message",
            to_addr="27123456780",
            from_addr='27123456789')  # prefixed

        # make sure db is empty
        self.assertFalse(ReceivedSMS.objects.exists())

        with mocking(utils.callback).to_return(urlcallback.url,
                                               "OK") as mocked:
            # consume the message with the callback mocked to return OK
            self.keyword_consumer.consume_message(message)

            # check for creation of 1 received sms
            all_received_sms = ReceivedSMS.objects.filter(
                to_msisdn='27123456780')
            self.assertEquals(all_received_sms.count(), 1)

            # check the data of the received sms
            received_sms = all_received_sms.latest()
            self.assertEquals(received_sms.from_msisdn, '27123456789')
            self.assertEquals(received_sms.user, user)
            self.assertEquals(received_sms.message, 'keyword message')
            self.assertEquals(received_sms.transport_name, 'testing')

            # check that the callback was actually fired
            self.assertEquals(mocked.called, 2)
            # test that both urlcallbacks where requested
            self.assertEquals(mocked.history[0].args[0],
                              'http://test.domain/1')
            self.assertEquals(mocked.history[1].args[0],
                              'http://test.domain/2')

            # check the vars posted to the url callback
            url, paramlist = mocked.args
            params = dict(paramlist)
            self.assertEquals(params['callback_name'], 'sms_received')
            self.assertEquals(params['to_msisdn'], '27123456780')
            self.assertEquals(params['from_msisdn'], '27123456789')
            self.assertEquals(params['message'], 'keyword message')

    @defer.inlineCallbacks
    def test_dynamic_keyword_consumer_creation(self):

        class TestingSMSKeywordWorker(SMSKeywordWorker):
            def __init__(self):
                """skip init as it tries to connect to AMQP"""
                self.log = []

            def start_consumer(self, consumer_class):

                def _cb(result):
                    self.log.append(result)
                    return result

                d = defer.Deferred()
                d.addCallback(_cb)
                d.callback(consumer_class)
                return d

        # FIXME: start subclass to avoid AMQP connection
        worker = TestingSMSKeywordWorker()
        # mock the config
        worker.config = {
            'TRANSPORT_NAME': 'testing',
            'OPERATOR_NUMBER': {
                'network1': '27123456780',
                'network2': '27123456781',
            }
        }
        # yield is important, a deferred is returned
        yield worker.startWorker()
        network1_consumer = worker.log[0]
        network2_consumer = worker.log[1]

        self.assertEquals(network1_consumer.__name__,
                          'network1_SMSKeywordConsumer')
        self.assertEquals(network1_consumer.queue_name,
                          'sms.inbound.testing.27123456780')
        self.assertEquals(network1_consumer.routing_key,
                          'sms.inbound.testing.27123456780')

        self.assertEquals(network2_consumer.__name__,
                          'network2_SMSKeywordConsumer')
        self.assertEquals(network2_consumer.queue_name,
                          'sms.inbound.testing.27123456781')
        self.assertEquals(network2_consumer.routing_key,
                          'sms.inbound.testing.27123456781')


