import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'vumi.webapp.settings'

from twisted.trial.unittest import TestCase
from twisted.python import log
from twisted.internet import defer
from txamqp.content import Content
from vumi.tests.utils import TestPublisher, TestChannel, TestQueue, \
                            fake_amq_message, mocking, \
                            setup_django_test_database, \
                            teardown_django_test_database, FakeRedis
from vumi.message import Message, VUMI_DATE_FORMAT
from vumi.service import Consumer, Publisher, RoutingKeyError
from vumi.workers.smpp.worker import SMSBatchConsumer, SMSReceiptConsumer, \
    SMSKeywordConsumer, dynamically_create_keyword_consumer, SMSKeywordWorker, \
    SMSReceiptWorker

from django.conf import settings

from django.contrib.auth.models import User
from vumi.webapp.api.models import SentSMSBatch, SentSMS, SMPPResp, Keyword, \
                                    ReceivedSMS, Transport
from vumi.webapp.api import utils

from datetime import datetime
import re

from smpp.pdu_builder import SubmitSMResp, BindTransceiverResp
from vumi.workers.smpp.client import EsmeTransceiver
from vumi.workers.smpp.transport import SmppTransport
import redis


#class SMSBatchTestCase(TestCase):

    #def setUp(self):
        #self.publisher = TestPublisher()
        #self.debatcher = SMSBatchConsumer(self.publisher)
        #self.runner, self.config = setup_django_test_database()
   # 
    #def tearDown(self):
        #teardown_django_test_database(self.runner, self.config)
   # 
    #def test_debatching(self):
        #"""It should publish an incoming batch of 3 into separate
        #individual messages"""
       # 
        #user = User.objects.create(username='vumi')
        #batch = SentSMSBatch(user=user, title='test batch')
        ## create 3 test messages in this batch
        #recipients = [u'27123456789%s' % i for i in range(0,3)]
        #for recipient in recipients:
            #batch.sentsms_set.create(to_msisdn=recipient,
                #from_msisdn='27123456789', message='testing message',
                #transport_name='Transport', user=user)
           # 
        #message = Message(kwargs={
            #"id": batch.pk
        #})

        #self.debatcher.consume_message(message)
        #self.assertEquals(len(self.publisher.queue), 3)
        #self.assertEquals(set([m.payload['to_msisdn'] for m,kwargs in
                #self.publisher.queue]),
                #set(recipients))
        ## ensure that the routing key is converted to lower case
        #self.assertTrue(all(kwargs['routing_key'] == 'sms.outbound.transport'
            #for m,kwargs in self.publisher.queue))

#class SMSReceiptTestCase(TestCase):
   # 
    #def setUp(self):
        #self.publisher = TestPublisher()
        #self.receipt_consumer = SMSReceiptConsumer()
        #self.test_db_args = setup_django_test_database()
   # 
    #def tearDown(self):
        #teardown_django_test_database(*self.test_db_args)
   # 
    #def test_find_sent_sms(self):
        #"""Given a transport and a message_id it should find the SentSMS record
        #that they belong to. This is hacky as there really shouldn't be two
        #tables to check for this message_id lookup stuff"""
       # 
        #user = User.objects.create(username='vumi')
        #sent_sms = SentSMS.objects.create(user=user,
            #transport_name='TransportName', transport_msg_id='message_id')
        #self.assertEquals(sent_sms,
            #self.receipt_consumer.find_sent_sms('TransportName', 'message_id'))


    #def test_failure_to_find_sent_sms(self):
        #"""When no SentSMS or SMPPResp can be found it should raise a
        #SentSMS.DoesNotExist exception"""
        #self.assertRaises(SentSMS.DoesNotExist,
                #self.receipt_consumer.find_sent_sms,
                #'TransportName','unknown_message_id')


    #def test_receipt_processing(self):
        #"""Receipts can come from an a varying amount of transports, they are
        #all processed by the receipt worker."""
   # 
    #def test_callback_handling(self):
        #"""When consuming a delivery receipt it should fire a bunch of
        #callbacks"""
        #user = User.objects.create(username='vumi')
        #profile = user.get_profile()
        #urlcallback = profile.urlcallback_set.create(url='http://test.domain',
                        #name='sms_receipt')
       # 
        #sent_sms = user.sentsms_set.create(transport_msg_id='message_id',
                    #transport_name='Opera')
        #sent_sms = SentSMS.objects.get(pk=sent_sms.pk)

        #delivery_timestamp = datetime.now()

        ## set the expectation
        #with mocking(utils.callback).to_return(urlcallback.url,"OK") as mocked:
            #message = Message(
                #transport_status='D', 
                #transport_name='Opera',
                #transport_delivered_at=delivery_timestamp,
                #transport_msg_id='message_id')
            #self.receipt_consumer.consume_message(message) 
           # 
            ## test that it was called
            #self.assertTrue(mocked.called)
           # 
            ## test what it was called with
            #url, params_list = mocked.args
            #params = dict(params_list)
            #self.assertEquals(url, urlcallback.url)
            #self.assertEquals(params['callback_name'], 'sms_receipt')
            #self.assertEquals(params['id'], str(sent_sms.pk))
            #self.assertEquals(params['from_msisdn'], sent_sms.from_msisdn)
            #self.assertEquals(params['to_msisdn'], sent_sms.to_msisdn)
            #self.assertEquals(params['message'], sent_sms.message)
           # 
    #@defer.inlineCallbacks
    #def test_dynamic_receipt_consumer_creation(self):
        #"""
        #Receipt consumers should be automatically created based on
        #whatever transports are registered in the db
        #"""
        #for transport_name in ['TransportName%s' % i for i in range(0,5)]:
            #Transport.objects.create(name=transport_name)
       # 
        #class TestingSMSReceiptWorker(SMSReceiptWorker):
            #def __init__(self):
                #self.log = []
           # 
            #def start_consumer(self, consumer_class):
                #def _cb(result):
                    #self.log.append(result)
               # 
                #d = defer.Deferred()
                #d.addCallback(_cb)
                #d.callback(consumer_class)
                #return d
           # 
        #worker = TestingSMSReceiptWorker()
        #yield worker.startWorker()
       # 
        #self.assertEquals(len(worker.log), 5)
        ## transport name should be the part of the class name
        #self.assertTrue(all([
            #re.match(r'TransportName[0-5]{1}_SMSReceiptConsumer', klass.__name__) 
            #for klass in worker.log]))
        ## queue_name & routing_key should include the lowercased transport name
        #self.assertTrue(all([
            #re.match(r'sms.receipt.transportname[0-5]{1}', klass.queue_name)
            #for klass in worker.log]))
        #self.assertTrue(all([
            #re.match(r'sms.receipt.transportname[0-5]{1}', klass.routing_key)
            #for klass in worker.log]))
       # 
   # 
#class SMSKeywordTestCase(TestCase):
   # 
    #def setUp(self):
        #self.test_db_args = setup_django_test_database()
        #consumer_class = dynamically_create_keyword_consumer('Testing', 
                                    #queue_name='sms.inbound.testing.27123456780',
                                    #routing_key='sms.inbound.testing.27123456780')
        #self.keyword_consumer = consumer_class()
   # 
    #def tearDown(self):
        #teardown_django_test_database(*self.test_db_args)
   # 
    #def test_callback_handling(self):
        #"""If an SMS arrives with a keyword it should be forwarded to a 
        #specific url"""
       # 
        #user = User.objects.create(username='vumi')
        #profile = user.get_profile()
        ## create two callbacks, each should be called
        #urlcallback = profile.urlcallback_set.create(url='http://test.domain/1', 
                                                        #name='sms_received')
        #urlcallback = profile.urlcallback_set.create(url='http://test.domain/2', 
                                                        #name='sms_received')
        #keyword = Keyword.objects.create(keyword='keyword', user=user)
        #message = Message(
                    #short_message="keyword message", 
                    #destination_addr="27123456780",
                    #source_addr='27123456789') # prefixed
       # 
        ## make sure db is empty
        #self.assertFalse(ReceivedSMS.objects.exists())
       # 
        #with mocking(utils.callback).to_return(urlcallback.url,"OK") as mocked:
            ## consume the message with the callback mocked to return OK
            #self.keyword_consumer.consume_message(message)
           # 
            ## check for creation of 1 received sms
            #all_received_sms = ReceivedSMS.objects.filter(to_msisdn='27123456780')
            #self.assertEquals(all_received_sms.count(), 1)
           # 
            ## check the data of the received sms
            #received_sms = all_received_sms.latest()
            #self.assertEquals(received_sms.from_msisdn, '27123456789')
            #self.assertEquals(received_sms.user, user)
            #self.assertEquals(received_sms.message, 'keyword message')
            #self.assertEquals(received_sms.transport_name, 'testing')
           # 
            ## check that the callback was actually fired
            #self.assertEquals(mocked.called, 2)
            ## test that both urlcallbacks where requested
            #self.assertEquals(mocked.history[0].args[0], 'http://test.domain/1')
            #self.assertEquals(mocked.history[1].args[0], 'http://test.domain/2')
           # 
            ## check the vars posted to the url callback
            #url, paramlist = mocked.args
            #params = dict(paramlist)
            #self.assertEquals(params['callback_name'], 'sms_received')
            #self.assertEquals(params['to_msisdn'], '27123456780')
            #self.assertEquals(params['from_msisdn'], '27123456789')
            #self.assertEquals(params['message'], 'keyword message')
   # 
    #@defer.inlineCallbacks
    #def test_dynamic_keyword_consumer_creation(self):
       # 
        #class TestingSMSKeywordWorker(SMSKeywordWorker):
            #def __init__(self):
                #"""skip init as it tries to connect to AMQP"""
                #self.log = []
           # 
            #def start_consumer(self, consumer_class):
               # 
                #def _cb(result):
                    #self.log.append(result)
                    #return result
               # 
                #d = defer.Deferred()
                #d.addCallback(_cb)
                #d.callback(consumer_class)
                #return d
       # 
        ## FIXME: start subclass to avoid AMQP connection
        #worker = TestingSMSKeywordWorker()
        ## mock the config
        #worker.config = {
            #'TRANSPORT_NAME': 'testing',
            #'OPERATOR_NUMBER': {
                #'network1': '27123456780',
                #'network2': '27123456781',
            #}
        #}
        ## yield is important, a deferred is returned
        #yield worker.startWorker()
        #network1_consumer = worker.log[0]
        #network2_consumer = worker.log[1]
       # 
        #self.assertEquals(network1_consumer.__name__, 'network1_SMSKeywordConsumer')
        #self.assertEquals(network1_consumer.queue_name, 'sms.inbound.testing.27123456780')
        #self.assertEquals(network1_consumer.routing_key, 'sms.inbound.testing.27123456780')
       # 
        #self.assertEquals(network2_consumer.__name__, 'network2_SMSKeywordConsumer')
        #self.assertEquals(network2_consumer.queue_name, 'sms.inbound.testing.27123456781')
        #self.assertEquals(network2_consumer.routing_key, 'sms.inbound.testing.27123456781')
   # 

#class ConsumerTestCase(TestCase):
   # 
    #def setUp(self):
       # 
        #class TestConsumer(Consumer):
            #def __init__(self):
                #self.log = []
           # 
            #def consume_message(self, message):
                #self.log.append(message)
           # 
       # 
        #self.consumer = TestConsumer()
        #self.channel = TestChannel()
   # 
    #def tearDown(self):
        #pass
   # 
    #@defer.inlineCallbacks
    #def test_starting_consumer(self):
        #queue = TestQueue([fake_amq_message({
            #'key': 'value'
        #})])
        #self.consumer.start(self.channel, queue)
        #yield self.consumer.stop()
        #message = self.consumer.log.pop()
        #self.assertEquals(message.payload, {
            #'key': 'value'
        #})
   # 
#class PublisherTestCase(TestCase):
   # 
    #def setUp(self):
        #self.publisher = Publisher()
        #self.channel = TestChannel()
   # 
    #def tearDown(self):
        #pass
   # 
    #def test_publish(self):
        #self.publisher.start(self.channel)
        #self.publisher.publish_message(Message(testing="data"), 
            #exchange_name="custom_exchange", routing_key="custom_routing_key")
       # 
        #published_msg = self.channel.publish_log.pop()
        #content = published_msg['content']
        #self.assertEquals(published_msg['exchange'], "custom_exchange")
        #self.assertEquals(published_msg['routing_key'], "custom_routing_key")
        #self.assertEquals(content.body, '{"testing": "data"}')
        #self.assertEquals(content.properties, {'delivery mode': 2})
       # 
    #def test_upper_case_routing_keys_exception(self):
        #self.publisher.start(self.channel)
        #self.assertRaises(
            #RoutingKeyError, # exception
            #self.publisher.publish_message, # callback
            #Message(testing="data"), # args
            #routing_key="IN.UPPER.CASE"
            #)
   # 


class RedisTestEsmeTransceiver(EsmeTransceiver):

    def sendPDU(self, pdu):
        pass # don't actually send anything

class RedisTestSmppTransport(SmppTransport):

    def send_smpp(self, id, to_msisdn, message, *args, **kwargs):
        sequence_number = self.esme_client.submit_sm(
                short_message = message.encode('utf-8'),
                destination_addr = str(to_msisdn),
                source_addr = "1234567890",
                )
        return sequence_number


class RedisRespTestCase(TestCase):

    def setUp(self):
        self.seq = [123456]
        self.config = {
                "system_id" : "vumitest-vumitest-vumitest",
                "host" : "host",
                "port" : "port",
                "smpp_increment" : 10,
                "smpp_offset" : 6,
                "TRANSPORT_NAME" : "redis_testing_transport",
                }
        self.vumi_options = {
                "vhost" : "develop",
                }

        # hack a lot of transport setup
        self.esme = RedisTestEsmeTransceiver(self.seq, self.config, self.vumi_options)
        self.esme.state = 'BOUND_TRX'
        self.transport = RedisTestSmppTransport(None, self.config)
        self.transport.esme_client = self.esme
        self.transport.smpp_offset = self.config['smpp_offset']
        self.transport.transport_name = self.config.get('TRANSPORT_NAME','fallback')
        self.transport.r_server = redis.Redis("localhost", db=7)
        self.transport.r_prefix = "%(system_id)s@%(host)s:%(port)s" % self.config
        self.transport.publisher = TestPublisher()
        self.esme.setSubmitSMRespCallback(self.transport.submit_sm_resp)


    def tearDown(self):
        # still need to clean out all redis keys which starting with:
        # "vumitest-vumitest-vumitest"
        keys = self.transport.r_server.keys(self.config["system_id"]+"*")
        if len(keys) and type(keys) is str:
            keys = keys.split(' ')
        if len(keys):
            for k in keys:
                self.transport.r_server.delete(k)


    def test_match_resp(self):
        message1 = Message(
            id = 444,
            message = "hello world",
            to_msisdn = "1111111111",
            )
        sequence_num1 = self.esme.getSeq()
        response1 = SubmitSMResp(sequence_num1, "3rd_party_id_1")
        self.transport.consume_message(message1)

        message2 = Message(
            id = 445,
            message = "hello world",
            to_msisdn = "1111111111",
            )
        sequence_num2 = self.esme.getSeq()
        response2 = SubmitSMResp(sequence_num2, "3rd_party_id_2")
        self.transport.consume_message(message2)

        # respond out of order - just to keep things interesting
        self.esme.handleData(response2.get_bin())
        self.esme.handleData(response1.get_bin())

        self.assertEquals(self.transport.publisher.queue[0][0].payload,
                {'id': '445', 'transport_message_id': '3rd_party_id_2'})

        self.assertEquals(self.transport.publisher.queue[1][0].payload,
                {'id': '444', 'transport_message_id': '3rd_party_id_1'})


class FakeRedisRespTestCase(TestCase):

    def setUp(self):
        self.seq = [123456]
        self.config = {
                "system_id" : "vumitest-vumitest-vumitest",
                "host" : "host",
                "port" : "port",
                "smpp_increment" : 10,
                "smpp_offset" : 6,
                "TRANSPORT_NAME" : "redis_testing_transport",
                }
        self.vumi_options = {
                "vhost" : "develop",
                }

        # hack a lot of transport setup
        self.esme = RedisTestEsmeTransceiver(self.seq, self.config, self.vumi_options)
        self.esme.state = 'BOUND_TRX'
        self.transport = RedisTestSmppTransport(None, self.config)
        self.transport.esme_client = self.esme
        self.transport.smpp_offset = self.config['smpp_offset']
        self.transport.transport_name = self.config.get('TRANSPORT_NAME','fallback')
        self.transport.r_server = FakeRedis()
        self.transport.r_prefix = "%(system_id)s@%(host)s:%(port)s" % self.config
        self.transport.publisher = TestPublisher()
        self.esme.setSubmitSMRespCallback(self.transport.submit_sm_resp)


    def tearDown(self):
        # no need to cleanup fake redis
        pass


    def test_match_resp(self):
        message1 = Message(
            id = 444,
            message = "hello world",
            to_msisdn = "1111111111",
            )
        sequence_num1 = self.esme.getSeq()
        response1 = SubmitSMResp(sequence_num1, "3rd_party_id_1")
        self.transport.consume_message(message1)

        message2 = Message(
            id = 445,
            message = "hello world",
            to_msisdn = "1111111111",
            )
        sequence_num2 = self.esme.getSeq()
        response2 = SubmitSMResp(sequence_num2, "3rd_party_id_2")
        self.transport.consume_message(message2)

        # respond out of order - just to keep things interesting
        self.esme.handleData(response2.get_bin())
        self.esme.handleData(response1.get_bin())

        self.assertEquals(self.transport.publisher.queue[0][0].payload,
                {'id': '445', 'transport_message_id': '3rd_party_id_2'})

        self.assertEquals(self.transport.publisher.queue[1][0].payload,
                {'id': '444', 'transport_message_id': '3rd_party_id_1'})


        message3 = Message(
            id = 446,
            message = "hello world",
            to_msisdn = "1111111111",
            )
        sequence_num3 = self.esme.getSeq()
        response3 = SubmitSMResp(sequence_num3, "3rd_party_id_3", command_status="ESME_RSYSERR")
        self.transport.consume_message(message3)
        self.esme.handleData(response3.get_bin())
        self.assertEquals(self.transport.publisher.queue[2][0].payload,
                {'id': '446', 'transport_message_id': '3rd_party_id_3'})


        message4 = Message(
            id = 447,
            message = "hello world",
            to_msisdn = "1111111111",
            )
        sequence_num4 = self.esme.getSeq()
        response4 = SubmitSMResp(sequence_num4, "3rd_party_id_4", command_status="ESME_RINVPASWD")
        self.transport.consume_message(message4)
        self.esme.handleData(response4.get_bin())
        self.assertEquals(self.transport.publisher.queue[3][0].payload,
                {'id': '447', 'transport_message_id': '3rd_party_id_4'})


        response4 = BindTransceiverResp(sequence_num4, "3rd_party_id_4", command_status="ESME_RINVPASWD")
        self.esme.handleData(response4.get_bin())
