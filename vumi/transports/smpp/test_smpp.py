import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'vumi.webapp.settings'

from datetime import datetime
import re

from twisted.trial.unittest import TestCase
from twisted.internet import defer
from django.contrib.auth.models import User
from smpp.pdu_builder import SubmitSMResp, BindTransceiverResp

from vumi.tests.utils import (
    TestPublisher, mocking, setup_django_test_database,
    teardown_django_test_database, FakeRedis)
from vumi.message import TransportUserMessage, TransportEvent, Message
from vumi.webapp.api.models import (SentSMSBatch, SentSMS, Keyword,
                                    ReceivedSMS, Transport)
from vumi.webapp.api import utils
from vumi.transports.smpp.client import EsmeTransceiver
from vumi.transports.smpp.transport import SmppTransport
from vumi.transports.smpp.worker import (
    SMSBatchConsumer, SMSReceiptConsumer, dynamically_create_keyword_consumer,
    SMSKeywordWorker, SMSReceiptWorker)
from vumi.transports.tests.test_base import TransportTestCase


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


class RedisTestEsmeTransceiver(EsmeTransceiver):

    def sendPDU(self, pdu):
        pass  # don't actually send anything


class RedisTestSmppTransport(SmppTransport):

    def send_smpp(self, message):
        to_addr = message['to_addr']
        text = message['content']
        sequence_number = self.esme_client.submit_sm(
                short_message=text.encode('utf-8'),
                destination_addr=str(to_addr),
                source_addr="1234567890",
                )
        return sequence_number

    def ok(self, *args, **kwargs):
        pass

    def mess_permfault(self, *args, **kwargs):
        pass

    def mess_tempfault(self, *args, **kwargs):
        pdu = kwargs.get('pdu')
        sequence_number = pdu['header']['sequence_number']
        id = self.r_get_id_for_sequence(sequence_number)
        reason = pdu['header']['command_status']
        self.send_failure(Message(id=id), RuntimeError("A random exception"),
                          reason)
        pass

    def conn_permfault(self, *args, **kwargs):
        pass

    def conn_tempfault(self, *args, **kwargs):
        pass

    def conn_throttle(self, *args, **kwargs):
        if kwargs.get('pdu'):
            self.throttle_invoked_via_pdu = True


def payload_equal_except_timestamp(dict1, dict2):
    return_value = True
    for k in dict1.keys():
        if return_value and k != "timestamp" and dict1.get(k):
            return_value = return_value and dict1.get(k) == dict2.get(k)
    for k in dict2.keys():
        if return_value and k != "timestamp" and dict2.get(k):
            return_value = return_value and dict1.get(k) == dict2.get(k)
    return return_value


class FakeRedisRespTestCase(TransportTestCase):

    transport_name = "redis_testing_transport"
    transport_class = RedisTestSmppTransport

    @defer.inlineCallbacks
    def setUp(self):
        super(FakeRedisRespTestCase, self).setUp()
        self.seq = [123456]
        self.config = {
                "system_id": "vumitest-vumitest-vumitest",
                "host": "host",
                "port": "port",
                "smpp_increment": 10,
                "smpp_offset": 6,
                "TRANSPORT_NAME": "redis_testing_transport",
                }
        self.vumi_options = {
                "vhost": "develop",
                }

        # hack a lot of transport setup
        self.esme = RedisTestEsmeTransceiver(
                self.seq, self.config, self.vumi_options)
        self.esme.state = 'BOUND_TRX'
        self.transport = yield self.get_transport(self.config, start=False)
        self.transport.esme_client = self.esme
        self.transport.r_server = FakeRedis()
        self.esme.setSubmitSMRespCallback(self.transport.submit_sm_resp)

        # set error handlers
        self.esme.update_error_handlers({
            "ok": self.transport.ok,
            "mess_permfault": self.transport.mess_permfault,
            "mess_tempfault": self.transport.mess_tempfault,
            "conn_permfault": self.transport.conn_permfault,
            "conn_tempfault": self.transport.conn_tempfault,
            "conn_throttle": self.transport.conn_throttle,
            })

        yield self.transport.startWorker()

    @defer.inlineCallbacks
    def test_match_resp(self):
        message1 = self.mkmsg_out(
            message_id='444',
            content="hello world",
            to_addr="1111111111")
        sequence_num1 = self.esme.getSeq()
        response1 = SubmitSMResp(sequence_num1, "3rd_party_id_1")
        yield self.transport._process_message(message1)

        message2 = self.mkmsg_out(
            message_id='445',
            content="hello world",
            to_addr="1111111111")
        sequence_num2 = self.esme.getSeq()
        response2 = SubmitSMResp(sequence_num2, "3rd_party_id_2")
        yield self.transport._process_message(message2)

        # respond out of order - just to keep things interesting
        self.esme.handleData(response2.get_bin())
        self.esme.handleData(response1.get_bin())

        self.assertEqual([
                self.mkmsg_ack('445', '3rd_party_id_2'),
                self.mkmsg_ack('444', '3rd_party_id_1'),
                ], self.get_dispatched_events())

        message3 = self.mkmsg_out(
            message_id=446,
            content="hello world",
            to_addr="1111111111")
        sequence_num3 = self.esme.getSeq()
        response3 = SubmitSMResp(sequence_num3, "3rd_party_id_3",
                command_status="ESME_RSUBMITFAIL")
        self.transport._process_message(message3)
        self.esme.handleData(response3.get_bin())
        self.assertEqual([self.mkmsg_ack('446', '3rd_party_id_3')],
                         self.get_dispatched_events()[2:])

        self.assertEqual([self.mkmsg_fail({'id': '446'}, 'ESME_RSUBMITFAIL')],
                         self.get_dispatched_failures())

        message4 = self.mkmsg_out(
            message_id=447,
            content="hello world",
            to_addr="1111111111")
        sequence_num4 = self.esme.getSeq()
        response4 = SubmitSMResp(sequence_num4, "3rd_party_id_4",
                command_status="ESME_RTHROTTLED")
        self.transport._process_message(message4)
        self.esme.handleData(response4.get_bin())
        self.assertEqual([self.mkmsg_ack('447', '3rd_party_id_4')],
                         self.get_dispatched_events()[3:])
        self.assertTrue(self.transport.throttle_invoked_via_pdu)

        fail_msg = self.mkmsg_out(
            message_id=555,
            content="hello world",
            to_addr="1111111111")

        self.transport.send_failure(fail_msg, Exception("Foo"), "testing")

        self.assertEqual([self.mkmsg_fail(fail_msg.payload, "testing")],
                         self.get_dispatched_failures()[1:])

        # Some error codes would occur on bind attempts
        bind_dispatch_methods = {
            "ESME_ROK": self.transport.ok,
            "ESME_RINVBNDSTS": self.transport.conn_tempfault,
            "ESME_RALYBND": self.transport.conn_tempfault,
            "ESME_RSYSERR": self.transport.conn_permfault,
            "ESME_RBINDFAIL": self.transport.conn_permfault,
            "ESME_RINVPASWD": self.transport.conn_permfault,
            "ESME_RINVSYSID": self.transport.conn_permfault,
            "ESME_RINVSERTYP": self.transport.conn_permfault,
        }

        # Some error codes would occur post bind i.e. on submission attempts
        submit_dispatch_methods = {
            "ESME_RINVMSGLEN": self.transport.mess_permfault,
            "ESME_RINVCMDLEN": self.transport.mess_permfault,
            "ESME_RINVCMDID": self.transport.mess_permfault,

            "ESME_RINVPRTFLG": self.transport.mess_permfault,
            "ESME_RINVREGDLVFLG": self.transport.mess_permfault,

            "ESME_RINVSRCADR": self.transport.mess_permfault,
            "ESME_RINVDSTADR": self.transport.mess_permfault,
            "ESME_RINVMSGID": self.transport.mess_permfault,

            "ESME_RCANCELFAIL": self.transport.mess_permfault,
            "ESME_RREPLACEFAIL": self.transport.mess_permfault,

            "ESME_RMSGQFUL": self.transport.conn_throttle,

            "ESME_RINVNUMDESTS": self.transport.mess_permfault,
            "ESME_RINVDLNAME": self.transport.mess_permfault,
            "ESME_RINVDESTFLAG": self.transport.mess_permfault,
            "ESME_RINVSUBREP": self.transport.mess_permfault,
            "ESME_RINVESMCLASS": self.transport.mess_permfault,
            "ESME_RCNTSUBDL": self.transport.mess_permfault,

            "ESME_RSUBMITFAIL": self.transport.mess_tempfault,

            "ESME_RINVSRCTON": self.transport.mess_permfault,
            "ESME_RINVSRCNPI": self.transport.mess_permfault,
            "ESME_RINVDSTTON": self.transport.mess_permfault,
            "ESME_RINVDSTNPI": self.transport.mess_permfault,
            "ESME_RINVSYSTYP": self.transport.conn_permfault,
            "ESME_RINVREPFLAG": self.transport.mess_permfault,

            "ESME_RINVNUMMSGS": self.transport.mess_tempfault,

            "ESME_RTHROTTLED": self.transport.conn_throttle,

            "ESME_RINVSCHED": self.transport.mess_permfault,
            "ESME_RINVEXPIRY": self.transport.mess_permfault,
            "ESME_RINVDFTMSGID": self.transport.mess_permfault,

            "ESME_RX_T_APPN": self.transport.mess_tempfault,

            "ESME_RX_P_APPN": self.transport.mess_permfault,
            "ESME_RX_R_APPN": self.transport.mess_permfault,
            "ESME_RQUERYFAIL": self.transport.mess_permfault,
            "ESME_RINVOPTPARSTREAM": self.transport.mess_permfault,
            "ESME_ROPTPARNOTALLWD": self.transport.mess_permfault,
            "ESME_RINVPARLEN": self.transport.mess_permfault,
            "ESME_RMISSINGOPTPARAM": self.transport.mess_permfault,
            "ESME_RINVOPTPARAMVAL": self.transport.mess_permfault,

            "ESME_RDELIVERYFAILURE": self.transport.mess_tempfault,
            "ESME_RUNKNOWNERR": self.transport.mess_tempfault,
        }

        newfangled_fake_error = {
            "ESME_NEWFNGLEDFAKERR": self.esme.dummy_unknown,
        }

        for code, method in bind_dispatch_methods.items():
            response = BindTransceiverResp(1, code)
            # check the dispatcher returns the correct transport method
            self.assertEquals(method,
                    self.esme.command_status_dispatch(response.get_obj()))

        for code, method in submit_dispatch_methods.items():
            response = SubmitSMResp(1, "2", code)
            # check the dispatcher returns the correct transport method
            self.assertEquals(method,
                    self.esme.command_status_dispatch(response.get_obj()))

        for code, method in newfangled_fake_error.items():
            response = SubmitSMResp(1, "2", code)
            # check the dispatcher returns the correct transport method
            self.assertEquals(method,
                    self.esme.command_status_dispatch(response.get_obj()))
