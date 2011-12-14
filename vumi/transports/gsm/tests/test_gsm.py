# -*- coding: utf-8 -*-
from twisted.trial.unittest import SkipTest
from twisted.internet.defer import inlineCallbacks
from vumi.message import TransportUserMessage
from vumi.transports.gsm.tests.test_gsm_stubs import (
    FakeGSMTransport, FakeGammuPhone, FailingFakeGammuPhone)
from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.failures import FailureMessage
from datetime import datetime


class GSMTransportTestCase(TransportTestCase):

    transport_class = FakeGSMTransport
    transport_name = 'fake_gsm_transport'
    to_addr = '27761234567'

    @inlineCallbacks
    def setUp(self):
        yield super(GSMTransportTestCase, self).setUp()
        self.transport = yield self.get_transport({
            'transport_name': self.transport_name,
            'phone_number': self.to_addr,
            'country_code': 27,
            'poll_interval': 120,
            'gammu': {
                'UseGlobalDebugFile': 0,
                'DebugFile': '',
                'SyncTime': 0,
                'Connection': 'at',
                'LockDevice': 0,
                'DebugLevel': '',
                'Device': '/dev/path-to-modem',
                'StartInfo': 0,
                'Model': ''
            }
        })

    def tearDown(self):
        self.transport.r_server.teardown()

    def mk_msg(self, **kwargs):
        defaults = {
            'to_addr': '27761234567',
            'from_addr': '27761234567',
            'content': 'hello world',
            'transport_name': self.transport_name,
            'transport_type': 'sms',
            'transport_metadata': {},
        }
        defaults.update(kwargs)
        return TransportUserMessage(**defaults)

    @inlineCallbacks
    def test_receiving_sms(self):
        phone = FakeGammuPhone([{
            'Location': 0,
            'Text': 'hello world',
            'Number': '27761234567',
            'Type': 'Deliver',
            'DateTime': datetime.now(),
            'SMSCDateTime': datetime.now(),
        }])

        yield self.transport.receive_and_send_messages(phone)
        self.assertEqual([], self.get_dispatched_failures())
        self.assertEqual([], self.get_dispatched_events())
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['to_addr'], '+%s' % self.to_addr)
        self.assertEqual(msg['content'], 'hello world')
        self.assertEqual(phone.messages, [])

    @inlineCallbacks
    def test_sending_sms_success(self):
        phone = FakeGammuPhone()
        # generate a fake message
        msg = self.mk_msg()
        # dispatch as if being sent from an application
        yield self.dispatch(msg, rkey='%s.outbound' % self.transport_name)
        # the transport should store them in redis until the next
        # send & receive loop is triggered
        queue_name = self.transport.redis_outbound_queue
        key = self.transport.r_key(queue_name)
        self.assertEqual(self.transport.r_server.llen(key), 1)
        # trigger manually
        yield self.transport.receive_and_send_messages(phone)
        # all should be happy
        self.assertEqual(self.get_dispatched_failures(), [])
        # the phone's outbox should have the messages
        self.assertEqual(phone.outbox, [{
            'Type': 'Status_Report',
            'SMSC': {
                'Location': 1,
            },
            'Text': msg['content'],
            'Number': msg['to_addr'],
        }])
        # the redis list should be empty
        self.assertEqual(self.transport.r_server.llen(key), 0)

    @inlineCallbacks
    def test_sending_multipart_sms(self):
        phone = FakeGammuPhone()
        msg = self.mk_msg()
        msg['content'] = 'a' * 200  # doesn't fit in a single SMS
        yield self.dispatch(msg, rkey='%s.outbound' % self.transport_name)
        yield self.transport.receive_and_send_messages(phone)
        self.assertEqual(len(phone.outbox), 2)
        self.assertEqual(phone.outbox[0]['Text'], 'a' * 153)
        self.assertEqual(phone.outbox[0]['UDH']['PartNumber'], 1)
        self.assertEqual(phone.outbox[0]['UDH']['AllParts'], 2)
        self.assertEqual(phone.outbox[1]['Text'], 'a' * 47)
        self.assertEqual(phone.outbox[1]['UDH']['PartNumber'], 2)
        self.assertEqual(phone.outbox[1]['UDH']['AllParts'], 2)

    @inlineCallbacks
    def test_receiving_multipart_sms(self):
        phone = FakeGammuPhone([{
            'MessageReference': u'9c103356fbf243b3805db555e4f9c2d3',
            'Number': u'27761234567',
            'DateTime': datetime.now(),
            'SMSCDateTime': datetime.now(),
            'Text': u'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
                    u'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
                    u'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
                    u'aaaaaaaaaa',
            'Type': 'Deliver',
            'Location': 0,
            'UDH': {
                'AllParts': 2,
                'PartNumber': 1,
                'Text': '\x05\x00\x03\x19\x02\x01',
                'Type': 'ConcatenatedMessages'
            }
        }, {
            'MessageReference': u'9c103356fbf243b3805db555e4f9c2d3',
            'Number': u'27761234567',
            'DateTime': datetime.now(),
            'SMSCDateTime': datetime.now(),
            'Text': u'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'Type': 'Deliver',
            'Location': 1,
            'UDH': {
                'AllParts': 2,
                'PartNumber': 2,
                'Text': '\x05\x00\x03\x19\x02\x02',
                'Type': 'ConcatenatedMessages'
            }
        }])
        yield self.transport.receive_and_send_messages(phone)
        self.assertEqual([], self.get_dispatched_failures())
        self.assertEqual([], self.get_dispatched_events())
        [message] = self.get_dispatched_messages()
        self.assertEqual(message['content'], 'a' * 200)
        self.assertEqual(message['from_addr'], '+27761234567')

    @inlineCallbacks
    def test_sending_sms_failure(self):
        phone = FailingFakeGammuPhone()
        msg = self.mk_msg()
        yield self.dispatch(msg, rkey='%s.outbound' % self.transport_name)
        yield self.transport.receive_and_send_messages(phone)
        self.assertEqual([], self.get_dispatched_messages())
        self.assertEqual([], self.get_dispatched_events())
        [failure] = self.get_dispatched_failures()
        self.assertEqual(failure['message'], msg.payload)
        self.assertEqual(failure['message_type'],
                            FailureMessage.MESSAGE_TYPE)
        self.assertEqual(failure['failure_code'],
                            FailureMessage.FC_UNSPECIFIED)

    @inlineCallbacks
    def test_delivery_reports(self):
        # fake an outbound delivery first
        phone = FakeGammuPhone()
        msg = self.mk_msg()
        yield self.dispatch(msg, rkey='%s.outbound' % self.transport_name)
        # have the phone handle it
        received, sent = yield self.transport.receive_and_send_messages(phone)
        # test correct behaviour
        self.assertEqual([], received)
        # expand the sent messages, should be a vumi message and a dict of
        # MessageReference -> GammuMessage mappings
        [(vumi_msg, gammu_messages)] = sent
        self.assertEqual(vumi_msg, msg)
        self.assertEqual(gammu_messages.keys(), [1]) # reference number given by modem
        self.assertEqual(gammu_messages[1]['Text'], msg['content'])

        # Now fake the delivery report for the given reference number
        phone = FakeGammuPhone([{
            'SMSCDateTime': datetime.now(),
            'Class': 0,
            'Text': u'Delivered',
            'Number': u'+27764493806',
            'DateTime': datetime.now(),
            'MessageReference': 1,
            'Length': 9,
            'Location': 0,
            'Type': 'Status_Report',
        }])

        # received & sent
        [delivery_report], [] = yield self.transport.receive_and_send_messages(phone)
        self.assertEqual(delivery_report['Text'], 'Delivered')
        self.assertEqual(delivery_report['MessageReference'], 1)

        [delivery_report] = self.get_dispatched_events()
        self.assertEqual(delivery_report['delivery_status'], 'delivered')
        self.assertEqual(delivery_report['user_message_id'], msg['message_id'])

