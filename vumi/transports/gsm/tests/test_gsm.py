# -*- coding: utf-8 -*-
from twisted.internet import defer
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

        messages = yield self.transport.receive_and_send_messages(phone)
        self.assertEqual([], self.get_dispatched_failures())
        self.assertEqual([], self.get_dispatched_events())
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['to_addr'], '+%s' % self.to_addr)
        self.assertEqual(msg['content'], 'hello world')
        self.assertEqual(phone.messages, [])

    @inlineCallbacks
    def test_sending_sms_success(self):
        phone = FakeGammuPhone([])
        # generate a fake message
        msg = self.mk_msg()
        # dispatch as if being sent from an application
        yield self.dispatch(msg, rkey='%s.outbound' % self.transport_name)
        # the transport should store them in redis until the next
        # send & receive loop is triggered
        queue_name = self.transport.redis_outbound_queue
        self.assertEqual(self.transport.r_server.llen(queue_name), 1)
        # trigger manually
        messages = yield self.transport.receive_and_send_messages(phone)
        # all should be happy
        self.assertEqual(self.get_dispatched_failures(), [])
        # the phone's outbox should have the messages
        self.assertEqual(phone.outbox, [{
            'Type': 'Status_Report',
            'MessageReference': msg['message_id'],
            'SMSC': {
                'Location': 1,
            },
            'Text': msg['content'],
            'Number': msg['to_addr'],
        }])
        # the redis list should be empty
        self.assertEqual(self.transport.r_server.llen(queue_name), 0)

    @inlineCallbacks
    def test_sending_sms_failure(self):
        phone = FailingFakeGammuPhone([])
        msg = self.mk_msg()
        yield self.dispatch(msg, rkey='%s.outbound' % self.transport_name)
        messages = yield self.transport.receive_and_send_messages(phone)
        self.assertEqual([], self.get_dispatched_messages())
        [failure] = self.get_dispatched_failures()
        self.assertEqual(failure['message'], msg)
        self.assertEqual(failure['message_type'], FailureMessage.MESSAGE_TYPE)
        self.assertEqual(failure['failure_code'], FailureMessage.FC_UNSPECIFIED)

    @inlineCallbacks
    def test_delivery_reports(self):
        phone = FakeGammuPhone([{
            'Type': 'Status_Report',
            'Location': 0,
            'MessageReference': '123456',
            'DeliveryStatus': 'D',
        }])
        messages = yield self.transport.receive_and_send_messages(phone)
        self.assertEqual([], self.get_dispatched_failures())
        self.assertEqual([], self.get_dispatched_messages())
        [event] = self.get_dispatched_events()
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')

