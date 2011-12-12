# -*- coding: utf-8 -*-
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks
from vumi.message import TransportUserMessage
from vumi.tests.utils import FakeRedis
from vumi.transports.gsm.tests.test_gsm_stubs import (
    FakeGSMTransport, FakeGammuPhone)
from vumi.transports.tests.test_base import TransportTestCase
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

        self.phone = FakeGammuPhone([{
            'Location': 0,
            'Text': 'hello world',
            'Number': '27761234567',
            'Type': 'Deliver',
            'DateTime': datetime.now(),
            'SMSCDateTime': datetime.now(),
        }])

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
        messages = yield self.transport.receive_and_send_messages(self.phone)
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['to_addr'], '+%s' % self.to_addr)
        self.assertEqual(msg['content'], 'hello world')
        self.assertEqual(self.phone.messages, [])

    @inlineCallbacks
    def test_sending_sms(self):
        msg = self.mk_msg()
        yield self.dispatch(msg,
            rkey='%s.outbound' % self.transport_name)
        messages = yield self.transport.receive_and_send_messages(self.phone)
        self.assertEqual(self.get_dispatched_failures(), [])
        self.assertEqual(self.phone.outbox, [{
            'Type': 'Status_Report',
            'MessageReference': msg['message_id'],
            'SMSC': {
                'Location': 1,
            },
            'Text': msg['content'],
            'Number': msg['to_addr'],
        }])

