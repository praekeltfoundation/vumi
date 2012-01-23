import json
import pytz
from datetime import datetime

from twisted.trial.unittest import TestCase

from vumi.tests.utils import RegexMatcher, UTCNearNow
from vumi.message import (Message, TransportMessage, TransportEvent,
                          TransportUserMessage, VUMI_DATE_FORMAT)


class MessageTest(TestCase):

    def test_message_equality(self):
        self.assertEqual(Message(a=5), Message(a=5))
        self.assertNotEqual(Message(a=5), Message(b=5))
        self.assertNotEqual(Message(a=5), Message(a=6))
        self.assertNotEqual(Message(a=5), {'a': 5})

    def test_transport_message(self):
        msg = TransportMessage(
            message_type='foo',
            )
        self.assertEqual('foo', msg['message_type'])
        self.assertEqual('20110921', msg['message_version'])
        self.assertEqual(UTCNearNow(), msg['timestamp'])

    def test_transport_user_message_basic(self):
        msg = TransportUserMessage(
            message_id='abc',
            to_addr='+27831234567',
            from_addr='12345',
            content='heya',
            transport_name='sphex',
            transport_type='sms',
            transport_metadata={},
            )
        self.assertEqual('user_message', msg['message_type'])
        self.assertEqual('sms', msg['transport_type'])
        self.assertEqual('abc', msg['message_id'])
        self.assertEqual('20110921', msg['message_version'])
        self.assertEqual('heya', msg['content'])
        self.assertEqual('sphex', msg['transport_name'])
        self.assertEqual({}, msg['transport_metadata'])
        self.assertEqual(UTCNearNow(), msg['timestamp'])
        self.assertEqual('+27831234567', msg['to_addr'])
        self.assertEqual('12345', msg['from_addr'])

    def test_transport_user_message_defaults(self):
        msg = TransportUserMessage(
            to_addr='+27831234567',
            from_addr='12345',
            transport_name='sphex',
            transport_type='sms',
            transport_metadata={},
            )
        self.assertEqual('user_message', msg['message_type'])
        self.assertEqual('sms', msg['transport_type'])
        self.assertEqual(RegexMatcher(r'^[0-9a-fA-F]{32}$'), msg['message_id'])
        self.assertEqual('20110921', msg['message_version'])
        self.assertEqual(None, msg['content'])
        self.assertEqual('sphex', msg['transport_name'])
        self.assertEqual({}, msg['transport_metadata'])
        self.assertEqual(UTCNearNow(), msg['timestamp'])
        self.assertEqual('+27831234567', msg['to_addr'])
        self.assertEqual('12345', msg['from_addr'])

    def test_transport_event_ack(self):
        msg = TransportEvent(
            event_id='def',
            event_type='ack',
            user_message_id='abc',
            # transport_name='sphex',
            sent_message_id='ghi',
            )
        self.assertEqual('event', msg['message_type'])
        self.assertEqual('ack', msg['event_type'])
        self.assertEqual('def', msg['event_id'])
        self.assertEqual('abc', msg['user_message_id'])
        self.assertEqual('20110921', msg['message_version'])
        # self.assertEqual('sphex', msg['transport_name'])
        self.assertEqual('ghi', msg['sent_message_id'])

    def test_transport_event_delivery_report(self):
        msg = TransportEvent(
            event_id='def',
            event_type='delivery_report',
            user_message_id='abc',
            to_addr='+27831234567',
            from_addr='12345',
            # transport_name='sphex',
            delivery_status='delivered',
            )
        self.assertEqual('event', msg['message_type'])
        self.assertEqual('delivery_report', msg['event_type'])
        self.assertEqual('def', msg['event_id'])
        self.assertEqual('abc', msg['user_message_id'])
        self.assertEqual('20110921', msg['message_version'])
        # self.assertEqual('sphex', msg['transport_name'])
        self.assertEqual('delivered', msg['delivery_status'])

    def test_date_decoding(self):
        defaults = {
            u'transport_name': u'sphex',
            u'transport_metadata': {},
            u'from_addr': u'12345',
            u'to_addr': u'+27831234567',
            u'message_id': u'c2fe343785b74b17a19d8f415c082c95',
            u'content': None,
            u'message_version': u'20110921',
            u'transport_type': u'sms',
            u'helper_metadata': {},
            u'in_reply_to': None,
            u'session_event': None,
            u'message_type': u'user_message'
        }

        timestamp = datetime(2012,1,23,17,17,13, tzinfo=pytz.UTC)

        old_format = defaults.copy()
        old_format.update({
            'timestamp': timestamp.strftime(VUMI_DATE_FORMAT)
        })
        old_format_json = json.dumps(old_format)

        new_format = defaults.copy()
        new_format.update({
            'timestamp': timestamp.isoformat()
        })
        new_format_json = json.dumps(new_format)

        msg = TransportUserMessage.from_json(old_format_json)
        self.assertEqual(msg['timestamp'], timestamp)

        msg = TransportUserMessage.from_json(new_format_json)
        self.assertEqual(msg['timestamp'], timestamp)
