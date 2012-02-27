from twisted.trial.unittest import TestCase

from vumi.tests.utils import RegexMatcher, UTCNearNow
from vumi.message import (Message, TransportMessage, TransportEvent,
                          TransportUserMessage)


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

    def test_transport_user_message_reply_no_group(self):
        msg = TransportUserMessage(
            to_addr='123',
            from_addr='456',
            transport_name='sphex',
            transport_type='sms',
            transport_metadata={'foo': 'bar'},
            helper_metadata={'otherfoo': 'otherbar'},
            )
        reply = msg.reply(content='Hi!')
        self.assertEqual(reply['from_addr'], '123')
        self.assertEqual(reply['to_addr'], '456')
        self.assertEqual(reply['group'], None)
        self.assertEqual(reply['session_event'], reply.SESSION_NONE)
        self.assertEqual(reply['in_reply_to'], msg['message_id'])
        self.assertEqual(reply['transport_name'], msg['transport_name'])
        self.assertEqual(reply['transport_type'], msg['transport_type'])
        self.assertEqual(reply['transport_metadata'],
                         msg['transport_metadata'])
        self.assertEqual(reply['helper_metadata'], msg['helper_metadata'])

    def test_transport_user_message_reply_undirected_group(self):
        msg = TransportUserMessage(
            to_addr=None,
            from_addr='456',
            group='#channel',
            transport_name='sphex',
            transport_type='irc',
            transport_metadata={'foo': 'bar'},
            helper_metadata={'otherfoo': 'otherbar'},
            )
        reply = msg.reply(content='Hi!')
        self.assertEqual(reply['from_addr'], None)
        self.assertEqual(reply['to_addr'], '456')
        self.assertEqual(reply['group'], '#channel')
        self.assertEqual(reply['session_event'], reply.SESSION_NONE)
        self.assertEqual(reply['in_reply_to'], msg['message_id'])
        self.assertEqual(reply['transport_name'], msg['transport_name'])
        self.assertEqual(reply['transport_type'], msg['transport_type'])
        self.assertEqual(reply['transport_metadata'],
                         msg['transport_metadata'])
        self.assertEqual(reply['helper_metadata'], msg['helper_metadata'])

    def test_transport_user_message_reply_directed_group(self):
        msg = TransportUserMessage(
            to_addr='123',
            from_addr='456',
            group='#channel',
            transport_name='sphex',
            transport_type='sms',
            transport_metadata={'foo': 'bar'},
            helper_metadata={'otherfoo': 'otherbar'},
            )
        reply = msg.reply(content='Hi!')
        self.assertEqual(reply['from_addr'], '123')
        self.assertEqual(reply['to_addr'], '456')
        self.assertEqual(reply['group'], '#channel')
        self.assertEqual(reply['session_event'], reply.SESSION_NONE)
        self.assertEqual(reply['in_reply_to'], msg['message_id'])
        self.assertEqual(reply['transport_name'], msg['transport_name'])
        self.assertEqual(reply['transport_type'], msg['transport_type'])
        self.assertEqual(reply['transport_metadata'],
                         msg['transport_metadata'])
        self.assertEqual(reply['helper_metadata'], msg['helper_metadata'])

    def test_transport_user_message_reply_group_no_group(self):
        msg = TransportUserMessage(
            to_addr='123',
            from_addr='456',
            transport_name='sphex',
            transport_type='sms',
            transport_metadata={'foo': 'bar'},
            helper_metadata={'otherfoo': 'otherbar'},
            )
        reply = msg.reply_group(content='Hi!')
        self.assertEqual(reply['from_addr'], '123')
        self.assertEqual(reply['to_addr'], '456')
        self.assertEqual(reply['group'], None)
        self.assertEqual(reply['session_event'], reply.SESSION_NONE)
        self.assertEqual(reply['in_reply_to'], msg['message_id'])
        self.assertEqual(reply['transport_name'], msg['transport_name'])
        self.assertEqual(reply['transport_type'], msg['transport_type'])
        self.assertEqual(reply['transport_metadata'],
                         msg['transport_metadata'])
        self.assertEqual(reply['helper_metadata'], msg['helper_metadata'])

    def test_transport_user_message_reply_group_undirected_group(self):
        msg = TransportUserMessage(
            to_addr=None,
            from_addr='456',
            group='#channel',
            transport_name='sphex',
            transport_type='irc',
            transport_metadata={'foo': 'bar'},
            helper_metadata={'otherfoo': 'otherbar'},
            )
        reply = msg.reply_group(content='Hi!')
        self.assertEqual(reply['from_addr'], None)
        self.assertEqual(reply['to_addr'], None)
        self.assertEqual(reply['group'], '#channel')
        self.assertEqual(reply['session_event'], reply.SESSION_NONE)
        self.assertEqual(reply['in_reply_to'], msg['message_id'])
        self.assertEqual(reply['transport_name'], msg['transport_name'])
        self.assertEqual(reply['transport_type'], msg['transport_type'])
        self.assertEqual(reply['transport_metadata'],
                         msg['transport_metadata'])
        self.assertEqual(reply['helper_metadata'], msg['helper_metadata'])

    def test_transport_user_message_reply_group_directed_group(self):
        msg = TransportUserMessage(
            to_addr='123',
            from_addr='456',
            group='#channel',
            transport_name='sphex',
            transport_type='sms',
            transport_metadata={'foo': 'bar'},
            helper_metadata={'otherfoo': 'otherbar'},
            )
        reply = msg.reply_group(content='Hi!')
        self.assertEqual(reply['from_addr'], '123')
        self.assertEqual(reply['to_addr'], None)
        self.assertEqual(reply['group'], '#channel')
        self.assertEqual(reply['session_event'], reply.SESSION_NONE)
        self.assertEqual(reply['in_reply_to'], msg['message_id'])
        self.assertEqual(reply['transport_name'], msg['transport_name'])
        self.assertEqual(reply['transport_type'], msg['transport_type'])
        self.assertEqual(reply['transport_metadata'],
                         msg['transport_metadata'])
        self.assertEqual(reply['helper_metadata'], msg['helper_metadata'])

    def test_transport_user_message_send(self):
        msg = TransportUserMessage.send('123', 'Hi!')
        self.assertEqual(msg['to_addr'], '123')
        self.assertEqual(msg['from_addr'], None)
        self.assertEqual(msg['session_event'], msg.SESSION_NONE)
        self.assertEqual(msg['in_reply_to'], None)
        self.assertEqual(msg['transport_name'], None)
        self.assertEqual(msg['transport_type'], None)
        self.assertEqual(msg['transport_metadata'], {})
        self.assertEqual(msg['helper_metadata'], {})

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
