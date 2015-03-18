from datetime import datetime
import json

from vumi.tests.utils import RegexMatcher, UTCNearNow
from vumi.message import (
    Message, TransportMessage, TransportEvent, TransportUserMessage,
    format_vumi_date, parse_vumi_date, from_json, to_json)
from vumi.tests.helpers import VumiTestCase


class ModuleUtilityTest(VumiTestCase):

    def test_parse_vumi_date(self):
        self.assertEqual(
            parse_vumi_date('2015-01-02 23:14:11.456000'),
            datetime(2015, 1, 2, 23, 14, 11, microsecond=456000))

    def test_parse_vumi_date_no_microseconds(self):
        """
        We can parse a timestamp even if it has no microseconds.
        """
        self.assertEqual(
            parse_vumi_date('2015-01-02 23:14:11'),
            datetime(2015, 1, 2, 23, 14, 11, microsecond=0))

    def test_format_vumi_date(self):
        self.assertEqual(
            format_vumi_date(
                datetime(2015, 1, 2, 23, 14, 11, microsecond=456000)),
            '2015-01-02 23:14:11.456000')
        self.assertEqual(
            format_vumi_date(
                datetime(2015, 1, 2, 23, 14, 11, microsecond=0)),
            '2015-01-02 23:14:11.000000')

    def test_from_json(self):
        data = {
            'foo': 1,
            'baz': {
                'a': 'b',
            }
        }
        self.assertEqual(from_json(json.dumps(data)), data)

    def test_to_json(self):
        data = {
            'foo': 1,
            'baz': {
                'a': 'b',
            }
        }
        self.assertEqual(json.loads(to_json(data)), data)

    def test_to_json_supports_vumi_dates(self):
        timestamp = datetime(
            2015, 1, 2, 12, 01, 02, microsecond=134001)
        data = {
            'foo': timestamp,
        }
        self.assertEqual(json.loads(to_json(data)), {
            'foo': '2015-01-02 12:01:02.134001',
        })

    def test_from_json_supports_vumi_dates(self):
        timestamp = datetime(
            2015, 1, 2, 12, 01, 02, microsecond=134002)
        data = {
            'foo': '2015-01-02 12:01:02.134002',
        }
        self.assertEqual(from_json(json.dumps(data)), {
            'foo': timestamp,
        })


class MessageTest(VumiTestCase):

    def test_message_equality(self):
        self.assertEqual(Message(a=5), Message(a=5))
        self.assertNotEqual(Message(a=5), Message(b=5))
        self.assertNotEqual(Message(a=5), Message(a=6))
        self.assertNotEqual(Message(a=5), {'a': 5})

    def test_message_contains(self):
        self.assertTrue('a' in Message(a=5))
        self.assertFalse('a' in Message(b=5))

    def test_message_cache(self):
        msg = Message(a=5)
        self.assertEqual(msg.cache, {})
        msg.cache["thing"] = "dont_store_me"
        self.assertEqual(msg.cache, {
            "thing": "dont_store_me",
        })
        self.assertEqual(msg[Message._CACHE_ATTRIBUTE], {
            "thing": "dont_store_me",
        })


class TransportMessageTestMixin(object):
    def make_message(self, **fields):
        raise NotImplementedError()

    def test_transport_message_fields(self):
        msg = self.make_message()
        self.assertEqual('20110921', msg['message_version'])
        self.assertEqual(UTCNearNow(), msg['timestamp'])

    def test_helper_metadata(self):
        self.assertEqual({}, self.make_message()['helper_metadata'])
        msg = self.make_message(helper_metadata={'foo': 'bar'})
        self.assertEqual({'foo': 'bar'}, msg['helper_metadata'])

    def test_routing_metadata(self):
        self.assertEqual({}, self.make_message().routing_metadata)
        msg = self.make_message(routing_metadata={'foo': 'bar'})
        self.assertEqual({'foo': 'bar'}, msg.routing_metadata)

    def test_check_routing_endpoint(self):
        msgcls = type(self.make_message())
        self.assertEqual('default', msgcls.check_routing_endpoint(None))
        self.assertEqual('foo', msgcls.check_routing_endpoint('foo'))

    def test_get_routing_endpoint(self):
        msg = self.make_message()
        self.assertEqual({}, msg.routing_metadata)
        self.assertEqual('default', msg.get_routing_endpoint())
        msg.routing_metadata['endpoint_name'] = None
        self.assertEqual('default', msg.get_routing_endpoint())
        msg.routing_metadata['endpoint_name'] = 'foo'
        self.assertEqual('foo', msg.get_routing_endpoint())

    def test_set_routing_endpoint(self):
        msg = self.make_message()
        self.assertEqual({}, msg.routing_metadata)
        msg.set_routing_endpoint(None)
        self.assertEqual('default', msg.routing_metadata['endpoint_name'])
        msg.set_routing_endpoint('foo')
        self.assertEqual('foo', msg.routing_metadata['endpoint_name'])


class TransportMessageTest(TransportMessageTestMixin, VumiTestCase):
    def make_message(self, **extra_fields):
        fields = dict(message_type='foo')
        fields.update(extra_fields)
        return TransportMessage(**fields)

    def test_transport_message(self):
        msg = TransportMessage(
            message_type='foo',
            )
        self.assertEqual('foo', msg['message_type'])


class TransportUserMessageTest(TransportMessageTestMixin, VumiTestCase):
    def make_message(self, **extra_fields):
        fields = dict(
            # message_id='abc',
            to_addr='+27831234567',
            from_addr='12345',
            # content='heya',
            transport_name='sphex',
            transport_type='sms',
            # transport_metadata={},
            )
        fields.update(extra_fields)
        return TransportUserMessage(**fields)

    def test_transport_user_message_basic(self):
        msg = TransportUserMessage(
            message_id='abc',
            to_addr='+27831234567',
            from_addr='12345',
            content='heya',
            transport_name='sphex',
            transport_type='sms',
            transport_metadata={},
            from_addr_type='twitter_handle',
            to_addr_type='gtalk_id',
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
        self.assertEqual('twitter_handle', msg['from_addr_type'])
        self.assertEqual('gtalk_id', msg['to_addr_type'])

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
        self.assertEqual(None, msg['to_addr_type'])
        self.assertEqual(None, msg['from_addr_type'])

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
        self.assertEqual(reply['provider'], msg['provider'])

    def test_transport_user_message_reply_no_provider(self):
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
        self.assertEqual(reply['provider'], None)

    def test_transport_user_message_reply_with_provider(self):
        msg = TransportUserMessage(
            to_addr='123',
            from_addr='456',
            transport_name='sphex',
            transport_type='sms',
            transport_metadata={'foo': 'bar'},
            helper_metadata={'otherfoo': 'otherbar'},
            provider='MNO',
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
        self.assertEqual(reply['provider'], msg['provider'])

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

    def test_transport_user_message_send_with_session_event(self):
        msg = TransportUserMessage.send(
            '123', 'Hi!', session_event=TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['to_addr'], '123')
        self.assertEqual(msg['from_addr'], None)
        self.assertEqual(msg['session_event'], msg.SESSION_NEW)
        self.assertEqual(msg['in_reply_to'], None)
        self.assertEqual(msg['transport_name'], None)
        self.assertEqual(msg['transport_type'], None)
        self.assertEqual(msg['transport_metadata'], {})
        self.assertEqual(msg['helper_metadata'], {})


class TransportEventTest(TransportMessageTestMixin, VumiTestCase):
    def make_message(self, **extra_fields):
        fields = dict(
            event_id='def',
            event_type='ack',
            user_message_id='abc',
            sent_message_id='ghi',
            )
        fields.update(extra_fields)
        return TransportEvent(**fields)

    def test_transport_event_ack(self):
        msg = TransportEvent(
            event_id='def',
            event_type='ack',
            user_message_id='abc',
            sent_message_id='ghi',
            )
        self.assertEqual('event', msg['message_type'])
        self.assertEqual('ack', msg['event_type'])
        self.assertEqual('def', msg['event_id'])
        self.assertEqual('abc', msg['user_message_id'])
        self.assertEqual('20110921', msg['message_version'])
        self.assertEqual('ghi', msg['sent_message_id'])
        self.assertEqual({}, msg['helper_metadata'])

    def test_transport_event_nack(self):
        msg = TransportEvent(
            event_id='def',
            event_type='nack',
            user_message_id='abc',
            nack_reason='unknown',
            )
        self.assertEqual('event', msg['message_type'])
        self.assertEqual('nack', msg['event_type'])
        self.assertEqual('unknown', msg['nack_reason'])
        self.assertEqual('def', msg['event_id'])
        self.assertEqual('abc', msg['user_message_id'])
        self.assertEqual('20110921', msg['message_version'])
        self.assertEqual({}, msg['helper_metadata'])

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
        self.assertEqual({}, msg['helper_metadata'])
