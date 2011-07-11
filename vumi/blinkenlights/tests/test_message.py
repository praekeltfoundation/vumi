from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.blinkenlights import message20110707 as message

def mkmsg(message_version, message_type, source_name, source_id, payload, timestamp):
    return {
        "message_version": message_version,
        "message_type": message_type,
        "source_name": source_name,
        "source_id": source_id,
        "payload": payload,
        "timestamp": timestamp,
        }

def mkmsgobj(message_type, source_name, source_id, payload, timestamp):
    return message.Message(message_type, source_name, source_id, payload, timestamp)


class MessageTestCase(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_decode_valid_message(self):
        """A valid message dict should decode into an appropriate Message object.
        """
        msg_data = mkmsg("20110707", "custom", "myworker", "abc123", ["foo"], None)
        msg = message.Message.from_dict(msg_data)

        self.assertEquals("custom", msg.message_type)
        self.assertEquals("myworker", msg.source_name)
        self.assertEquals("abc123", msg.source_id)
        self.assertEquals(["foo"], msg.payload)
        self.assertEquals(None, msg.timestamp)

    def test_encode_valid_message(self):
        """A Message object should encode into an appropriate message dict.
        """
        msg_data = mkmsg("20110707", "custom", "myworker", "abc123", ["foo"], None)
        msg = message.Message("custom", "myworker", "abc123", ["foo"], None)

        self.assertEquals(msg_data, msg.to_dict())

    def test_decode_invalid_messages(self):
        """Various kinds of invalid messages should fail to decode.
        """
        msg_data = mkmsg("19800902", "custom", "myworker", "abc123", ["foo"], None)
        self.assertRaises(ValueError, message.Message.from_dict, msg_data)

        msg_data = mkmsg("20110707", "custom", "myworker", "abc123", ["foo"], None)
        msg_data.pop('payload')
        self.assertRaises(ValueError, message.Message.from_dict, msg_data)

        msg_data = mkmsg("20110707", "custom", "myworker", "abc123", ["foo"], None)
        msg_data['foo'] = 'bar'
        self.assertRaises(ValueError, message.Message.from_dict, msg_data)

    def test_message_equality(self):
        """Identical messages should compare equal. Different messages should not.
        """
        msg_data = mkmsg("20110707", "custom", "myworker", "abc123", ["foo"], None)
        msg1 = message.Message.from_dict(msg_data.copy())
        msg2 = message.Message.from_dict(msg_data.copy())
        msg3 = message.Message.from_dict(msg1.to_dict())
        diff_msgs = [
            mkmsgobj("custom1", "myworker", "abc123", ["foo"], None),
            mkmsgobj("custom", "myworker1", "abc123", ["foo"], None),
            mkmsgobj("custom", "myworker", "abc1231", ["foo"], None),
            mkmsgobj("custom", "myworker", "abc123", ["foo1"], None),
            ]

        self.assertEquals(msg1, msg1)
        self.assertEquals(msg1, msg2)
        self.assertEquals(msg1, msg3)
        for msg in diff_msgs:
            self.assertNotEquals(msg1, msg)


def mkmetricsmsg(metrics):
    payload = metrics
    return mkmsg("20110707", "metrics", "myworker", "abc123", payload, None)


class MetricsMessageTestCase(TestCase):
    def test_parse_empty_metrics(self):
        msg_data = mkmetricsmsg([])
        msg = message.MetricsMessage.from_dict(msg_data)

        self.assertEquals({}, msg.metrics)

    def test_parse_metrics(self):
        msg_data = mkmetricsmsg([
                {'name': 'foo_counter', 'worker_name': 'foo', 'method': 'bar.baz', 'count': 5},
                {'name': 'foo_counter', 'worker_name': 'foo', 'method': 'bar.quux', 'count': 6},
                {'name': 'foo_counter', 'worker_name': 'foo', 'method': 'bar.baz', 'count': 7},
                {'name': 'foo_timer', 'worker_name': 'foo', 'method': 'bar.baz', 'count': 3, 'time': 120},
                ])
        msg = message.MetricsMessage.from_dict(msg_data)

        expected = {
            'foo_counter': [
                (5, None, {'worker_name': 'foo', 'method': 'bar.baz'}),
                (6, None, {'worker_name': 'foo', 'method': 'bar.quux'}),
                (7, None, {'worker_name': 'foo', 'method': 'bar.baz'}),
                ],
            'foo_timer': [
                (3, 120, {'worker_name': 'foo', 'method': 'bar.baz'}),
                ]
            }
        self.assertEquals(expected, msg.metrics)

