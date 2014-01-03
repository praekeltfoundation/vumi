from datetime import datetime

from vumi.blinkenlights import message20110707 as message
from vumi.tests.helpers import VumiTestCase


TIMEOBJ = datetime(2011, 07, 07, 12, 00, 00)
TIMELIST = [2011, 07, 07, 12, 00, 00]


def mkmsg(message_version, message_type, source_name, source_id, payload,
          timestamp):
    return {
        "message_version": message_version,
        "message_type": message_type,
        "source_name": source_name,
        "source_id": source_id,
        "payload": payload,
        "timestamp": timestamp,
        }


def mkmsgobj(message_type, source_name, source_id, payload, timestamp):
    return message.Message(message_type, source_name, source_id, payload,
                           timestamp)


class TestMessage(VumiTestCase):

    def test_decode_valid_message(self):
        """A valid message dict should decode into an appropriate Message
        object.
        """
        msg_data = mkmsg("20110707", "custom", "myworker", "abc123", ["foo"],
                         TIMELIST)
        msg = message.Message.from_dict(msg_data)

        self.assertEquals("custom", msg.message_type)
        self.assertEquals("myworker", msg.source_name)
        self.assertEquals("abc123", msg.source_id)
        self.assertEquals(["foo"], msg.payload)
        self.assertEquals(TIMEOBJ, msg.timestamp)

    def test_encode_valid_message(self):
        """A Message object should encode into an appropriate message dict.
        """
        msg_data = mkmsg("20110707", "custom", "myworker", "abc123", ["foo"],
                         TIMELIST)
        msg = message.Message("custom", "myworker", "abc123", ["foo"], TIMEOBJ)

        self.assertEquals(msg_data, msg.to_dict())

    def test_decode_invalid_messages(self):
        """Various kinds of invalid messages should fail to decode.
        """
        msg_data = mkmsg("19800902", "custom", "myworker", "abc123", ["foo"],
                         TIMELIST)
        self.assertRaises(ValueError, message.Message.from_dict, msg_data)

        msg_data = mkmsg("20110707", "custom", "myworker", "abc123", ["foo"],
                         None)
        self.assertRaises(ValueError, message.Message.from_dict, msg_data)

        msg_data = mkmsg("20110707", "custom", "myworker", "abc123", ["foo"],
                         TIMELIST)
        msg_data.pop('payload')
        self.assertRaises(ValueError, message.Message.from_dict, msg_data)

        msg_data = mkmsg("20110707", "custom", "myworker", "abc123", ["foo"],
                         TIMELIST)
        msg_data['foo'] = 'bar'
        self.assertRaises(ValueError, message.Message.from_dict, msg_data)

    def test_message_equality(self):
        """Identical messages should compare equal. Different messages should
        not.
        """
        msg_data = mkmsg("20110707", "custom", "myworker", "abc123", ["foo"],
                         TIMELIST)
        msg1 = mkmsgobj("custom", "myworker", "abc123", ["foo"], TIMEOBJ)
        msg2 = message.Message.from_dict(msg_data)
        msg3 = message.Message.from_dict(msg1.to_dict())
        diff_msgs = [
            mkmsgobj("custom1", "myworker", "abc123", ["foo"], TIMEOBJ),
            mkmsgobj("custom", "myworker1", "abc123", ["foo"], TIMEOBJ),
            mkmsgobj("custom", "myworker", "abc1231", ["foo"], TIMEOBJ),
            mkmsgobj("custom", "myworker", "abc123", ["foo1"], TIMEOBJ),
            ]

        self.assertEquals(msg1, msg1)
        self.assertEquals(msg1, msg2)
        self.assertEquals(msg1, msg3)
        for msg in diff_msgs:
            self.assertNotEquals(msg1, msg)

    def test_timestamp_injection(self):
        """A message created without a timestamp should get one.
        """
        start = datetime.utcnow()
        msg = mkmsgobj("custom", "myworker", "abc123", ["foo"], None)
        self.assertTrue(start <= msg.timestamp <= datetime.utcnow(),
                        "Expected a time near %s, got %s"
                        % (start, msg.timestamp))


def mkmetricsmsg(metrics):
    payload = metrics
    return mkmsg("20110707", "metrics", "myworker", "abc123", payload,
                 TIMELIST)


class TestMetricsMessage(VumiTestCase):
    def test_parse_empty_metrics(self):
        msg_data = mkmetricsmsg([])
        msg = message.MetricsMessage.from_dict(msg_data)

        self.assertEquals({}, msg.metrics)

    def test_parse_metrics(self):
        msg_data = mkmetricsmsg([
                {'name': 'vumi.metrics.test.foo', 'method': 'do_stuff',
                 'count': 5},
                {'name': 'vumi.metrics.test.foo', 'method': 'do_more_stuff',
                 'count': 6},
                {'name': 'vumi.metrics.test.foo', 'method': 'do_stuff',
                 'count': 7},
                {'name': 'vumi.metrics.test.bar', 'method': 'do_stuff',
                 'count': 3, 'time': 120},
                ])
        msg = message.MetricsMessage.from_dict(msg_data)

        expected = {
            'vumi.metrics.test.foo': [
                (5, None, {'method': 'do_stuff'}),
                (6, None, {'method': 'do_more_stuff'}),
                (7, None, {'method': 'do_stuff'}),
                ],
            'vumi.metrics.test.bar': [
                (3, 120, {'method': 'do_stuff'}),
                ],
            }
        self.assertEquals(expected, msg.metrics)
