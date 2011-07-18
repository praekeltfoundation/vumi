import uuid

from twisted.internet.defer import inlineCallbacks

from vumi.database.tests.test_base import UglyModelTestCase
from vumi.database.message_io import ReceivedMessage, SentMessage


def mkmsg_r(message, transport_message_id=None, from_msisdn=None, to_msisdn=None):
    if not transport_message_id:
        transport_message_id = uuid.uuid4().get_hex()
    if not from_msisdn:
        from_msisdn = '27831234567'
    if not to_msisdn:
        to_msisdn = '12345'
    return {
        'message': message,
        'transport_message_id': transport_message_id,
        'from_msisdn': from_msisdn,
        'to_msisdn': to_msisdn,
        }

def mkmsg_s(message, reply_to_msg_id=None, from_msisdn=None, to_msisdn=None):
    if not from_msisdn:
        from_msisdn = '12345'
    if not to_msisdn:
        to_msisdn = '27831234567'
    msg_dict = {
        'message': message,
        'from_msisdn': from_msisdn,
        'to_msisdn': to_msisdn,
        }
    if reply_to_msg_id:
        msg_dict['reply_to_msg_id'] = reply_to_msg_id
    return msg_dict


class ReceivedMessageTestCase(UglyModelTestCase):

    def setUp(self):
        return self.setup_db(ReceivedMessage)

    def tearDown(self):
        return self.shutdown_db()

    def assert_msg_fields(self, msg_dict, msg):
        for field, value in msg_dict.items():
            self.assertEquals(value, getattr(msg, field))

    def test_receive_message(self):
        """
        A received message should be stored in the database with all its details.
        """
        msg_dict = mkmsg_r('foo')

        def _txn(txn):
            self.assertEquals(0, ReceivedMessage.count_messages(txn))
            msg_id = ReceivedMessage.receive_message(txn, msg_dict)
            self.assertEquals(1, ReceivedMessage.count_messages(txn))
            msg = ReceivedMessage.get_message(txn, msg_id)
            self.assert_msg_fields(msg_dict, msg)

        return self.ri(_txn)


class SentMessageTestCase(UglyModelTestCase):

    def setUp(self):
        return self.setup_db(ReceivedMessage, SentMessage)

    def tearDown(self):
        return self.shutdown_db()

    def assert_msg_fields(self, msg_dict, msg):
        for field, value in msg_dict.items():
            self.assertEquals(value, getattr(msg, field))

    @inlineCallbacks
    def test_send_message(self):
        """
        A sent message should store basic information.
        """
        msg_dict = mkmsg_s('foo')

        # Send message
        def _txn(txn):
            self.assertEquals(0, SentMessage.count_messages(txn))
            msg_id = SentMessage.send_message(txn, msg_dict)
            self.assertEquals(1, SentMessage.count_messages(txn))
            msg = SentMessage.get_message(txn, msg_id)
            self.assert_msg_fields(msg_dict, msg)
            self.assertEquals(None, msg.reply_to_msg_id)
            self.assertEquals(None, msg.transport_message_id)
            return msg_id
        sent_msg_id = yield self.ri(_txn)

        transport_message_id = uuid.uuid4().get_hex()
        # Transport ack
        def _txn(txn):
            self.assertEquals(1, SentMessage.count_messages(txn))
            SentMessage.ack_message(txn, sent_msg_id, transport_message_id)
            msg = SentMessage.get_message(txn, sent_msg_id)
            self.assert_msg_fields(msg_dict, msg)
            self.assertEquals(None, msg.reply_to_msg_id)
            self.assertEquals(transport_message_id, msg.transport_message_id)
            self.assertEquals(1, SentMessage.count_messages(txn))
        yield self.ri(_txn)

    @inlineCallbacks
    def test_send_message_reply(self):
        """
        A reply message should also store the message it replies to.
        """
        src_dict = mkmsg_r('foo')
        src_id = yield self.ri(ReceivedMessage.receive_message, src_dict)
        self.assertNotEquals(None, src_id)
        msg_dict = mkmsg_s('bar', src_id)

        # Send message
        def _txn(txn):
            self.assertEquals(0, SentMessage.count_messages(txn))
            msg_id = SentMessage.send_message(txn, msg_dict)
            self.assertEquals(1, SentMessage.count_messages(txn))
            msg = SentMessage.get_message(txn, msg_id)
            self.assert_msg_fields(msg_dict, msg)
            self.assertEquals(src_id, msg.reply_to_msg_id)
            self.assertEquals(None, msg.transport_message_id)
            return msg_id
        sent_msg_id = yield self.ri(_txn)

        transport_message_id = uuid.uuid4().get_hex()
        # Transport ack
        def _txn(txn):
            self.assertEquals(1, SentMessage.count_messages(txn))
            SentMessage.ack_message(txn, sent_msg_id, transport_message_id)
            msg = SentMessage.get_message(txn, sent_msg_id)
            self.assert_msg_fields(msg_dict, msg)
            self.assertEquals(src_id, msg.reply_to_msg_id)
            self.assertEquals(transport_message_id, msg.transport_message_id)
            self.assertEquals(1, SentMessage.count_messages(txn))
        yield self.ri(_txn)
