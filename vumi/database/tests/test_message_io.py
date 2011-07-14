from twisted.internet.defer import inlineCallbacks

from vumi.database.tests.test_base import UglyModelTestCase
from vumi.database.message_io import ReceivedMessage

class ReceivedMessageTestCase(UglyModelTestCase):

    def setUp(self):
        @inlineCallbacks
        def _cb(_):
            yield ReceivedMessage.drop_table(self.db)
            yield ReceivedMessage.create_table(self.db)
        return self.setup_db(_cb)

    def tearDown(self):
        self.close_db()

    def test_receive_message(self):
        msg_dict = {
            'from_msisdn': '27831234567',
            'to_msisdn': '27832345678',
            'message': 'foo',
            }

        def _rec_msg(txn):
            self.assertEquals(0, ReceivedMessage.count_messages(txn))
            msg_id = ReceivedMessage.receive_message(txn, msg_dict)
            self.assertEquals(1, msg_id)
            self.assertEquals(1, ReceivedMessage.count_messages(txn))
            msg = ReceivedMessage.get_message(txn, msg_id)
            self.assertEquals(msg_dict, dict((k, getattr(msg, k)) for k in msg_dict))

        return self.ri(_rec_msg)


