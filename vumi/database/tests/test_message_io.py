from twisted.internet.defer import inlineCallbacks

from vumi.database.tests.test_base import UglyModelTestCase
from vumi.database.message_io import ReceivedMessage

class ReceivedMessageTestCase(UglyModelTestCase):

    def setUp(self):
        return self.setup_db(ReceivedMessage)

    def tearDown(self):
        return self.shutdown_db()

    def test_receive_message(self):
        """
        A received message should be stored in the database with all its details.
        """
        msg_dict = {
            'from_msisdn': '27831234567',
            'to_msisdn': '27832345678',
            'message': 'foo',
            }

        def _txn(txn):
            self.assertEquals(0, ReceivedMessage.count_messages(txn))
            msg_id = ReceivedMessage.receive_message(txn, msg_dict)
            self.assertEquals(1, msg_id)
            self.assertEquals(1, ReceivedMessage.count_messages(txn))
            msg = ReceivedMessage.get_message(txn, msg_id)
            self.assertEquals(msg_dict, dict((k, getattr(msg, k)) for k in msg_dict))

        return self.ri(_txn)

    def test_dispatch_message(self):
        """
        A dispatched message should be updated with the destination.
        """
        msg_dict = {
            'from_msisdn': '27831234567',
            'to_msisdn': '27832345678',
            'message': 'foo',
            }

        def _txn(txn):
            msg_id = ReceivedMessage.receive_message(txn, msg_dict)
            self.assertEquals(None, ReceivedMessage.get_message(txn, msg_id).destination)
            ReceivedMessage.dispatch_message(txn, msg_id, 'handler')
            self.assertEquals('handler', ReceivedMessage.get_message(txn, msg_id).destination)

        return self.ri(_txn)


