from datetime import datetime, timedelta
import uuid

from vumi.database.tests.test_base import UglyModelTestCase
from vumi.database.prospect import Prospect
from vumi.database.message_io import ReceivedMessage


class ProspectTestCase(UglyModelTestCase):

    def setUp(self):
        return self.setup_db(ReceivedMessage, Prospect)

    def tearDown(self):
        return self.shutdown_db()

    def mkmsg(self, content):
        return {
                'from_msisdn': '27831234567',
                'to_msisdn': '90210',
                'message': content,
                'transport_message_id': uuid.uuid4().get_hex(),
                }

    def test_create_prospect(self):
        """
        A fresh prospect is pretty bare-bones.
        """
        def _txn(txn):
            prospect_id = Prospect.create_prospect(txn, '27831234567')
            prospect = Prospect.get_prospect(txn, '27831234567')
            self.assertEquals('27831234567', prospect.msisdn)
            self.assertEquals(False, prospect.opted_in)
            self.assertEquals(False, prospect.opted_out)
            self.assertEquals(None, prospect.opted_out_source_id)
            self.assertEquals(None, prospect.birthdate)
            self.assertEquals(None, prospect.birthdate_source_id)
            self.assertEquals(None, prospect.name)
            self.assertEquals(None, prospect.name_source_id)
            self.assert_utc_near_now(prospect.created_at)
            self.assert_utc_near_now(prospect.modified_at)
        return self.ri(_txn)

    def test_opt_in(self):
        """
        Once opted in, some fields are set.
        """
        d = self.ri(Prospect.create_prospect, '27831234567')

        def _txn(txn):
            msg_id = ReceivedMessage.receive_message(txn, self.mkmsg('YES 1980-09-02'))
            prospect = Prospect.get_prospect(txn, '27831234567')
            prospect.opt_in(txn, msg_id, '1980-09-02')
            self.assertEquals('27831234567', prospect.msisdn)
            self.assertEquals(True, prospect.opted_in)
            self.assertEquals(False, prospect.opted_out)
            self.assertEquals(None, prospect.opted_out_source_id)
            self.assertEquals('1980-09-02', prospect.birthdate)
            self.assertEquals(msg_id, prospect.birthdate_source_id)
            self.assertEquals(None, prospect.name)
            self.assertEquals(None, prospect.name_source_id)
            self.assert_utc_near_now(prospect.created_at)
            self.assert_utc_near_now(prospect.modified_at)
            self.assertNotEquals(prospect.created_at, prospect.modified_at)
        return d.addCallback(self.ricb, _txn)

    def test_update_name(self):
        """
        More fields are set when a name is added.
        """
        def _txn(txn):
            Prospect.create_prospect(txn, '27831234567')
            prospect = Prospect.get_prospect(txn, '27831234567')
            msg_id = ReceivedMessage.receive_message(txn, self.mkmsg('YES 1980-09-02'))
            prospect.opt_in(txn, msg_id, '1980-09-02')
        d = self.ri(_txn)

        def _txn(txn):
            msg_id = ReceivedMessage.receive_message(txn, self.mkmsg('VIP dude'))
            prospect = Prospect.get_prospect(txn, '27831234567')
            prospect.update_name(txn, msg_id, 'dude')
            self.assertEquals('27831234567', prospect.msisdn)
            self.assertEquals(True, prospect.opted_in)
            self.assertEquals(False, prospect.opted_out)
            self.assertEquals('1980-09-02', prospect.birthdate)
            self.assertEquals('dude', prospect.name)
            self.assertEquals(msg_id, prospect.name_source_id)
            self.assert_utc_near_now(prospect.created_at)
            self.assert_utc_near_now(prospect.modified_at)
            self.assertNotEquals(prospect.created_at, prospect.modified_at)
            self.assertNotEquals(prospect.birthdate_source_id, prospect.name_source_id)
        return d.addCallback(self.ricb, _txn)

    def test_opt_out(self):
        """
        Opting out does what we expect.
        """
        def _txn(txn):
            Prospect.create_prospect(txn, '27831234567')
            prospect = Prospect.get_prospect(txn, '27831234567')
            msg_id = ReceivedMessage.receive_message(txn, self.mkmsg('YES 1980-09-02'))
            prospect.opt_in(txn, msg_id, '1980-09-02')
            msg_id = ReceivedMessage.receive_message(txn, self.mkmsg('VIP dude'))
            prospect.update_name(txn, msg_id, 'dude')
        d = self.ri(_txn)

        def _txn(txn):
            prospect = Prospect.get_prospect(txn, '27831234567')
            msg_id = ReceivedMessage.receive_message(txn, self.mkmsg('NO STOP PLS'))
            prospect.opt_out(txn, msg_id)
            self.assertEquals('27831234567', prospect.msisdn)
            self.assertEquals(False, prospect.opted_in)
            self.assertEquals(True, prospect.opted_out)
            self.assertEquals(msg_id, prospect.opted_out_source_id)
            self.assertEquals('1980-09-02', prospect.birthdate)
            self.assertEquals('dude', prospect.name)
            self.assert_utc_near_now(prospect.created_at)
            self.assert_utc_near_now(prospect.modified_at)
            self.assertNotEquals(prospect.created_at, prospect.modified_at)
            self.assertNotEquals(prospect.birthdate_source_id, prospect.name_source_id)
            self.assertNotEquals(prospect.name_source_id, prospect.opted_out_source_id)
        return d.addCallback(self.ricb, _txn)
