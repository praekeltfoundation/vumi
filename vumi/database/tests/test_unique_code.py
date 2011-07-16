from datetime import datetime, timedelta

from vumi.database.tests.test_base import UglyModelTestCase
from vumi.database.unique_code import UniqueCode, VoucherCode, CampaignEntry
from vumi.database.message_io import ReceivedMessage


class UniqueCodeTestCase(UglyModelTestCase):

    def setUp(self):
        return self.setup_db(UniqueCode)

    def tearDown(self):
        return self.shutdown_db()

    def test_check_status(self):
        d = self.ri(UniqueCode.load_codes, ['abc', '123', 'useme'])
        d.addCallback(self.ricb, UniqueCode.modify_code, '123', used=True)

        def _check_status(txn):
            self.assertEquals('unused', UniqueCode.get_code_status(txn, 'abc'))
            self.assertEquals('used', UniqueCode.get_code_status(txn, '123'))
            self.assertEquals('invalid', UniqueCode.get_code_status(txn, 'xyz'))

        return d.addCallback(self.ricb, _check_status)

    def test_burn_code(self):
        d = self.ri(UniqueCode.load_codes, ['abc', '123', 'useme'])
        d.addCallback(self.ricb, UniqueCode.modify_code, '123', used=True)

        def _bad_burn(txn):
            self.assertEquals('unused', UniqueCode.get_code_status(txn, 'useme'))
            self.assertEquals((True, 'valid'), UniqueCode.burn_code(txn, 'useme'))
            self.assertEquals('used', UniqueCode.get_code_status(txn, 'useme'))
            raise ValueError("foo")
        d.addCallback(self.ricb, _bad_burn).addErrback(lambda f: None)

        def _check_burn(txn):
            self.assertEquals('unused', UniqueCode.get_code_status(txn, 'useme'))
            self.assertEquals((True, 'valid'), UniqueCode.burn_code(txn, 'useme'))
            self.assertEquals('used', UniqueCode.get_code_status(txn, 'useme'))
            self.assertEquals((False, 'used'), UniqueCode.burn_code(txn, 'useme'))
        d.addCallback(self.ricb, _check_burn)

        def _recheck_burn(txn):
            self.assertEquals('used', UniqueCode.get_code_status(txn, 'useme'))
        d.addCallback(self.ricb, _recheck_burn)
        return d


class VoucherCodeTestCase(UglyModelTestCase):

    def setUp(self):
        return self.setup_db(VoucherCode)

    def tearDown(self):
        return self.shutdown_db()

    def set_up_codes(self, with_suppliers=True):
        codes = [('abc1', 's1'), ('abc2', 's1'), ('abc3', 's1'),
                 ('xyz1', 's2'), ('xyz2', 's2'), ('xyz3', 's2')]
        if with_suppliers:
            d = self.ri(VoucherCode.load_supplier_codes, codes)
        else:
            d = self.ri(VoucherCode.load_codes, [c for c, _ in codes])
        # FIXME: Avoid doing this based on insert order.
        d.addCallback(self.ricb, VoucherCode.modify_code, 1, used=True)
        d.addCallback(self.ricb, VoucherCode.modify_code, 4, used=True)
        return d

    def assert_counts(self, txn, used, unused, supplier=None):
        self.assertEquals(used, VoucherCode.count_used_codes(txn, supplier))
        self.assertEquals(unused, VoucherCode.count_unused_codes(txn, supplier))
        self.assertEquals(used + unused, VoucherCode.count_codes(txn, supplier))

    def test_counts(self):
        """
        Counts of total, used and unused codes should be available and correct.
        """
        d = self.set_up_codes(with_suppliers=False)
        d.addCallback(self.ricb, self.assert_counts, 2, 4)
        return d

    def test_counts_by_supplier(self):
        """
        Counts of total, used and unused codes per supplier should be available and correct.
        """
        d = self.set_up_codes()

        def _txn(txn):
            self.assert_counts(txn, 2, 4)
            self.assert_counts(txn, 1, 2, 's1')
            self.assert_counts(txn, 1, 2, 's2')
        d.addCallback(self.ricb, _txn)
        return d

    def test_vend_code(self):
        """
        Codes should be vended exactly once.
        """
        d = self.set_up_codes(with_suppliers=False)

        def _txn(txn):
            self.assert_counts(txn, 2, 4)
            used = [VoucherCode.vend_code(txn).code for _ in [1,2,3,4]]
            self.assert_counts(txn, 6, 0)
            self.assertEquals(['abc2', 'abc3', 'xyz2', 'xyz3'], sorted(used))
        d.addCallback(self.ricb, _txn)
        return d

    def test_vend_code_by_supplier(self):
        """
        Per-supplier codes should be vended exactly once.
        """
        d = self.set_up_codes()

        d.addCallback(self.ricb, self.assert_counts, 2, 4)

        def _txn(txn, supplier, codes):
            self.assert_counts(txn, 1, 2, supplier)
            used = [VoucherCode.vend_code(txn, supplier).code for _ in [1,2]]
            self.assert_counts(txn, 3, 0, supplier)
            self.assertEquals(codes, sorted(used))
        d.addCallback(self.ricb, _txn, 's1', ['abc2', 'abc3'])
        d.addCallback(self.ricb, self.assert_counts, 4, 2)
        d.addCallback(self.ricb, _txn, 's2', ['xyz2', 'xyz3'])
        d.addCallback(self.ricb, self.assert_counts, 6, 0)
        return d

    def test_duplicate_codes(self):
        """
        Duplicate codes should be handled correctly.
        """
        codes = [('abc', 's1'), ('abc', 's1'), ('abc', 's1'),
                 ('abc', 's2'), ('abc', 's2'), ('abc', 's2')]
        d = self.ri(VoucherCode.load_supplier_codes, codes)

        def _txn(txn):
            self.assert_counts(txn, 0, 6)
            self.assert_counts(txn, 0, 3, 's1')
            self.assert_counts(txn, 0, 3, 's2')

            used = [VoucherCode.vend_code(txn, 's1').code for _ in [1,2]]

            self.assert_counts(txn, 2, 4)
            self.assert_counts(txn, 2, 1, 's1')
            self.assert_counts(txn, 0, 3, 's2')
            self.assertEquals(['abc', 'abc'], used)

            used = [VoucherCode.vend_code(txn, 's2').code for _ in [1,2]]

            self.assert_counts(txn, 4, 2)
            self.assert_counts(txn, 2, 1, 's1')
            self.assert_counts(txn, 2, 1, 's2')

            self.assertEquals(['abc', 'abc'], used)
        d.addCallback(self.ricb, _txn)
        return d


class CampaignEntryTestCase(UglyModelTestCase):

    def setUp(self):
        d = self.setup_db(ReceivedMessage, UniqueCode, VoucherCode, CampaignEntry)
        return d.addCallback(lambda _: self.setup_data())

    def tearDown(self):
        return self.shutdown_db()

    def mkmsg(self, content):
        return {
                'from_msisdn': '27831234567',
                'to_msisdn': '90210',
                'message': content,
                }

    def setup_data(self):
        unique_codes = ['aaa', 'bbb', 'ccc', 'ddd']
        voucher_codes = [('abc1', 's1'), ('abc2', 's1'), ('abc3', 's1'),
                         ('xyz1', 's2'), ('xyz2', 's2'), ('xyz3', 's2')]
        rmsgs = [self.mkmsg(m) for m in ['foo', 'aaa', 'bbb', 'ccc']]
        d = self.ri(VoucherCode.load_supplier_codes, voucher_codes)
        d.addCallback(self.ricb, UniqueCode.load_codes, unique_codes)
        for msg in rmsgs:
            d.addCallback(self.ricb, ReceivedMessage.receive_message, msg)
        return d

    def assert_counts(self, txn, used, unused, supplier=None):
        self.assertEquals(used, VoucherCode.count_used_codes(txn, supplier))
        self.assertEquals(unused, VoucherCode.count_unused_codes(txn, supplier))
        self.assertEquals(used + unused, VoucherCode.count_codes(txn, supplier))

    def test_campaign_entry(self):
        """
        A campaign entry should be pretty.
        TODO: Write a useful test description.
        """
        def _txn(txn):
            entry_id = CampaignEntry.enter(txn, 2, 'aaa', '27831234567', 1)
            entry = CampaignEntry.get_entry(txn, entry_id)
            self.assertEquals(2, entry.received_message_id)
            self.assertEquals('aaa', entry.unique_code)
            self.assertEquals('27831234567', entry.user_id)
            self.assertEquals(1, entry.voucher_code_id)
        return self.ri(_txn)

    def test_campaign_entry_counts(self):
        """
        A campaign entry should be pretty.
        TODO: Write a useful test description.
        """
        def _txn(txn):
            recent = datetime.utcnow() - timedelta(100)
            future = datetime.utcnow() + timedelta(100)
            self.assertEquals(0, CampaignEntry.count_entries(txn))
            self.assertEquals(0, CampaignEntry.count_entries_since(txn, recent))
            self.assertEquals(0, CampaignEntry.count_entries_since(txn, future))
            CampaignEntry.enter(txn, 2, 'aaa', '27831234567', 1)
            self.assertEquals(1, CampaignEntry.count_entries(txn))
            self.assertEquals(1, CampaignEntry.count_entries_since(txn, recent))
            self.assertEquals(0, CampaignEntry.count_entries_since(txn, future))

            self.assertEquals(0, CampaignEntry.count_entries(txn, 'foo'))
            self.assertEquals(0, CampaignEntry.count_entries_since(txn, recent, 'foo'))

            self.assertEquals(1, CampaignEntry.count_entries(txn, '27831234567'))
            self.assertEquals(1, CampaignEntry.count_entries_since(txn, recent, '27831234567'))
            self.assertEquals(0, CampaignEntry.count_entries_since(txn, future, '27831234567'))
        return self.ri(_txn)
