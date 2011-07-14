from twisted.internet.defer import inlineCallbacks

from vumi.database.tests.test_base import UglyModelTestCase
from vumi.database.unique_code import UniqueCode, VendedCode

class UniqueCodeTestCase(UglyModelTestCase):

    def setUp(self):
        @inlineCallbacks
        def _cb(_):
            yield UniqueCode.drop_table(self.db)
            yield UniqueCode.create_table(self.db)
        return self.setup_db(_cb)

    def tearDown(self):
        self.close_db()

    def test_check_status(self):
        d = self.ri(UniqueCode.load_codes, ['abc', '123', 'useme'])
        d.addCallback(self.ricb, UniqueCode.modify_code, '123', used=True)

        def _check_status(txn):
            self.assertEquals('unused', UniqueCode.get_code_status(txn, 'abc'))
            self.assertEquals('used', UniqueCode.get_code_status(txn, '123'))
            self.assertEquals('invalid', UniqueCode.get_code_status(txn, 'xyz'))

        d.addCallback(self.ricb, _check_status)

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


class VendedCodeTestCase(UglyModelTestCase):

    def setUp(self):
        @inlineCallbacks
        def _cb(_):
            yield VendedCode.drop_table(self.db)
            yield VendedCode.create_table(self.db)
        return self.setup_db(_cb)

    def tearDown(self):
        self.close_db()

    def set_up_codes(self, with_suppliers=True):
        codes = [('abc1', 's1'), ('abc2', 's1'), ('abc3', 's1'),
                 ('xyz1', 's2'), ('xyz2', 's2'), ('xyz3', 's2')]
        if with_suppliers:
            d = self.ri(VendedCode.load_supplier_codes, codes)
        else:
            d = self.ri(VendedCode.load_codes, [c for c, _ in codes])
        # FIXME: Avoid doing this based on insert order.
        d.addCallback(self.ricb, VendedCode.modify_code, 1, used=True)
        d.addCallback(self.ricb, VendedCode.modify_code, 4, used=True)
        return d

    def assert_counts(self, txn, used, unused, supplier=None):
        self.assertEquals(used, VendedCode.count_used_codes(txn, supplier))
        self.assertEquals(unused, VendedCode.count_unused_codes(txn, supplier))
        self.assertEquals(used + unused, VendedCode.count_codes(txn, supplier))

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
            used = [VendedCode.vend_code(txn).code for _ in [1,2,3,4]]
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
            used = [VendedCode.vend_code(txn, supplier).code for _ in [1,2]]
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
        d = self.ri(VendedCode.load_supplier_codes, codes)

        def _txn(txn):
            self.assert_counts(txn, 0, 6)
            self.assert_counts(txn, 0, 3, 's1')
            self.assert_counts(txn, 0, 3, 's2')

            used = [VendedCode.vend_code(txn, 's1').code for _ in [1,2]]

            self.assert_counts(txn, 2, 4)
            self.assert_counts(txn, 2, 1, 's1')
            self.assert_counts(txn, 0, 3, 's2')
            self.assertEquals(['abc', 'abc'], used)

            used = [VendedCode.vend_code(txn, 's2').code for _ in [1,2]]

            self.assert_counts(txn, 4, 2)
            self.assert_counts(txn, 2, 1, 's1')
            self.assert_counts(txn, 2, 1, 's2')

            self.assertEquals(['abc', 'abc'], used)
        d.addCallback(self.ricb, _txn)
        return d
