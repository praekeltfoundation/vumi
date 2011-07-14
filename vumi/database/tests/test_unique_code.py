from twisted.internet.defer import inlineCallbacks

from vumi.database.tests.test_base import UglyModelTestCase
from vumi.database.unique_code import UniqueCode

class UniqueCodeTestCase(UglyModelTestCase):

    def setUp(self):
        @inlineCallbacks
        def _cb(_):
            yield UniqueCode.drop_table(self.db)
            yield UniqueCode.create_table(self.db)
        return self.setup_db(_cb)

    def tearDown(self):
        self.close_db()

    def test_uc(self):
        d = self.ri(UniqueCode.load_codes, ['abc', '123', 'useme'])
        d.addCallback(self.ricb, UniqueCode.modify_code, '123', used=True)

        def _check_status(txn):
            self.assertEquals('unused', UniqueCode.get_code_status(txn, 'abc'))
            self.assertEquals('used', UniqueCode.get_code_status(txn, '123'))
            self.assertEquals('invalid', UniqueCode.get_code_status(txn, 'xyz'))

        d.addCallback(self.ricb, _check_status)

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

