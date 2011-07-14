from twisted.internet.defer import inlineCallbacks

from vumi.database.tests.test_base import UglyModelTestCase
from vumi.database.unique_code import UniqueCode2

class UniqueCodeTestCase(UglyModelTestCase):

    def setUp(self):
        @inlineCallbacks
        def _cb(_):
            yield UniqueCode2.drop_table(self.db)
            yield UniqueCode2.create_table(self.db)
        return self.setup_db(_cb)

    @inlineCallbacks
    def test_uc(self):
        yield self.ri(UniqueCode2.load_codes, ['abc', '123', 'useme'])
        yield self.ri(UniqueCode2.modify_code, '123', used=True)

        def _check_status(txn):
            self.assertEquals('unused', UniqueCode2.get_code_status(txn, 'abc'))
            self.assertEquals('used', UniqueCode2.get_code_status(txn, '123'))
            self.assertEquals('invalid', UniqueCode2.get_code_status(txn, 'xyz'))

        yield self.ri(_check_status)

        def _bad_burn(txn):
            self.assertEquals('unused', UniqueCode2.get_code_status(txn, 'useme'))
            self.assertEquals((True, 'valid'), UniqueCode2.burn_code(txn, 'useme'))
            self.assertEquals('used', UniqueCode2.get_code_status(txn, 'useme'))
            raise ValueError("foo")

        try:
            yield self.ri(_bad_burn)
        except ValueError:
            pass

        def _check_burn(txn):
            self.assertEquals('unused', UniqueCode2.get_code_status(txn, 'useme'))
            self.assertEquals((True, 'valid'), UniqueCode2.burn_code(txn, 'useme'))
            self.assertEquals('used', UniqueCode2.get_code_status(txn, 'useme'))
            self.assertEquals((False, 'used'), UniqueCode2.burn_code(txn, 'useme'))

        yield self.ri(_check_burn)

        def _recheck_burn(txn):
            self.assertEquals('used', UniqueCode2.get_code_status(txn, 'useme'))

        yield self.ri(_recheck_burn)

