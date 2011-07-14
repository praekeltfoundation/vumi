# -*- test-case-name: vumi.database.tests.test_unique_code -*-

from vumi.database.base import UglyModel


class UniqueCode(UglyModel):
    table_name = 'unique_codes'
    fields = (
        ('code', 'varchar UNIQUE NOT NULL PRIMARY KEY'),
        ('used', 'boolean DEFAULT false'),
        ('created', 'timestamp DEFAULT current_timestamp'),
        ('modified', 'timestamp DEFAULT current_timestamp'),
        )

    @classmethod
    def get_code(cls, txn, code):
        codes = cls.run_select(txn, "WHERE code=%(code)s", {'code': code})
        if codes:
            return cls(txn, *codes[0])
        return None

    @classmethod
    def get_code_status(cls, txn, code):
        result = cls.get_code(txn, code)
        if not result:
            return 'invalid'
        if result.used:
            return 'used'
        return 'unused'

    @classmethod
    def load_codes(cls, txn, codes):
        for code in codes:
            txn.execute(cls.insert_values_query(code=code), {'code': code})

    @classmethod
    def modify_code(cls, txn, code, **kw):
        kw.pop('modified', None)
        setchunks = ["modified=current_timestamp"]
        setchunks.extend(["%s=%%(%s)s" % (f, f) for f in kw.keys()])
        setchunks = ", ".join(setchunks)
        kw['_code'] = code
        query = "UPDATE %s SET %s WHERE code=%%(_code)s" % (cls.table_name, setchunks)
        txn.execute(query, kw)

    @classmethod
    def burn_code(cls, txn, code):
        result = cls.get_code(txn, code)
        if not result:
            return False, 'invalid'
        if result.used:
            return False, 'used'
        result.burn()
        return True, 'valid'

    def burn(self):
        query = "UPDATE %s SET used=%%(used)s WHERE code=%%(code)s" % (self.table_name)
        self.txn.execute(query, {'code': self.code, 'used': True})
        self.used = True

