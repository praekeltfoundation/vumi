# -*- test-case-name: vumi.database.tests.test_unique_code -*-

from vumi.database.base import UglyModel, UglyNoDbModel

class UniqueCode(UglyModel):
    db_name = 'test'
    table_name = 'unique_codes'
    fields = (
        ('code', 'varchar(10) UNIQUE NOT NULL'),
        ('used', 'boolean DEFAULT false'),
        ('created', 'timestamp DEFAULT current_timestamp'),
        ('modified', 'timestamp DEFAULT current_timestamp'),
        )

    @classmethod
    def get_code(cls, code):
        def _cb(ucs):
            if ucs:
                return ucs[0]
            return None
        return cls.run_select("WHERE code=%(code)s", {'code': code}).addCallback(_cb)

    @classmethod
    def get_code_status(cls, code):
        def _cb(uc):
            if not uc:
                return 'invalid'
            if uc.used:
                return 'used'
            return 'unused'
        return cls.get_code(code).addCallback(_cb)

    @classmethod
    def load_codes(cls, codes):
        def _lc(txn, cs):
            for code in cs:
                txn.execute(cls._insert_values_query(code=code), {'code': code})
        return cls.runInteraction(_lc, codes)

    @classmethod
    def modify_code(cls, code, **kw):
        kw.pop('modified', None)
        setchunks = ["modified=current_timestamp"]
        setchunks.extend(["%s=%%(%s)s" % (f, f) for f in kw.keys()])
        setchunks = ", ".join(setchunks)
        kw['_code'] = code
        query = "UPDATE %s SET %s WHERE code=%%(_code)s" % (cls.table_name, setchunks)
        return cls.runOperation(query, kw)

    @classmethod
    def burn_code(cls, code):
        def _cb(txn, code):
            query = cls._select_query("WHERE code=%(code)s")
            txn.execute(query, {'code': code})
            codes = cls._wrap_results(txn.fetchall())
            if not codes:
                return False, 'invalid'
            if codes[0].used:
                return False, 'used'
            query = "UPDATE %s SET used=%%(used)s WHERE code=%%(code)s" % (cls.table_name)
            txn.execute(query, {'code': code, 'used': True})
            return True, 'valid'
        return cls.runInteraction(_cb, code)




class UniqueCode2(UglyNoDbModel):
    table_name = 'unique_codes'
    fields = (
        ('code', 'varchar(10) UNIQUE NOT NULL'),
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

