# -*- test-case-name: vumi.database.tests.test_unique_code -*-

from vumi.database.base import UglyModel


class UniqueCode(UglyModel):
    table_name = 'unique_codes'
    fields = (
        ('code', 'varchar PRIMARY KEY'),
        ('used', 'boolean DEFAULT false'),
        ('created_at', 'timestamp with time zone DEFAULT current_timestamp'),
        ('modified_at', 'timestamp with time zone DEFAULT current_timestamp'),
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
        kw.pop('modified_at', None)
        setchunks = ["modified_at=current_timestamp"]
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



class VoucherCode(UglyModel):
    table_name = 'voucher_codes'
    fields = (
        ('id', 'SERIAL PRIMARY KEY'),
        ('code', 'varchar NOT NULL'),
        ('supplier', 'varchar'),
        ('used', 'boolean DEFAULT false'),
        ('created_at', 'timestamp with time zone DEFAULT current_timestamp'),
        ('modified_at', 'timestamp with time zone DEFAULT current_timestamp'),
        )
    indexes = ('supplier',)

    @classmethod
    def load_codes(cls, txn, codes):
        cls.load_supplier_codes(txn, ((code, None) for code in codes))

    @classmethod
    def load_supplier_codes(cls, txn, codes):
        for code, supplier in codes:
            txn.execute(cls.insert_values_query(code=code, supplier=supplier),
                        {'code': code, 'supplier': supplier})

    @classmethod
    def get_code(cls, txn, code_id):
        codes = cls.run_select(txn, "WHERE id=%(id)s", {'id': code_id})
        if codes:
            return cls(txn, *codes[0])
        return None

    @classmethod
    def get_unused_code(cls, txn, supplier=None):
        suffix = "WHERE "
        if supplier:
            suffix += "supplier=%(supplier)s AND "
        suffix += "NOT used LIMIT 1"
        codes = cls.run_select(txn, suffix, {'supplier': supplier})
        if not codes:
            raise ValueError("No unused codes")
        return cls(txn, *codes[0])

    @classmethod
    def vend_code(cls, txn, supplier=None):
        code = cls.get_unused_code(txn, supplier=supplier)
        code.burn()
        return code

    @classmethod
    def count_codes(cls, txn, supplier=None):
        suffix = ""
        if supplier:
            suffix += "WHERE supplier=%(supplier)s"
        return cls.count_rows(txn, suffix, {'supplier': supplier})

    @classmethod
    def count_used_codes(cls, txn, supplier=None):
        suffix = "WHERE "
        if supplier:
            suffix += "supplier=%(supplier)s AND"
        suffix += " used"
        return cls.count_rows(txn, suffix, {'supplier': supplier})

    @classmethod
    def count_unused_codes(cls, txn, supplier=None):
        suffix = "WHERE "
        if supplier:
            suffix += "supplier=%(supplier)s AND"
        suffix += " NOT used"
        return cls.count_rows(txn, suffix, {'supplier': supplier})

    @classmethod
    def modify_code(cls, txn, code_id, **kw):
        kw.pop('modified_at', None)
        setchunks = ["modified_at=current_timestamp"]
        setchunks.extend(["%s=%%(%s)s" % (f, f) for f in kw.keys()])
        setchunks = ", ".join(setchunks)
        kw['_id'] = code_id
        query = "UPDATE %s SET %s WHERE id=%%(_id)s" % (cls.table_name, setchunks)
        txn.execute(query, kw)

    def burn(self):
        query = "UPDATE %s SET used=%%(used)s WHERE id=%%(id)s" % (self.table_name)
        self.txn.execute(query, {'id': self.id, 'used': True})
        self.used = True


class CampaignEntry(UglyModel):
    table_name = 'campaign_entries'
    fields = (
        ('id', 'SERIAL PRIMARY KEY'),
        ('received_message_id', 'integer NOT NULL REFERENCES received_messages'),
        ('created_at', 'timestamp with time zone DEFAULT current_timestamp'),
        ('entrant_id', 'varchar NOT NULL'),
        ('unique_code', 'varchar NOT NULL REFERENCES unique_codes'),
        ('voucher_code_id', 'integer REFERENCES voucher_codes'),
        )
    indexes = ('entrant_id', 'created_at')

    @classmethod
    def get_entry(cls, txn, entry_id):
        entries = cls.run_select(txn, "WHERE id=%(id)s", {'id': entry_id})
        if entries:
            return cls(txn, *entries[0])
        return None

    @classmethod
    def count_entries(cls, txn, entrant_id=None):
        suffix = ""
        if entrant_id:
            suffix += "WHERE entrant_id=%(entrant_id)s"
        return cls.count_rows(txn, suffix, {'entrant_id': entrant_id})

    @classmethod
    def count_entries_since(cls, txn, timestamp, entrant_id=None):
        suffix = "WHERE "
        if entrant_id:
            suffix += "entrant_id=%(entrant_id)s AND"
        suffix += " created_at>=%(timestamp)s"
        return cls.count_rows(txn, suffix, {'entrant_id': entrant_id,
                                            'timestamp': timestamp})

    @classmethod
    def enter(cls, txn, received_message_id, unique_code, entrant_id, voucher_code_id):
        params = {
            'unique_code': unique_code,
            'received_message_id': received_message_id,
            'entrant_id': entrant_id,
            'voucher_code_id': voucher_code_id,
            }
        txn.execute(cls.insert_values_query(**params), params)
        txn.execute("SELECT lastval()")
        return txn.fetchone()[0]
