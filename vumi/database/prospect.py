# -*- test-case-name: vumi.database.tests.test_prospects -*-

from vumi.database.base import UglyModel


class Prospect(UglyModel):
    table_name = 'prospects'
    fields = (
        ('id', 'SERIAL PRIMARY KEY'),
        ('msisdn', 'varchar UNIQUE NOT NULL'),
        ('opted_in', 'boolean DEFAULT false'),
        ('opted_out', 'boolean DEFAULT false'),
        ('opted_out_source_id', 'integer REFERENCES received_messages'),
        ('birthdate', 'varchar'),
        ('birthdate_source_id', 'integer REFERENCES received_messages'),
        ('name', 'varchar'),
        ('name_source_id', 'integer REFERENCES received_messages'),
        ('created_at', 'timestamp with time zone DEFAULT current_timestamp'),
        ('modified_at', 'timestamp with time zone DEFAULT current_timestamp'),
        )
    indexes = ('msisdn',)

    @classmethod
    def get_prospect(cls, txn, msisdn):
        prospects = cls.run_select(txn, "WHERE msisdn=%(msisdn)s", {'msisdn': msisdn})
        if prospects:
            return cls(txn, *prospects[0])
        return None

    @classmethod
    def create_prospect(cls, txn, msisdn):
        params = {'msisdn': msisdn}
        txn.execute(cls.insert_values_query(**params), params)
        txn.execute("SELECT lastval()")
        return txn.fetchone()[0]

    def _update_fields(self, txn):
        fields = self.run_select(txn, "WHERE id=%(id)s", {'id': self.id})[0]
        for f, v in zip([f[0] for f in self.fields], fields):
            setattr(self, f, v)

    def _modify(self, txn, **kw):
        kw.pop('modified_at', None)
        kw.pop('id', None)
        setchunks = ["modified_at=current_timestamp"]
        setchunks.extend(["%s=%%(%s)s" % (f, f) for f in kw.keys()])
        setchunks = ", ".join(setchunks)
        kw['_id'] = self.id
        query = "UPDATE %s SET %s WHERE id=%%(_id)s" % (self.table_name, setchunks)
        txn.execute(query, kw)
        self._update_fields(txn)

    def opt_in(self, txn, source_id, birthdate):
        self._modify(txn, birthdate_source_id=source_id, birthdate=birthdate,
                     opted_in=True, opted_out=False)

    def update_name(self, txn, source_id, name):
        self._modify(txn, name_source_id=source_id, name=name)

    def opt_out(self, txn, source_id):
        self._modify(txn, opted_out_source_id=source_id, opted_out=True, opted_in=False)
