from twisted.enterprise import adbapi

DATABASES = {}

def setup_db(name, *args, **kw):
    if DATABASES.get(name, None):
        raise ValueError("Database %s already exists." % (name,))
    # Hardcoding this, because we currently only support one db.
    db = adbapi.ConnectionPool('psycopg2', *args, **kw)
    DATABASES[name] = db
    return db

def get_db(name):
    db = DATABASES.get(name, None)
    if db:
        return db
    raise ValueError("Database %s is not set up." % (name,))

def close_db(name):
    db = DATABASES.get(name, None)
    if db:
        db.close()
        DATABASES[name] = None


class UglyModel(object):
    table_name = None
    fields = ()

    def __init__(self, txn, *args):
        if len(args) != len(self.fields):
            raise ValueError("Bad values.")
        for f, v in zip([f[0] for f in self.fields], args):
            setattr(self, f, v)
        self.txn = txn

    @classmethod
    def create_table(cls, db):
        cols = ', '.join([' '.join(f) for f in cls.fields])
        query = ''.join(['CREATE TABLE ', cls.table_name, ' (', cols, ')'])
        return db.runOperation(query)

    @classmethod
    def drop_table(cls, db, if_exists=True):
        q = "DROP TABLE "
        if if_exists:
            q += "IF EXISTS "
        return db.runOperation(q + cls.table_name)

    @classmethod
    def wrap_results(cls, txn, results):
        return [cls(txn, *r) for r in results]

    @classmethod
    def select_query(cls, suffix):
        return "SELECT * FROM %s %s" % (cls.table_name, suffix)

    @classmethod
    def run_select(cls, txn, suffix, *args, **kw):
        txn.execute(cls.select_query(suffix), *args, **kw)
        return txn.fetchall()

    @classmethod
    def insert_values_query(cls, **kw):
        fields = kw.keys()
        valuespecs = ", ".join(["%%(%s)s" % f for f in fields])
        return "INSERT INTO %s (%s) VALUES (%s)" % (cls.table_name, ", ".join(fields), valuespecs)

    @classmethod
    def count_rows(cls, txn, suffix='', values=None):
        txn.execute("SELECT count(1) FROM %s %s" % (cls.table_name, suffix), values)
        return txn.fetchone()[0]
