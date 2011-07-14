from twisted.enterprise import adbapi

DATABASES = {}

def setup_db(name, *args, **kw):
    if DATABASES.get(name, None):
        raise ValueError("Database %s already exists." % (name,))
    DATABASES[name] = adbapi.ConnectionPool(*args, **kw)

def get_db(name):
    db = DATABASES.get(name, None)
    if db:
        return db
    raise ValueError("Database %s is not set up." % (name,))


class UglyModel(object):
    db_name = None
    table_name = None
    fields = ()

    def __init__(self, *args):
        if len(args) != len(self.fields):
            raise ValueError("Bad values.")
        for f, v in zip([f[0] for f in self.fields], args):
            setattr(self, f, v)

    @classmethod
    def create_table(cls, if_not_exists=False):
        ine = ""
        if if_not_exists:
            ine = "IF NOT EXISTS "
        cols = ', '.join([' '.join(f) for f in cls.fields])
        query = ''.join(['CREATE TABLE ', ine, cls.table_name, ' (', cols, ')'])
        return cls.runOperation(query)

    @classmethod
    def drop_table(cls, if_exists=True):
        q = "DROP TABLE "
        if if_exists:
            q += "IF EXISTS "
        return cls.runOperation(q + cls.table_name)

    @classmethod
    def runQuery(cls, *args, **kw):
        return get_db(cls.db_name).runQuery(*args, **kw)

    @classmethod
    def runOperation(cls, *args, **kw):
        return get_db(cls.db_name).runOperation(*args, **kw)

    @classmethod
    def runInteraction(cls, *args, **kw):
        return get_db(cls.db_name).runInteraction(*args, **kw)

    @classmethod
    def _wrap_results(cls, results):
        return [cls(*r) for r in results]

    @classmethod
    def _select_query(cls, suffix):
        return "SELECT * FROM %s %s" % (cls.table_name, suffix)

    @classmethod
    def run_select(cls, suffix, *args, **kw):
        d = cls.runQuery(cls._select_query(suffix), *args, **kw)
        d.addCallback(cls._wrap_results)
        return d

    @classmethod
    def _insert_values_query(cls, **kw):
        fields = kw.keys()
        valuespecs = ", ".join(["%%(%s)s" % f for f in fields])
        return "INSERT INTO %s (%s) VALUES (%s)" % (cls.table_name, ", ".join(fields), valuespecs)

    @classmethod
    def insert_values(cls, **kw):
        return cls.runOperation(cls._insert_values_query(**kw), kw)






class UglyNoDbModel(object):
    table_name = None
    fields = ()

    def __init__(self, txn, *args):
        if len(args) != len(self.fields):
            raise ValueError("Bad values.")
        for f, v in zip([f[0] for f in self.fields], args):
            setattr(self, f, v)
        self.txn = txn

    @classmethod
    def create_table(cls, db, if_not_exists=False):
        ine = ""
        if if_not_exists:
            ine = "IF NOT EXISTS "
        cols = ', '.join([' '.join(f) for f in cls.fields])
        query = ''.join(['CREATE TABLE ', ine, cls.table_name, ' (', cols, ')'])
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

