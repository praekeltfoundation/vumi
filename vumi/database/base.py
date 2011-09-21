# -*- test-case-name: vumi.database.tests.test_base -*-

from twisted.internet.defer import DeferredList
from twisted.enterprise import adbapi

DATABASES = {}


def setup_db(name, *args, **kw):
    if DATABASES.get(name, None):
        raise ValueError("Database %s already exists." % (name,))
    # See twisted ticket #2680 for this.
    adbapi.ConnectionPool.shutdownID = None
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


class TableNamePrefixFormatter(object):
    """
    This is a custom mapping object to add a prefix to table names.

    To use it, treat table names as keys into a format dict and then
    format with an instance of this class, constructed with the prefix
    to use. For example::

    >>> '%(mytable)s' % TableNamePrefixFormatter()
    'mytable'
    >>> '%(mytable)s' % TableNamePrefixFormatter('pre')
    'pre_mytable'

    """

    def __init__(self, prefix=None):
        self.prefix = prefix

    def __getitem__(self, key):
        if self.prefix is not None:
            return "%s_%s" % (self.prefix, key)
        return key


class UglyModel(object):
    TABLE_NAME = None
    TABLE_PREFIX = None
    fields = ()
    indexes = ()

    def __init__(self, txn, *args):
        if len(args) != len(self.fields):
            raise ValueError("Bad values.")
        for f, v in zip([f[0] for f in self.fields], args):
            setattr(self, f, v)
        self.txn = txn

    @classmethod
    def with_table_prefix(cls, prefix):
        if prefix is None:
            return cls
        return type("%s_%s" % (cls.__name__, prefix), (cls,),
                    {'TABLE_PREFIX': prefix})

    @classmethod
    def get_table_name(cls):
        if cls.TABLE_PREFIX is not None:
            return "%s_%s" % (cls.TABLE_PREFIX, cls.TABLE_NAME)
        return cls.TABLE_NAME

    @classmethod
    def create_table(cls, db):
        cols = ', '.join([' '.join(f) for f in cls.fields])
        query = ''.join(['CREATE TABLE ',
                         cls.get_table_name(),
                         ' (', cols, ')'])
        query %= TableNamePrefixFormatter(cls.TABLE_PREFIX)
        d = db.runOperation(query)
        d.addCallback(lambda _: cls.create_indexes(db))
        return d

    @classmethod
    def drop_table(cls, db, if_exists=True, cascade=False):
        d = cls.drop_indexes(db, if_exists, cascade)
        q = "DROP TABLE "
        if if_exists:
            q += "IF EXISTS "
        q += cls.get_table_name()
        if cascade:
            q += " CASCADE"
        return d.addCallback(lambda _: db.runOperation(q))

    @classmethod
    def create_indexes(cls, db):
        deferreds = []
        for index in cls.indexes:
            name = 'idx_%s_%s' % (cls.get_table_name(), index)
            query = 'CREATE INDEX %s ON %s (%s)' % (
                name, cls.get_table_name(), index)
            deferreds.append(db.runOperation(query))
        return DeferredList(deferreds)

    @classmethod
    def drop_indexes(cls, db, if_exists=True, cascade=False):
        deferreds = []
        for index in cls.indexes:
            q = "DROP INDEX "
            if if_exists:
                q += "IF EXISTS "
            q += index
            if cascade:
                q += " CASCADE"
            deferreds.append(db.runOperation(q))
        return DeferredList(deferreds)

    @classmethod
    def wrap_results(cls, txn, results):
        return [cls(txn, *r) for r in results]

    @classmethod
    def select_query(cls, suffix):
        return "SELECT * FROM %s %s" % (cls.get_table_name(), suffix)

    @classmethod
    def run_select(cls, txn, suffix, *args, **kw):
        txn.execute(cls.select_query(suffix), *args, **kw)
        return txn.fetchall()

    @classmethod
    def insert_values_query(cls, **kw):
        fields = kw.keys()
        valuespecs = ", ".join(["%%(%s)s" % f for f in fields])
        return "INSERT INTO %s (%s) VALUES (%s)" % (
            cls.get_table_name(), ", ".join(fields), valuespecs)

    @classmethod
    def count_rows(cls, txn, suffix='', values=None):
        txn.execute("SELECT count(1) FROM %s %s" % (
                cls.get_table_name(), suffix), values)
        return txn.fetchone()[0]
