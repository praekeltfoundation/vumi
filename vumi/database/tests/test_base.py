from datetime import datetime

import pytz
from twisted.trial.unittest import TestCase, SkipTest
from twisted.internet.defer import succeed

from vumi.tests.utils import UTCNearNow
from vumi.database.base import (setup_db, get_db, close_db, UglyModel,
                                TableNamePrefixFormatter)


class UglyModelTestCase(TestCase):
    def assert_utc_near_now(self, other, offset=10):
        self.assertEquals(UTCNearNow(offset), other)

    def ri(self, *args, **kw):
        return self.db.runInteraction(*args, **kw)

    def ricb(self, _r, *args, **kw):
        return self.db.runInteraction(*args, **kw)

    def _sdb(self, dbname, **kw):
        self._dbname = dbname
        try:
            get_db(dbname)
            close_db(dbname)
        except:
            pass
        # TODO: Pull this out into some kind of config?
        self.db = setup_db(dbname, database=dbname,
                           user=kw.get('dbuser', 'vumi'),
                           password=kw.get('dbpassword', 'vumi'))
        return self.db.runQuery("SELECT 1")

    def setup_db(self, *tables, **kw):
        dbname = kw.pop('dbname', 'test')
        self._test_tables = tables

        def _eb(f):
            raise SkipTest("Unable to connect to test database: %s" % (
                    f.getErrorMessage(),))
        d = self._sdb(dbname)
        d.addErrback(_eb)

        def add_callback(func, *args, **kw):
            # This function exists to create a closure around a
            # variable in the correct scope. If we just add the
            # callback directly in the loops below, we only get the
            # final value of "table", not each intermediate value.
            d.addCallback(lambda _: func(self.db, *args, **kw))
        for table in reversed(tables):
            add_callback(table.drop_table, cascade=True)
        for table in tables:
            add_callback(table.create_table)
        return d

    def shutdown_db(self):
        d = succeed(None)

        def add_callback(func, *args, **kw):
            # This function exists to create a closure around a
            # variable in the correct scope. If we just add the
            # callback directly in the loops below, we only get the
            # final value of "table", not each intermediate value.
            d.addCallback(lambda _: func(self.db, *args, **kw))
        for tbl in reversed(self._test_tables):
            add_callback(tbl.drop_table)

        def _cb(_):
            close_db(self._dbname)
            self.db = None
        return d.addCallback(_cb)


class UglyModelTestCaseTest(UglyModelTestCase):

    def test_assert_utc_near_now(self):
        self.assert_utc_near_now(datetime.utcnow().replace(tzinfo=pytz.UTC))


class ToyModel(UglyModel):
    TABLE_NAME = 'toy_items'
    fields = (
        ('id', 'SERIAL PRIMARY KEY'),
        ('name', 'varchar UNIQUE NOT NULL'),
        ('thingy', 'varchar NOT NULL'),
        ('other_item', 'integer REFERENCES %(toy_items)s'),
        )
    indexes = ('name',)

    @classmethod
    def get_item(cls, txn, name):
        items = cls.run_select(txn, "WHERE name=%(name)s",
                                   {'name': name})
        if items:
            return cls(txn, *items[0])
        return None

    @classmethod
    def create_item(cls, txn, name, thingy):
        params = {'name': name, 'thingy': thingy}
        txn.execute(cls.insert_values_query(**params), params)
        txn.execute("SELECT lastval()")
        return txn.fetchone()[0]


class UglyModelTest(UglyModelTestCase):

    def setUp(self):
        return self.setup_db(ToyModel)

    def tearDown(self):
        return self.shutdown_db()

    def test_create_and_get_item(self):
        """
        A fresh item should be created and then retrieved.
        """
        def _txn(txn):
            self.assertEqual(0, ToyModel.count_rows(txn))
            ToyModel.create_item(txn, 'item1', 'a test item')
            self.assertEqual(1, ToyModel.count_rows(txn))
            item = ToyModel.get_item(txn, 'item1')
            self.assertEqual('item1', item.name)
            self.assertEqual('a test item', item.thingy)
            self.assertEqual(None, item.other_item)
        d = self.ri(_txn)

        def _txn2(txn):
            self.assertEqual(None, ToyModel.get_item(txn, 'missing_item'))
        return d.addCallback(self.ricb, _txn2)

    def test_with_prefix(self):
        """
        with_table_prefix() should create a fresh class with a custom prefix.
        """
        new_model = ToyModel.with_table_prefix('foo')
        self.assertNotEqual(ToyModel, new_model)
        self.assertEqual(None, ToyModel.TABLE_PREFIX)
        self.assertEqual('toy_items', ToyModel.get_table_name())
        self.assertEqual('foo', new_model.TABLE_PREFIX)
        self.assertEqual('foo_toy_items', new_model.get_table_name())

    def test_with_prefix_none(self):
        """
        with_table_prefix() should create a fresh class with a custom prefix.
        """
        self.assertEqual(ToyModel, ToyModel.with_table_prefix(None))
        self.assertEqual(None, ToyModel.TABLE_PREFIX)


class TableNamePrefixFormatterTest(TestCase):
    def test_formatter(self):
        self.assertEqual('mytable',
                         '%(mytable)s' % TableNamePrefixFormatter())
        self.assertEqual('pre_mytable',
                         '%(mytable)s' % TableNamePrefixFormatter('pre'))
