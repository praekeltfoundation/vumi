from datetime import datetime, timedelta

from twisted.trial.unittest import TestCase, SkipTest
from twisted.internet.defer import succeed
import pytz

from vumi.database.base import setup_db, get_db, close_db


class UTCNearNow(object):
    def __init__(self, offset=10):
        self.now = datetime.utcnow().replace(tzinfo=pytz.UTC)
        self.offset = timedelta(offset)

    def __eq__(self, other):
        return (self.now - self.offset) < other < (self.now + self.offset)


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
        for tbl in reversed(self._test_tables):
            d.addCallback(lambda _: tbl.drop_table(self.db))

        def _cb(_):
            close_db(self._dbname)
            self.db = None
        return d.addCallback(_cb)
