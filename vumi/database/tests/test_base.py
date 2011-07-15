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

    def recreate_table(self, d, table):
        d.addCallback(lambda _: table.drop_table(self.db))
        d.addCallback(lambda _: table.create_table(self.db))
        return d

    def _sdb(self):
        if getattr(self, 'db', None):
            close_db('test')
        # TODO: Pull this out into some kind of config?
        setup_db('test', user='vumi', password='vumi', database='test')
        self.db = get_db('test')
        return succeed(None)

    def setup_db(self, table, *tables):
        def _eb(f):
            raise SkipTest("Unable to connect to test database: %s" % (f.getErrorMessage(),))
        d = self._sdb()
        d.pause()
        self.recreate_table(d, table)
        d.addErrback(_eb)
        d.unpause()
        for tbl in tables:
            self.recreate_table(d, tbl)
        return d

    def close_db(self):
        close_db('test')
        self.db = None
        return succeed(None)

