from twisted.trial.unittest import TestCase, SkipTest
from twisted.internet.defer import succeed

from vumi.database.base import setup_db, get_db, close_db

class UglyModelTestCase(TestCase):
    def ri(self, *args, **kw):
        return self.db.runInteraction(*args, **kw)

    def ricb(self, _r, *args, **kw):
        return self.db.runInteraction(*args, **kw)

    def setup_db(self, cb):
        def _sdb():
            close_db('test')
            # TODO: Pull this out into some kind of config?
            setup_db('test', 'psycopg2', user='vumi', password='vumi', database='test')
            self.db = get_db('test')
            return succeed(None)
        def _eb(f):
            raise SkipTest("Unable to connect to test database: %s" % (f.getErrorMessage(),))
        return _sdb().addCallback(cb).addErrback(_eb)

    def close_db(self):
        close_db('test')

