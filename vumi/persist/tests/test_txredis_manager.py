"""Tests for vumi.persist.txredis_manager."""

import os
from functools import wraps

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from twisted.trial.unittest import SkipTest

from vumi.persist.txredis_manager import TxRedisManager
from vumi.tests.helpers import VumiTestCase


def wait(secs):
    d = Deferred()
    reactor.callLater(secs, d.callback, None)
    return d


def skip_fake_redis(func):
    @wraps(func)
    def wrapper(*args, **kw):
        if 'VUMITEST_REDIS_DB' not in os.environ:
            # We're using a fake redis, so skip this test.
            raise SkipTest(
                "This test requires a real Redis server. Set VUMITEST_REDIS_DB"
                " to run it.")
        return func(*args, **kw)
    return wrapper


class TestTxRedisManager(VumiTestCase):
    @inlineCallbacks
    def get_manager(self):
        manager = yield TxRedisManager.from_config({
            'FAKE_REDIS': 'yes',
            'key_prefix': 'redistest',
        })
        self.add_cleanup(self.cleanup_manager, manager)
        yield manager._purge_all()
        returnValue(manager)

    @inlineCallbacks
    def cleanup_manager(self, manager):
        yield manager._purge_all()
        yield manager._close()

    @inlineCallbacks
    def test_key_unkey(self):
        manager = yield self.get_manager()
        self.assertEqual('redistest:foo', manager._key('foo'))
        self.assertEqual('foo', manager._unkey('redistest:foo'))
        self.assertEqual('redistest:redistest:foo',
                         manager._key('redistest:foo'))
        self.assertEqual('redistest:foo',
                         manager._unkey('redistest:redistest:foo'))

    @inlineCallbacks
    def test_set_get_keys(self):
        manager = yield self.get_manager()
        self.assertEqual([], (yield manager.keys()))
        self.assertEqual(None, (yield manager.get('foo')))
        yield manager.set('foo', 'bar')
        self.assertEqual(['foo'], (yield manager.keys()))
        self.assertEqual('bar', (yield manager.get('foo')))
        yield manager.set('foo', 'baz')
        self.assertEqual(['foo'], (yield manager.keys()))
        self.assertEqual('baz', (yield manager.get('foo')))

    @inlineCallbacks
    def test_disconnect_twice(self):
        manager = yield self.get_manager()
        yield manager._close()
        yield manager._close()

    @inlineCallbacks
    def test_scan(self):
        manager = yield self.get_manager()
        self.assertEqual([], (yield manager.keys()))
        for i in range(10):
            yield manager.set('key%d' % i, 'value%d' % i)
        all_keys = set()
        cursor = None
        for i in range(20):
            # loop enough times to have gone through all the keys in our test
            # redis instance but not forever so we can assert on the value of
            # cursor if we get stuck.
            cursor, keys = yield manager.scan(cursor)
            all_keys.update(keys)
            if cursor is None:
                break
        self.assertEqual(cursor, None)
        self.assertEqual(all_keys, set(
            'key%d' % i for i in range(10)))

    @inlineCallbacks
    def test_ttl(self):
        manager = yield self.get_manager()
        missing_ttl = yield manager.ttl("missing_key")
        self.assertEqual(missing_ttl, None)

        yield manager.set("key-no-ttl", "value")
        no_ttl = yield manager.ttl("key-no-ttl")
        self.assertEqual(no_ttl, None)

        yield manager.setex("key-ttl", 30, "value")
        ttl = yield manager.ttl("key-ttl")
        self.assertTrue(10 <= ttl <= 30)

    @skip_fake_redis
    @inlineCallbacks
    def test_reconnect_sub_managers(self):
        manager = yield self.get_manager()
        sub_manager = manager.sub_manager('subredis')
        sub_sub_manager = sub_manager.sub_manager('subsubredis')

        yield manager.set("foo", "1")
        yield sub_manager.set("foo", "2")
        yield sub_sub_manager.set("foo", "3")

        # Our three managers are all connected properly.
        f1 = yield manager.get("foo")
        f2 = yield sub_manager.get("foo")
        f3 = yield sub_sub_manager.get("foo")
        self.assertEqual([f1, f2, f3], ["1", "2", "3"])

        # Kill the connection and wait a few moments for the reconnect.
        yield manager._client.quit()
        yield wait(manager._client.factory.initialDelay + 0.05)

        # Our three managers are all reconnected properly.
        f1 = yield manager.get("foo")
        f2 = yield sub_manager.get("foo")
        f3 = yield sub_sub_manager.get("foo")
        self.assertEqual([f1, f2, f3], ["1", "2", "3"])
