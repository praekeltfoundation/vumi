"""Tests for vumi.persist.txredis_manager."""

from twisted.internet.defer import inlineCallbacks

from vumi.persist.txredis_manager import TxRedisManager
from vumi.tests.helpers import VumiTestCase


class TestTxRedisManager(VumiTestCase):
    @inlineCallbacks
    def setUp(self):
        self.manager = yield TxRedisManager.from_config(
            {'FAKE_REDIS': 'yes',
             'key_prefix': 'redistest'})
        self.add_cleanup(self.cleanup_manager)
        yield self.manager._purge_all()

    @inlineCallbacks
    def cleanup_manager(self):
        yield self.manager._purge_all()
        yield self.manager._close()

    def test_key_unkey(self):
        self.assertEqual('redistest:foo', self.manager._key('foo'))
        self.assertEqual('foo', self.manager._unkey('redistest:foo'))
        self.assertEqual('redistest:redistest:foo',
                         self.manager._key('redistest:foo'))
        self.assertEqual('redistest:foo',
                         self.manager._unkey('redistest:redistest:foo'))

    @inlineCallbacks
    def test_set_get_keys(self):
        self.assertEqual([], (yield self.manager.keys()))
        self.assertEqual(None, (yield self.manager.get('foo')))
        yield self.manager.set('foo', 'bar')
        self.assertEqual(['foo'], (yield self.manager.keys()))
        self.assertEqual('bar', (yield self.manager.get('foo')))
        yield self.manager.set('foo', 'baz')
        self.assertEqual(['foo'], (yield self.manager.keys()))
        self.assertEqual('baz', (yield self.manager.get('foo')))

    @inlineCallbacks
    def test_disconnect_twice(self):
        yield self.manager._close()
        yield self.manager._close()
