"""Tests for vumi.persist.redis_manager."""

from vumi.tests.helpers import VumiTestCase, import_skip


class TestRedisManager(VumiTestCase):
    def setUp(self):
        try:
            from vumi.persist.redis_manager import RedisManager
        except ImportError, e:
            import_skip(e, 'redis')

        self.manager = RedisManager.from_config(
            {'FAKE_REDIS': 'yes',
             'key_prefix': 'redistest'})
        self.add_cleanup(self.cleanup_manager)
        self.manager._purge_all()

    def cleanup_manager(self):
        self.manager._purge_all()
        self.manager._close()

    def test_key_unkey(self):
        self.assertEqual('redistest:foo', self.manager._key('foo'))
        self.assertEqual('foo', self.manager._unkey('redistest:foo'))
        self.assertEqual('redistest:redistest:foo',
                         self.manager._key('redistest:foo'))
        self.assertEqual('redistest:foo',
                         self.manager._unkey('redistest:redistest:foo'))

    def test_set_get_keys(self):
        self.assertEqual([], self.manager.keys())
        self.assertEqual(None, self.manager.get('foo'))
        self.manager.set('foo', 'bar')
        self.assertEqual(['foo'], self.manager.keys())
        self.assertEqual('bar', self.manager.get('foo'))
        self.manager.set('foo', 'baz')
        self.assertEqual(['foo'], self.manager.keys())
        self.assertEqual('baz', self.manager.get('foo'))

    def test_disconnect_twice(self):
        self.manager._close()
        self.manager._close()

    def test_scan(self):
        self.assertEqual([], self.manager.keys())
        for i in range(10):
            self.manager.set('key%d' % i, 'value%d' % i)
        all_keys = set()
        cursor = None
        while True:
            cursor, keys = self.manager.scan(cursor)
            all_keys.update(keys)
            if cursor is None:
                break
        self.assertEqual(all_keys, set(
            'key%d' % i for i in range(10)))
