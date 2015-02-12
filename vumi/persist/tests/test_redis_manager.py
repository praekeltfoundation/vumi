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
        for i in range(20):
            # loop enough times to have gone through all the keys in our test
            # redis instance but not forever so we can assert on the value of
            # cursor if we get stuck. Also prevents hanging the whole test
            # suite (since this test doesn't yield to the reactor).
            cursor, keys = self.manager.scan(cursor)
            all_keys.update(keys)
            if cursor is None:
                break
        self.assertEqual(cursor, None)
        self.assertEqual(all_keys, set(
            'key%d' % i for i in range(10)))

    def test_ttl(self):
        missing_ttl = self.manager.ttl("missing_key")
        self.assertEqual(missing_ttl, None)

        self.manager.set("key-no-ttl", "value")
        no_ttl = self.manager.ttl("key-no-ttl")
        self.assertEqual(no_ttl, None)

        self.manager.setex("key-ttl", 30, "value")
        ttl = self.manager.ttl("key-ttl")
        self.assertTrue(10 <= ttl <= 30)
