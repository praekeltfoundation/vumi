"""Tests for vumi.persist.redis_manager."""

from twisted.trial.unittest import TestCase

from vumi.persist.redis_manager import RedisManager


class RedisManagerTestCase(TestCase):
    def setUp(self):
        self.manager = RedisManager.from_config({'key_prefix': 'redistest'})
        self.manager._purge_all()

    def tearDown(self):
        self.manager._purge_all()
        self.manager._close()

    def test_key_unkey(self):
        self.assertEqual('redistest#foo', self.manager._key('foo'))
        self.assertEqual('foo', self.manager._unkey('redistest#foo'))
        self.assertEqual('redistest#redistest#foo',
                         self.manager._key('redistest#foo'))
        self.assertEqual('redistest#foo',
                         self.manager._unkey('redistest#redistest#foo'))

    def test_set_get_keys(self):
        self.assertEqual([], self.manager.keys())
        self.assertEqual(None, self.manager.get('foo'))
        self.manager.set('foo', 'bar')
        self.assertEqual(['foo'], self.manager.keys())
        self.assertEqual('bar', self.manager.get('foo'))
        self.manager.set('foo', 'baz')
        self.assertEqual(['foo'], self.manager.keys())
        self.assertEqual('baz', self.manager.get('foo'))
