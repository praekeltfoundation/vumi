"""Tests for vumi.persist.redis_base."""

from twisted.trial.unittest import TestCase

from vumi.persist.redis_base import Manager


class ManagerTestCase(TestCase):
    def mk_manager(self, client=None, key_prefix='test'):
        if client is None:
            client = object()
        return Manager(client, key_prefix)

    def test_sub_manager(self):
        manager = self.mk_manager()
        sub_manager = manager.sub_manager("foo")
        self.assertEqual(sub_manager._key_prefix, "test#foo")
        self.assertEqual(sub_manager._client, manager._client)
        self.assertEqual(sub_manager._key_separator, manager._key_separator)
