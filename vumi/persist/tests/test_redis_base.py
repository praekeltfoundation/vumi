"""Tests for vumi.persist.redis_base."""

from twisted.trial.unittest import TestCase

from vumi.persist.redis_base import Manager


class ManagerTestCase(TestCase):
    def mk_manager(self, client, key_prefix='test'):
        return Manager(client, key_prefix)

    def test_sub_manager(self):
        dummy_client = object()
        manager = self.mk_manager(dummy_client)
        sub_manager = manager.sub_manager("foo")
        self.assertEqual(sub_manager._key_prefix, "test#foo")
        self.assertEqual(sub_manager._client, dummy_client)
