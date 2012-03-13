# -*- coding: utf-8 -*-

"""Tests for vumi.application.tagpool."""

from twisted.trial.unittest import TestCase

from vumi.tests.utils import FakeRedis

from vumi.application.tagpool import TagpoolManager


class TestTagpoolManager(TestCase):

    def setUp(self):
        self.prefix = "testpool"
        self.tagpool = TagpoolManager(FakeRedis(), self.prefix)

    def pool_key_generator(self, pool):
        def tkey(x):
            return ":".join([self.prefix, "tagpools", pool, x])
        return tkey

    def test_declare_tags(self):
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        self.tagpool.declare_tags([tag1, tag2])
        self.assertEqual(self.tagpool.acquire_tag("poolA"), tag1)
        self.assertEqual(self.tagpool.acquire_tag("poolA"), tag2)
        self.assertEqual(self.tagpool.acquire_tag("poolA"), None)
        tag3 = ("poolA", "tag3")
        self.tagpool.declare_tags([tag2, tag3])
        self.assertEqual(self.tagpool.acquire_tag("poolA"), tag3)

    def test_acquire_tag(self):
        tkey = self.pool_key_generator("poolA")
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        self.tagpool.declare_tags([tag1, tag2])
        self.assertEqual(self.tagpool.acquire_tag("poolA"), tag1)
        self.assertEqual(self.tagpool.acquire_tag("poolB"), None)
        self.assertEqual(self.tagpool.r_server.lrange(tkey("free:list"),
                                                    0, -1),
                         ["tag2"])
        self.assertEqual(self.tagpool.r_server.smembers(tkey("free:set")),
                         set(["tag2"]))
        self.assertEqual(self.tagpool.r_server.smembers(tkey("inuse:set")),
                         set(["tag1"]))

    def test_release_tag(self):
        tkey = self.pool_key_generator("poolA")
        tag1, tag2, tag3 = [("poolA", "tag%d" % i) for i in (1, 2, 3)]
        self.tagpool.declare_tags([tag1, tag2, tag3])
        self.tagpool.acquire_tag("poolA")
        self.tagpool.acquire_tag("poolA")
        self.tagpool.release_tag(tag1)
        self.assertEqual(self.tagpool.r_server.lrange(tkey("free:list"),
                                                    0, -1),
                         ["tag3", "tag1"])
        self.assertEqual(self.tagpool.r_server.smembers(tkey("free:set")),
                         set(["tag1", "tag3"]))
        self.assertEqual(self.tagpool.r_server.smembers(tkey("inuse:set")),
                         set(["tag2"]))
