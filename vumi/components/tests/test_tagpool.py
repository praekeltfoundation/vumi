# -*- coding: utf-8 -*-

"""Tests for vumi.components.tagpool."""

import json

from twisted.trial.unittest import TestCase

from vumi.persist.fake_redis import FakeRedis

from vumi.components.tagpool import TagpoolManager, TagpoolError


class TestTagpoolManager(TestCase):

    def setUp(self):
        self.tagpool = TagpoolManager(FakeRedis())

    def pool_key_generator(self, pool):
        def tkey(x):
            return ":".join(["tagpools", pool, x])
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

    def test_purge_pool(self):
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        self.tagpool.declare_tags([tag1, tag2])
        self.tagpool.purge_pool('poolA')
        self.assertEqual(self.tagpool.acquire_tag('poolA'), None)

    def test_purge_inuse_pool(self):
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        self.tagpool.declare_tags([tag1, tag2])
        self.assertEqual(self.tagpool.acquire_tag('poolA'), tag1)
        self.assertRaises(TagpoolError, self.tagpool.purge_pool, 'poolA')

    def test_list_pools(self):
        tag1, tag2 = ("poolA", "tag1"), ("poolB", "tag2")
        self.tagpool.declare_tags([tag1, tag2])
        self.assertEqual(self.tagpool.list_pools(),
                         set(['poolA', 'poolB']))

    def test_acquire_tag(self):
        tkey = self.pool_key_generator("poolA")
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        self.tagpool.declare_tags([tag1, tag2])
        self.assertEqual(self.tagpool.acquire_tag("poolA"), tag1)
        self.assertEqual(self.tagpool.acquire_tag("poolB"), None)
        self.assertEqual(self.tagpool.redis.lrange(tkey("free:list"),
                                                   0, -1),
                         ["tag2"])
        self.assertEqual(self.tagpool.redis.smembers(tkey("free:set")),
                         set(["tag2"]))
        self.assertEqual(self.tagpool.redis.smembers(tkey("inuse:set")),
                         set(["tag1"]))

    def test_acquire_specific_tag(self):
        tkey = self.pool_key_generator("poolA")
        tags = [("poolA", "tag%d" % i) for i in range(10)]
        tag5 = tags[5]
        self.tagpool.declare_tags(tags)
        self.assertEqual(self.tagpool.acquire_specific_tag(tag5), tag5)
        self.assertEqual(self.tagpool.acquire_specific_tag(tag5), None)
        free_local_tags = [t[1] for t in tags]
        free_local_tags.remove("tag5")
        self.assertEqual(self.tagpool.redis.lrange(tkey("free:list"),
                                                   0, -1),
                         free_local_tags)
        self.assertEqual(self.tagpool.redis.smembers(tkey("free:set")),
                         set(free_local_tags))
        self.assertEqual(self.tagpool.redis.smembers(tkey("inuse:set")),
                         set(["tag5"]))

    def test_release_tag(self):
        tkey = self.pool_key_generator("poolA")
        tag1, tag2, tag3 = [("poolA", "tag%d" % i) for i in (1, 2, 3)]
        self.tagpool.declare_tags([tag1, tag2, tag3])
        self.tagpool.acquire_tag("poolA")
        self.tagpool.acquire_tag("poolA")
        self.tagpool.release_tag(tag1)
        self.assertEqual(self.tagpool.redis.lrange(tkey("free:list"),
                                                   0, -1),
                         ["tag3", "tag1"])
        self.assertEqual(self.tagpool.redis.smembers(tkey("free:set")),
                         set(["tag1", "tag3"]))
        self.assertEqual(self.tagpool.redis.smembers(tkey("inuse:set")),
                         set(["tag2"]))

    def test_metadata(self):
        mkey = self.pool_key_generator("poolA")("metadata")
        mget = lambda field: json.loads(self.tagpool.redis.hget(mkey,
                                                                field))
        metadata = {
            "transport_type": "sms",
            "default_msg_fields": {
                "transport_name": "sphex",
                "helper_metadata": {
                    "even_more_nested": "foo",
                },
            },
        }
        self.tagpool.set_metadata("poolA", metadata)
        self.assertEqual(self.tagpool.get_metadata("poolA"), metadata)
        self.assertEqual(mget("transport_type"), "sms")

        short_metadata = {"foo": "bar"}
        self.tagpool.set_metadata("poolA", short_metadata)
        self.assertEqual(self.tagpool.get_metadata("poolA"), short_metadata)
