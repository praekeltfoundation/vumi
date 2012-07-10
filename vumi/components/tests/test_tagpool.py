# -*- coding: utf-8 -*-

"""Tests for vumi.components.tagpool."""

import json

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.persist.txredis_manager import TxRedisManager
from vumi.persist.redis_manager import RedisManager

from vumi.components.tagpool import TagpoolManager, TagpoolError


class TestTxTagpoolManager(TestCase):

    @inlineCallbacks
    def setUp(self):
        redis = yield TxRedisManager.from_config('FAKE_REDIS', 'teststore')
        self.tagpool = TagpoolManager(redis)

    def pool_key_generator(self, pool):
        def tkey(x):
            return ":".join(["tagpools", pool, x])
        return tkey

    @inlineCallbacks
    def test_declare_tags(self):
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        yield self.tagpool.declare_tags([tag1, tag2])
        self.assertEqual((yield self.tagpool.acquire_tag("poolA")), tag1)
        self.assertEqual((yield self.tagpool.acquire_tag("poolA")), tag2)
        self.assertEqual((yield self.tagpool.acquire_tag("poolA")), None)
        tag3 = ("poolA", "tag3")
        yield self.tagpool.declare_tags([tag2, tag3])
        self.assertEqual((yield self.tagpool.acquire_tag("poolA")), tag3)

    @inlineCallbacks
    def test_purge_pool(self):
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        yield self.tagpool.declare_tags([tag1, tag2])
        yield self.tagpool.purge_pool('poolA')
        self.assertEqual((yield self.tagpool.acquire_tag('poolA')), None)

    @inlineCallbacks
    def test_purge_inuse_pool(self):
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        yield self.tagpool.declare_tags([tag1, tag2])
        self.assertEqual((yield self.tagpool.acquire_tag('poolA')), tag1)
        try:
            yield self.tagpool.purge_pool('poolA')
        except TagpoolError:
            pass
        else:
            self.fail("Expected TagpoolError to be raised.")

    @inlineCallbacks
    def test_list_pools(self):
        tag1, tag2 = ("poolA", "tag1"), ("poolB", "tag2")
        yield self.tagpool.declare_tags([tag1, tag2])
        self.assertEqual((yield self.tagpool.list_pools()),
                         set(['poolA', 'poolB']))

    @inlineCallbacks
    def test_acquire_tag(self):
        tkey = self.pool_key_generator("poolA")
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        yield self.tagpool.declare_tags([tag1, tag2])
        self.assertEqual((yield self.tagpool.acquire_tag("poolA")), tag1)
        self.assertEqual((yield self.tagpool.acquire_tag("poolB")), None)
        redis = self.tagpool.redis
        self.assertEqual((yield redis.lrange(tkey("free:list"), 0, -1)),
                         ["tag2"])
        self.assertEqual((yield redis.smembers(tkey("free:set"))),
                         set(["tag2"]))
        self.assertEqual((yield redis.smembers(tkey("inuse:set"))),
                         set(["tag1"]))

    @inlineCallbacks
    def test_acquire_specific_tag(self):
        tkey = self.pool_key_generator("poolA")
        tags = [("poolA", "tag%d" % i) for i in range(10)]
        tag5 = tags[5]
        yield self.tagpool.declare_tags(tags)
        self.assertEqual((yield self.tagpool.acquire_specific_tag(tag5)), tag5)
        self.assertEqual((yield self.tagpool.acquire_specific_tag(tag5)), None)
        free_local_tags = [t[1] for t in tags]
        free_local_tags.remove("tag5")
        redis = self.tagpool.redis
        self.assertEqual((yield redis.lrange(tkey("free:list"), 0, -1)),
                         free_local_tags)
        self.assertEqual((yield redis.smembers(tkey("free:set"))),
                         set(free_local_tags))
        self.assertEqual((yield redis.smembers(tkey("inuse:set"))),
                         set(["tag5"]))

    @inlineCallbacks
    def test_release_tag(self):
        tkey = self.pool_key_generator("poolA")
        tag1, tag2, tag3 = [("poolA", "tag%d" % i) for i in (1, 2, 3)]
        yield self.tagpool.declare_tags([tag1, tag2, tag3])
        yield self.tagpool.acquire_tag("poolA")
        yield self.tagpool.acquire_tag("poolA")
        yield self.tagpool.release_tag(tag1)
        redis = self.tagpool.redis
        self.assertEqual((yield redis.lrange(tkey("free:list"), 0, -1)),
                         ["tag3", "tag1"])
        self.assertEqual((yield redis.smembers(tkey("free:set"))),
                         set(["tag1", "tag3"]))
        self.assertEqual((yield redis.smembers(tkey("inuse:set"))),
                         set(["tag2"]))

    @inlineCallbacks
    def test_metadata(self):
        mkey = self.pool_key_generator("poolA")("metadata")
        metadata = {
            "transport_type": "sms",
            "default_msg_fields": {
                "transport_name": "sphex",
                "helper_metadata": {
                    "even_more_nested": "foo",
                },
            },
        }
        yield self.tagpool.set_metadata("poolA", metadata)
        self.assertEqual((yield self.tagpool.get_metadata("poolA")),
                         metadata)
        tt_json = yield self.tagpool.redis.hget(mkey, "transport_type")
        transport_type = json.loads(tt_json)
        self.assertEqual(transport_type, "sms")

        short_metadata = {"foo": "bar"}
        yield self.tagpool.set_metadata("poolA", short_metadata)
        self.assertEqual((yield self.tagpool.get_metadata("poolA")),
                         short_metadata)


class TestTagpoolManager(TestTxTagpoolManager):

    def setUp(self):
        redis = RedisManager.from_config('FAKE_REDIS', 'teststore')
        self.tagpool = TagpoolManager(redis)
