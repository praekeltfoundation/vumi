# -*- coding: utf-8 -*-

"""Tests for vumi.components.tagpool."""

import json

from twisted.internet.defer import inlineCallbacks

from vumi.components.tagpool import TagpoolManager, TagpoolError
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


class TestTxTagpoolManager(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.redis = yield self.persistence_helper.get_redis_manager()
        yield self.redis._purge_all()  # Just in case
        self.tpm = TagpoolManager(self.redis)

    def pool_key_generator(self, pool):
        def tkey(x):
            return "tagpools:%s:%s" % (pool, x)
        return tkey

    @inlineCallbacks
    def test_declare_tags(self):
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        yield self.tpm.declare_tags([tag1, tag2])
        self.assertEqual((yield self.tpm.acquire_tag("poolA")), tag1)
        self.assertEqual((yield self.tpm.acquire_tag("poolA")), tag2)
        self.assertEqual((yield self.tpm.acquire_tag("poolA")), None)
        tag3 = ("poolA", "tag3")
        yield self.tpm.declare_tags([tag2, tag3])
        self.assertEqual((yield self.tpm.acquire_tag("poolA")), tag3)

    @inlineCallbacks
    def test_declare_unicode_tag(self):
        tag = (u"poöl", u"tág")
        yield self.tpm.declare_tags([tag])
        self.assertEqual((yield self.tpm.acquire_tag(tag[0])), tag)

    @inlineCallbacks
    def test_purge_pool(self):
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        yield self.tpm.declare_tags([tag1, tag2])
        yield self.tpm.purge_pool('poolA')
        self.assertEqual((yield self.tpm.acquire_tag('poolA')), None)

    @inlineCallbacks
    def test_purge_unicode_pool(self):
        tag = (u"poöl", u"tág")
        yield self.tpm.declare_tags([tag])
        yield self.tpm.purge_pool(tag[0])
        self.assertEqual((yield self.tpm.acquire_tag(tag[0])), None)

    @inlineCallbacks
    def test_purge_inuse_pool(self):
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        yield self.tpm.declare_tags([tag1, tag2])
        self.assertEqual((yield self.tpm.acquire_tag('poolA')), tag1)
        try:
            yield self.tpm.purge_pool('poolA')
        except TagpoolError:
            pass
        else:
            self.fail("Expected TagpoolError to be raised.")

    @inlineCallbacks
    def test_list_pools(self):
        tag1, tag2 = ("poolA", "tag1"), ("poolB", "tag2")
        yield self.tpm.declare_tags([tag1, tag2])
        self.assertEqual((yield self.tpm.list_pools()),
                         set(['poolA', 'poolB']))

    @inlineCallbacks
    def test_list_unicode_pool(self):
        tag = (u"poöl", u"tág")
        yield self.tpm.declare_tags([tag])
        self.assertEqual((yield self.tpm.list_pools()),
                         set([tag[0]]))

    @inlineCallbacks
    def test_acquire_tag(self):
        tkey = self.pool_key_generator("poolA")
        tag1, tag2 = ("poolA", "tag1"), ("poolA", "tag2")
        yield self.tpm.declare_tags([tag1, tag2])
        self.assertEqual((yield self.tpm.acquire_tag("poolA")), tag1)
        self.assertEqual((yield self.tpm.acquire_tag("poolB")), None)
        redis = self.redis
        self.assertEqual((yield redis.lrange(tkey("free:list"), 0, -1)),
                         ["tag2"])
        self.assertEqual((yield redis.smembers(tkey("free:set"))),
                         set(["tag2"]))
        self.assertEqual((yield redis.smembers(tkey("inuse:set"))),
                         set(["tag1"]))

    @inlineCallbacks
    def test_acquire_unicode_tag(self):
        tag = (u"poöl", u"tág")
        yield self.tpm.declare_tags([tag])
        self.assertEqual((yield self.tpm.acquire_tag(tag[0])), tag)
        self.assertEqual((yield self.tpm.acquire_tag(tag[0])), None)

    @inlineCallbacks
    def test_acquire_specific_tag(self):
        tkey = self.pool_key_generator("poolA")
        tags = [("poolA", "tag%d" % i) for i in range(10)]
        tag5 = tags[5]
        yield self.tpm.declare_tags(tags)
        self.assertEqual((yield self.tpm.acquire_specific_tag(tag5)), tag5)
        self.assertEqual((yield self.tpm.acquire_specific_tag(tag5)), None)
        free_local_tags = [t[1] for t in tags]
        free_local_tags.remove("tag5")
        redis = self.redis
        self.assertEqual((yield redis.lrange(tkey("free:list"), 0, -1)),
                         free_local_tags)
        self.assertEqual((yield redis.smembers(tkey("free:set"))),
                         set(free_local_tags))
        self.assertEqual((yield redis.smembers(tkey("inuse:set"))),
                         set(["tag5"]))

    @inlineCallbacks
    def test_acquire_specific_unicode_tag(self):
        tag = (u"poöl", u"tág")
        yield self.tpm.declare_tags([tag])
        self.assertEqual((yield self.tpm.acquire_specific_tag(tag)), tag)
        self.assertEqual((yield self.tpm.acquire_specific_tag(tag)), None)

    @inlineCallbacks
    def test_release_tag(self):
        tkey = self.pool_key_generator("poolA")
        tag1, tag2, tag3 = [("poolA", "tag%d" % i) for i in (1, 2, 3)]
        yield self.tpm.declare_tags([tag1, tag2, tag3])
        yield self.tpm.acquire_tag("poolA")
        yield self.tpm.acquire_tag("poolA")
        yield self.tpm.release_tag(tag1)
        redis = self.redis
        self.assertEqual((yield redis.lrange(tkey("free:list"), 0, -1)),
                         ["tag3", "tag1"])
        self.assertEqual((yield redis.smembers(tkey("free:set"))),
                         set(["tag1", "tag3"]))
        self.assertEqual((yield redis.smembers(tkey("inuse:set"))),
                         set(["tag2"]))

    @inlineCallbacks
    def test_release_unicode_tag(self):
        tag = (u"poöl", u"tág")
        yield self.tpm.declare_tags([tag])
        yield self.tpm.acquire_tag(tag[0])
        yield self.tpm.release_tag(tag)
        self.assertEqual((yield self.tpm.acquire_tag(tag[0])), tag)

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
        yield self.tpm.set_metadata("poolA", metadata)
        self.assertEqual((yield self.tpm.get_metadata("poolA")), metadata)
        tt_json = yield self.redis.hget(mkey, "transport_type")
        transport_type = json.loads(tt_json)
        self.assertEqual(transport_type, "sms")

        short_md = {"foo": "bar"}
        yield self.tpm.set_metadata("poolA", short_md)
        self.assertEqual((yield self.tpm.get_metadata("poolA")), short_md)

    @inlineCallbacks
    def test_metadata_for_unicode_pool_name(self):
        pool = u"poöl"
        metadata = {"foo": "bar"}
        yield self.tpm.set_metadata(pool, metadata)
        self.assertEqual((yield self.tpm.get_metadata(pool)),
                         metadata)

    @inlineCallbacks
    def test_unicode_metadata(self):
        metadata = {u"föo": u"báz"}
        yield self.tpm.set_metadata("pool", metadata)
        self.assertEqual((yield self.tpm.get_metadata("pool")),
                         metadata)

    def _check_reason(self, expected_owner, owner, reason, expected_data):
        self.assertEqual(expected_owner, owner)
        self.assertEqual(expected_owner, reason.pop('owner'))
        timestamp = reason.pop('timestamp')
        self.assertTrue(isinstance(timestamp, float))
        self.assertEqual(reason, expected_data)

    @inlineCallbacks
    def test_acquired_by(self):
        tag = ["pool", "tag"]
        yield self.tpm.declare_tags([tag])
        yield self.tpm.acquire_tag(tag[0], "me", {"foo": "bar"})
        owner, reason = yield self.tpm.acquired_by(tag)
        self._check_reason("me", owner, reason, {"foo": "bar"})

    @inlineCallbacks
    def test_acquired_by_undeclared_tags(self):
        tag = ["pool", "tag"]
        owner, reason = yield self.tpm.acquired_by(tag)
        self.assertEqual(owner, None)
        self.assertEqual(reason, None)

    @inlineCallbacks
    def test_acquired_by_no_owner(self):
        tag = ["pool", "tag"]
        yield self.tpm.declare_tags([tag])
        yield self.tpm.acquire_tag(tag[0])
        owner, reason = yield self.tpm.acquired_by(tag)
        self._check_reason(None, owner, reason, {})

    @inlineCallbacks
    def test_acquired_by_unicode_owner(self):
        tag = ["pool", "tag"]
        yield self.tpm.declare_tags([tag])
        yield self.tpm.acquire_tag(tag[0], u"mé")
        owner, reason = yield self.tpm.acquired_by(tag)
        self._check_reason(u"mé", owner, reason, {})

    @inlineCallbacks
    def test_acquired_by_from_unicode_tag(self):
        tag = [u"poöl", u"tág"]
        yield self.tpm.declare_tags([tag])
        yield self.tpm.acquire_tag(tag[0], "me")
        owner, reason = yield self.tpm.acquired_by(tag)
        self._check_reason(u"me", owner, reason, {})

    @inlineCallbacks
    def test_acquired_by_after_using_specific_tag(self):
        tag = ["pool", "tag"]
        yield self.tpm.declare_tags([tag])
        yield self.tpm.acquire_specific_tag(tag, "me", {"foo": "bar"})
        owner, reason = yield self.tpm.acquired_by(tag)
        self._check_reason("me", owner, reason, {"foo": "bar"})

    @inlineCallbacks
    def test_owned_tags(self):
        tags = [["pool1", "tag1"], ["pool2", "tag2"]]
        yield self.tpm.declare_tags(tags)
        yield self.tpm.acquire_tag(tags[0][0], owner="me")
        my_tags = yield self.tpm.owned_tags("me")
        self.assertEqual(my_tags, [tags[0]])

    @inlineCallbacks
    def test_owned_tags_no_owner(self):
        tags = [["pool1", "tag1"], ["pool2", "tag2"]]
        yield self.tpm.declare_tags(tags)
        yield self.tpm.acquire_tag(tags[0][0])
        my_tags = yield self.tpm.owned_tags(None)
        self.assertEqual(my_tags, [tags[0]])

    @inlineCallbacks
    def test_owned_tags_unicode_owner(self):
        tags = [["pool1", "tag1"], ["pool2", "tag2"]]
        yield self.tpm.declare_tags(tags)
        yield self.tpm.acquire_tag(tags[0][0], owner=u"mé")
        my_tags = yield self.tpm.owned_tags(u"mé")
        self.assertEqual(my_tags, [tags[0]])

    @inlineCallbacks
    def test_owned_tags_unicode_tags(self):
        tags = [[u"poöl1", u"tág1"], [u"poöl2", u"tág2"]]
        yield self.tpm.declare_tags(tags)
        yield self.tpm.acquire_tag(tags[0][0], owner="me")
        my_tags = yield self.tpm.owned_tags(u"me")
        self.assertEqual(my_tags, [tags[0]])


class TestTagpoolManager(TestTxTagpoolManager):
    sync_persistence = True
