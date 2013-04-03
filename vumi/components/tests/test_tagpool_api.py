# -*- coding: utf-8 -*-

"""Tests for vumi.components.tagpool_api."""

from txjsonrpc.web.jsonrpc import Proxy
from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.web.server import Site

from vumi.components.tagpool_api import TagpoolApiServer
from vumi.components.tagpool import TagpoolManager
from vumi.tests.utils import PersistenceMixin


class TestTagpoolApiServer(TestCase, PersistenceMixin):

    @inlineCallbacks
    def setUp(self):
        self._persist_setUp()
        self.redis = yield self.get_redis_manager()
        self.tagpool = TagpoolManager(self.redis)
        site = Site(TagpoolApiServer(self.tagpool))
        self.server = yield reactor.listenTCP(0, site)
        addr = self.server.getHost()
        self.proxy = Proxy("http://localhost:%d/" % addr.port)
        yield self.setup_tags()

    @inlineCallbacks
    def tearDown(self):
        yield self.server.loseConnection()
        yield self._persist_tearDown()

    @inlineCallbacks
    def setup_tags(self):
        # pool1 has two tags which are free
        yield self.tagpool.declare_tags([
                ("pool1", "tag1"), ("pool1", "tag2")])
        # pool2 has two tags which are used
        yield self.tagpool.declare_tags([
                ("pool2", "tag1"), ("pool2", "tag2")])
        yield self.tagpool.acquire_specific_tag(["pool2", "tag1"])
        yield self.tagpool.acquire_specific_tag(["pool2", "tag2"])
        # pool3 is empty but has metadata
        yield self.tagpool.set_metadata("pool3", {"meta": "data"})

    @inlineCallbacks
    def test_acquire_tag(self):
        result = yield self.proxy.callRemote("acquire_tag", "pool1")
        self.assertEqual(result, ["pool1", "tag1"])
        self.assertEqual((yield self.tagpool.inuse_tags("pool1")),
                         [("pool1", "tag1")])
        result = yield self.proxy.callRemote("acquire_tag", "pool2")
        self.assertEqual(result, None)

    @inlineCallbacks
    def test_acquire_specific_tag(self):
        result = yield self.proxy.callRemote("acquire_specific_tag",
                                             ["pool1", "tag1"])
        self.assertEqual(result, ["pool1", "tag1"])
        self.assertEqual((yield self.tagpool.inuse_tags("pool1")),
                         [("pool1", "tag1")])
        result = yield self.proxy.callRemote("acquire_specific_tag",
                                             ["pool2", "tag1"])
        self.assertEqual(result, None)

    @inlineCallbacks
    def test_release_tag(self):
        result = yield self.proxy.callRemote("release_tag",
                                             ["pool1", "tag1"])
        self.assertEqual(result, None)
        result = yield self.proxy.callRemote("release_tag",
                                             ["pool2", "tag1"])
        self.assertEqual(result, None)
        self.assertEqual((yield self.tagpool.inuse_tags("pool2")),
                         [("pool2", "tag2")])

    @inlineCallbacks
    def test_declare_tags(self):
        tags = [("newpool", "tag1"), ("newpool", "tag2")]
        result = yield self.proxy.callRemote("declare_tags", tags)
        self.assertEqual(result, None)
        self.assertEqual((yield self.tagpool.free_tags("newpool")), tags)

    @inlineCallbacks
    def test_get_metadata(self):
        result = yield self.proxy.callRemote("get_metadata", "pool3")
        self.assertEqual(result, {"meta": "data"})
        result = yield self.proxy.callRemote("get_metadata", "pool1")
        self.assertEqual(result, {})

    @inlineCallbacks
    def test_set_metadata(self):
        result = yield self.proxy.callRemote("set_metadata", "newpool",
                                             {"my": "data"})
        self.assertEqual(result, None)
        self.assertEqual((yield self.tagpool.get_metadata("newpool")),
                         {"my": "data"})

    @inlineCallbacks
    def test_purge_pool(self):
        result = yield self.proxy.callRemote("purge_pool", "pool1")
        self.assertEqual(result, None)
        self.assertEqual((yield self.tagpool.free_tags("pool1")), [])

    @inlineCallbacks
    def test_list_pools(self):
        result = yield self.proxy.callRemote("list_pools")
        self.assertEqual(sorted(result), ["pool1", "pool2", "pool3"])

    @inlineCallbacks
    def test_free_tags(self):
        result = yield self.proxy.callRemote("free_tags", "pool1")
        self.assertEqual(result, [["pool1", "tag1"], ["pool1", "tag2"]])
        result = yield self.proxy.callRemote("free_tags", "pool2")
        self.assertEqual(result, [])
        result = yield self.proxy.callRemote("free_tags", "pool3")
        self.assertEqual(result, [])

    @inlineCallbacks
    def test_inuse_tags(self):
        result = yield self.proxy.callRemote("inuse_tags", "pool1")
        self.assertEqual(result, [])
        result = yield self.proxy.callRemote("inuse_tags", "pool2")
        self.assertEqual(result, [["pool2", "tag1"], ["pool2", "tag2"]])
        result = yield self.proxy.callRemote("inuse_tags", "pool3")
        self.assertEqual(result, [])


class TestTagpoolApiWorker(TestCase):
    pass


class TestTagpoolApiClient(TestCase):
    pass
