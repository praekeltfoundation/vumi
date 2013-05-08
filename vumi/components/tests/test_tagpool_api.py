# -*- coding: utf-8 -*-

"""Tests for vumi.components.tagpool_api."""

from txjsonrpc.web.jsonrpc import Proxy
from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.web.server import Site
from twisted.python import log

from vumi.components.tagpool_api import TagpoolApiServer, TagpoolApiWorker
from vumi.components.tagpool import TagpoolManager
from vumi.tests.utils import VumiWorkerTestCase, PersistenceMixin


class TestTagpoolApiServer(TestCase, PersistenceMixin):

    @inlineCallbacks
    def setUp(self):
        self._persist_setUp()
        self.redis = yield self.get_redis_manager()
        self.tagpool = TagpoolManager(self.redis)
        site = Site(TagpoolApiServer(self.tagpool))
        self.server = yield reactor.listenTCP(0, site)
        addr = self.server.getHost()
        self.proxy = Proxy("http://%s:%d/" % (addr.host, addr.port))
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

    def _check_reason(self, result, expected_owner, expected_reason):
        owner, reason = result
        self.assertEqual(owner, expected_owner)
        self.assertEqual(reason.pop('owner'), expected_owner)
        self.assertTrue(isinstance(reason.pop('timestamp'), float))
        self.assertEqual(reason, expected_reason)

    @inlineCallbacks
    def test_acquire_tag(self):
        result = yield self.proxy.callRemote("acquire_tag", "pool1")
        self.assertEqual(result, ["pool1", "tag1"])
        self.assertEqual((yield self.tagpool.inuse_tags("pool1")),
                         [("pool1", "tag1")])
        result = yield self.proxy.callRemote("acquire_tag", "pool2")
        self.assertEqual(result, None)

    @inlineCallbacks
    def test_acquire_tag_with_owner_and_reason(self):
        result = yield self.proxy.callRemote(
            "acquire_tag", "pool1", "me", {"foo": "bar"})
        self.assertEqual(result, ["pool1", "tag1"])
        result = yield self.tagpool.acquired_by(["pool1", "tag1"])
        self._check_reason(result, "me", {"foo": "bar"})

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
    def test_acquire_specific_tag_with_owner_and_reason(self):
        result = yield self.proxy.callRemote(
            "acquire_specific_tag", ["pool1", "tag1"], "me", {"foo": "bar"})
        self.assertEqual(result, ["pool1", "tag1"])
        result = yield self.tagpool.acquired_by(["pool1", "tag1"])
        self._check_reason(result, "me", {"foo": "bar"})

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
    def test_purge_pool_with_keys_in_use(self):
        d = self.proxy.callRemote("purge_pool", "pool2")
        yield d.addErrback(lambda f: log.err(f))
        errors = self.flushLoggedErrors('txjsonrpc.jsonrpclib.Fault')
        self.assertEqual(len(errors), 1)
        server_errors = self.flushLoggedErrors(
            'vumi.components.tagpool.TagpoolError')
        self.assertEqual(len(server_errors), 1)

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

    @inlineCallbacks
    def test_acquired_by(self):
        result = yield self.proxy.callRemote("acquired_by", ["pool1", "tag1"])
        self.assertEqual(result, [None, None])
        result = yield self.proxy.callRemote("acquired_by", ["pool2", "tag1"])
        self._check_reason(result, None, {})
        yield self.tagpool.acquire_tag("pool1", owner="me",
                                       reason={"foo": "bar"})
        result = yield self.proxy.callRemote("acquired_by", ["pool1", "tag1"])
        self._check_reason(result, "me", {"foo": "bar"})

    @inlineCallbacks
    def test_owned_tags(self):
        result = yield self.proxy.callRemote("owned_tags", None)
        self.assertEqual(sorted(result),
                         [[u'pool2', u'tag1'], [u'pool2', u'tag2']])
        yield self.tagpool.acquire_tag("pool1", owner="me",
                                       reason={"foo": "bar"})
        result = yield self.proxy.callRemote("owned_tags", "me")
        self.assertEqual(result, [["pool1", "tag1"]])


class TestTagpoolApiWorker(VumiWorkerTestCase, PersistenceMixin):

    def setUp(self):
        self._persist_setUp()
        super(TestTagpoolApiWorker, self).setUp()

    @inlineCallbacks
    def tearDown(self):
        for worker in self._workers:
            if worker.running:
                yield worker.stopService()
        yield super(TestTagpoolApiWorker, self).tearDown()

    @inlineCallbacks
    def get_api_worker(self, config=None, start=True):
        config = {} if config is None else config
        config.setdefault('worker_name', 'test_api_worker')
        config.setdefault('twisted_endpoint', 'tcp:0')
        config = self.mk_config(config)
        worker = yield self.get_worker(config, TagpoolApiWorker, start)
        if not start:
            returnValue(worker)
        yield worker.startService()
        port = worker.services[0]._waitingForPort.result
        addr = port.getHost()
        proxy = Proxy("http://%s:%d" % (addr.host, addr.port))
        returnValue((worker, proxy))

    @inlineCallbacks
    def test_list_methods(self):
        worker, proxy = yield self.get_api_worker()
        result = yield proxy.callRemote('system.listMethods')
        self.assertTrue(u'acquire_tag' in result)

    @inlineCallbacks
    def test_method_help(self):
        worker, proxy = yield self.get_api_worker()
        result = yield proxy.callRemote('system.methodHelp', 'acquire_tag')
        self.assertEqual(result, u"\n".join([
            "Acquire a tag from the pool (returns None if"
            " no tags are avaliable).",
            "",
            ":param Unicode pool:",
            "    Name of pool to acquire tag from.",
            ":param Unicode owner:",
            "    Owner acquiring tag (or None). May be null. Default: None.",
            ":param Dict reason:",
            "    Metadata on why tag is being acquired (or None)."
            " May be null.",
            "    Default: None.",
            ":rtype Tag:",
            "    Tag acquired (or None).",
        ]))

    @inlineCallbacks
    def test_method_signature(self):
        worker, proxy = yield self.get_api_worker()
        result = yield proxy.callRemote('system.methodSignature',
                                        'acquire_tag')
        self.assertEqual(result, [[u'array', u'string', u'string', u'struct']])
