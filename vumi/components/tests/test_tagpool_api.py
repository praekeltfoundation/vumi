# -*- coding: utf-8 -*-

"""Tests for vumi.components.tagpool_api."""

from txjsonrpc.web.jsonrpc import Proxy
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.web.server import Site
from twisted.python import log

from vumi.components.tagpool_api import TagpoolApiServer, TagpoolApiWorker
from vumi.components.tagpool import TagpoolManager
from vumi.utils import http_request
from vumi.tests.helpers import VumiTestCase, WorkerHelper, PersistenceHelper


class TestTagpoolApiServer(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.redis = yield self.persistence_helper.get_redis_manager()
        self.tagpool = TagpoolManager(self.redis)
        site = Site(TagpoolApiServer(self.tagpool))
        self.server = yield reactor.listenTCP(0, site, interface='127.0.0.1')
        self.add_cleanup(self.server.loseConnection)
        addr = self.server.getHost()
        self.proxy = Proxy("http://%s:%d/" % (addr.host, addr.port))
        yield self.setup_tags()

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
        free_tags = yield self.tagpool.free_tags("newpool")
        self.assertEqual(sorted(free_tags), sorted(tags))

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
        self.assertEqual(
            sorted(result), [["pool1", "tag1"], ["pool1", "tag2"]])
        result = yield self.proxy.callRemote("free_tags", "pool2")
        self.assertEqual(result, [])
        result = yield self.proxy.callRemote("free_tags", "pool3")
        self.assertEqual(result, [])

    @inlineCallbacks
    def test_inuse_tags(self):
        result = yield self.proxy.callRemote("inuse_tags", "pool1")
        self.assertEqual(result, [])
        result = yield self.proxy.callRemote("inuse_tags", "pool2")
        self.assertEqual(
            sorted(result), [["pool2", "tag1"], ["pool2", "tag2"]])
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


class TestTagpoolApiWorker(VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.worker_helper = self.add_helper(WorkerHelper())

    @inlineCallbacks
    def cleanup_worker(self, worker):
        if worker.running:
            yield worker.redis_manager._purge_all()
            yield worker.redis_manager.close_manager()
            yield worker.stopService()

    @inlineCallbacks
    def get_api_worker(self, config=None, start=True):
        config = {} if config is None else config
        config.setdefault('worker_name', 'test_api_worker')
        config.setdefault('twisted_endpoint', 'tcp:0')
        config.setdefault('web_path', 'api')
        config.setdefault('health_path', 'health')
        config = self.persistence_helper.mk_config(config)
        worker = yield self.worker_helper.get_worker(
            TagpoolApiWorker, config, start)
        self.add_cleanup(self.cleanup_worker, worker)
        if not start:
            returnValue(worker)
        yield worker.startService()
        port = worker.services[0]._waitingForPort.result
        addr = port.getHost()
        proxy = Proxy("http://%s:%d/api/" % (addr.host, addr.port))
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

    @inlineCallbacks
    def test_health_resource(self):
        worker, proxy = yield self.get_api_worker()
        result = yield http_request(
            "http://%s:%s/health" % (proxy.host, proxy.port),
            data=None, method='GET')
        self.assertEqual(result, "OK")
