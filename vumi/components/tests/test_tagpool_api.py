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

    @inlineCallbacks
    def tearDown(self):
        yield self.server.loseConnection()
        yield self._persist_tearDown()

    @inlineCallbacks
    def test_acquire_tag(self):
        result = yield self.proxy.callRemote("acquire_tag", "foo")
        self.assertEqual(result, None)


class TestTagpoolApiWorker(TestCase):
    pass


class TestTagpoolApiClient(TestCase):
    pass
