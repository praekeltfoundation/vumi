# -*- coding: utf-8 -*-

"""Tests for vumi.components.message_store_cache."""

from twisted.internet.defer import inlineCallbacks

from vumi.application.tests.test_base import ApplicationTestCase
from vumi.components import MessageStore
from vumi.components.message_store_cache import MessageStoreCache


class TestMessageStoreCache(ApplicationTestCase):

    @inlineCallbacks
    def setUp(self):
        yield super(TestMessageStoreCache, self).setUp()
        self.redis = yield self.get_redis_manager()
        self.manager = yield self.get_riak_manager()
        self.store = yield MessageStore(self.manager, self.redis)
        self.store_cache = MessageStoreCache(self.redis)

    def test_something(self):
        self.assertTrue(True)
