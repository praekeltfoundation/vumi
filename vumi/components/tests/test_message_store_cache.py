# -*- coding: utf-8 -*-

"""Tests for vumi.components.message_store_cache."""

from datetime import datetime, timedelta

from twisted.internet.defer import inlineCallbacks

from vumi.message import TransportMessage
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
        self.cache = MessageStoreCache(self.redis)
        self.batch_id = 'a-batch-id'

    @inlineCallbacks
    def test_add_outbound_message(self):
        msg = self.mkmsg_out(to_addr='to@addr.com',
            message_id=TransportMessage.generate_id())
        self.cache.add_outbound_message(self.batch_id, msg)
        [msg_key] = yield self.cache.get_outbound_message_keys(self.batch_id)
        self.assertEqual(msg_key, msg['message_id'])

    @inlineCallbacks
    def test_get_outbound_message_keys(self):
        messages = []
        for i in range(10):
            msg = self.mkmsg_out(to_addr='to@addr.com',
                message_id=TransportMessage.generate_id())
            # insert them with starting with newest first
            msg['timestamp'] = datetime.now() - timedelta(seconds=i)
            self.cache.add_outbound_message(self.batch_id, msg)
            messages.append(msg)

        # make sure we get keys back ordered according to timestamp, which
        # means the reverse of how we put them in.
        keys = yield self.cache.get_outbound_message_keys(self.batch_id)
        self.assertEqual(len(keys), 10)
        self.assertEqual(keys, list(reversed(
            [m['message_id'] for m in messages])))
