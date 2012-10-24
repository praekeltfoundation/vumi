# -*- coding: utf-8 -*-

"""Tests for vumi.components.message_store_cache."""

from datetime import datetime, timedelta

from twisted.internet.defer import inlineCallbacks, returnValue

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

    def mkmsg_out(self, **kwargs):
        defaults = {
            'message_id': TransportMessage.generate_id(),
        }
        defaults.update(kwargs)
        return super(TestMessageStoreCache, self).mkmsg_out(**defaults)

    def mkmsg_in(self, **kwargs):
        defaults = {
            'message_id': TransportMessage.generate_id(),
        }
        defaults.update(kwargs)
        return super(TestMessageStoreCache, self).mkmsg_in(**defaults)

    @inlineCallbacks
    def add_messages(self, batch_id, callback, count=10):
        messages = []
        for i in range(count):
            msg = self.mkmsg_in(from_addr='from-%s' % (i,))
            msg['timestamp'] = datetime.now() - timedelta(seconds=i)
            yield callback(batch_id, msg)
            messages.append(msg)
        returnValue(messages)

    @inlineCallbacks
    def test_add_outbound_message(self):
        msg = self.mkmsg_out()
        yield self.cache.add_outbound_message(self.batch_id, msg)
        [msg_key] = yield self.cache.get_outbound_message_keys(self.batch_id)
        self.assertEqual(msg_key, msg['message_id'])

    @inlineCallbacks
    def test_get_outbound_message_keys(self):
        messages = yield self.add_messages(self.batch_id,
            self.cache.add_outbound_message)
        # make sure we get keys back ordered according to timestamp, which
        # means the reverse of how we put them in.
        keys = yield self.cache.get_outbound_message_keys(self.batch_id)
        self.assertEqual(len(keys), 10)
        self.assertEqual(keys, list(reversed(
            [m['message_id'] for m in messages])))

    @inlineCallbacks
    def test_get_batch_ids(self):
        yield self.cache.add_outbound_message('batch-1', self.mkmsg_out())
        yield self.cache.add_outbound_message('batch-2', self.mkmsg_out())
        self.assertEqual((yield self.cache.get_batch_ids()), set([
            'batch-1', 'batch-2']))

    @inlineCallbacks
    def test_add_inbound_message(self):
        msg = self.mkmsg_in()
        yield self.cache.add_inbound_message(self.batch_id, msg)
        [msg_key] = yield self.cache.get_inbound_message_keys(self.batch_id)
        self.assertEqual(msg_key, msg['message_id'])

    @inlineCallbacks
    def test_get_inbound_message_keys(self):
        messages = yield self.add_messages(self.batch_id,
            self.cache.add_inbound_message)
        keys = yield self.cache.get_inbound_message_keys(self.batch_id)
        self.assertEqual(len(keys), 10)
        self.assertEqual(keys, list(reversed(
            [m['message_id'] for m in messages])))

    @inlineCallbacks
    def test_get_from_addrs(self):
        yield self.add_messages(self.batch_id,
            self.cache.add_inbound_message)
        from_addrs = yield self.cache.get_from_addrs(self.batch_id)
        self.assertEqual(from_addrs, ['from-%s' % i for i in
                                        reversed(range(10))])
