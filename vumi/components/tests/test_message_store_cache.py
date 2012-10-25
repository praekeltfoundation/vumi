# -*- coding: utf-8 -*-

"""Tests for vumi.components.message_store_cache."""

from datetime import datetime, timedelta

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import TransportMessage
from vumi.application.tests.test_base import ApplicationTestCase
from vumi.components import MessageStore


class TestMessageStoreCache(ApplicationTestCase):
    use_riak = True

    @inlineCallbacks
    def setUp(self):
        yield super(TestMessageStoreCache, self).setUp()
        self.redis = yield self.get_redis_manager()
        self.manager = yield self.get_riak_manager()
        self.store = yield MessageStore(self.manager, self.redis)
        self.cache = self.store.cache
        self.batch_id = 'a-batch-id'
        self.cache.batch_start(self.batch_id)

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
            msg = self.mkmsg_in(from_addr='from-%s' % (i,),
                to_addr='to-%s' % (i,))
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
    def test_count_outbound_message_keys(self):
        yield self.add_messages(self.batch_id,
            self.cache.add_outbound_message)
        count = yield self.cache.count_outbound_message_keys(self.batch_id)
        self.assertEqual(count, 10)

    @inlineCallbacks
    def test_paged_get_outbound_message_keys(self):
        messages = yield self.add_messages(self.batch_id,
            self.cache.add_outbound_message)
        # make sure we get keys back ordered according to timestamp, which
        # means the reverse of how we put them in.
        keys = yield self.cache.get_outbound_message_keys(self.batch_id, 0, 4)
        self.assertEqual(len(keys), 5)
        self.assertEqual(keys, list(reversed(
            [m['message_id'] for m in messages]))[0:5])

    @inlineCallbacks
    def test_get_batch_ids(self):
        yield self.cache.batch_start('batch-1')
        yield self.cache.add_outbound_message('batch-1', self.mkmsg_out())
        yield self.cache.batch_start('batch-2')
        yield self.cache.add_outbound_message('batch-2', self.mkmsg_out())
        self.assertEqual((yield self.cache.get_batch_ids()), set([
            self.batch_id, 'batch-1', 'batch-2']))

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
    def test_count_inbound_message_keys(self):
        yield self.add_messages(self.batch_id,
            self.cache.add_inbound_message)
        count = yield self.cache.count_inbound_message_keys(self.batch_id)
        self.assertEqual(count, 10)

    @inlineCallbacks
    def test_paged_get_inbound_message_keys(self):
        messages = yield self.add_messages(self.batch_id,
            self.cache.add_inbound_message)
        keys = yield self.cache.get_inbound_message_keys(self.batch_id, 0, 4)
        self.assertEqual(len(keys), 5)
        self.assertEqual(keys, list(reversed(
            [m['message_id'] for m in messages]))[0:5])

    @inlineCallbacks
    def test_get_from_addrs(self):
        yield self.add_messages(self.batch_id,
            self.cache.add_inbound_message)
        from_addrs = yield self.cache.get_from_addrs(self.batch_id)
        self.assertEqual(from_addrs, ['from-%s' % i for i in
                                        reversed(range(10))])

    @inlineCallbacks
    def test_count_from_addrs(self):
        yield self.add_messages(self.batch_id,
            self.cache.add_inbound_message)
        count = yield self.cache.count_from_addrs(self.batch_id)
        self.assertEqual(count, 10)

    @inlineCallbacks
    def test_get_to_addrs(self):
        yield self.add_messages(self.batch_id,
            self.cache.add_outbound_message)
        to_addrs = yield self.cache.get_to_addrs(self.batch_id)
        self.assertEqual(to_addrs, ['to-%s' % i for i in
                                        reversed(range(10))])

    @inlineCallbacks
    def test_count_to_addrs(self):
        yield self.add_messages(self.batch_id,
            self.cache.add_outbound_message)
        count = yield self.cache.count_to_addrs(self.batch_id)
        self.assertEqual(count, 10)

    @inlineCallbacks
    def test_add_event(self):
        msg = self.mkmsg_out()
        self.cache.add_outbound_message(self.batch_id, msg)
        ack = self.mkmsg_ack(user_message_id=msg['message_id'],
            sent_message_id=TransportMessage.generate_id())
        delivery = self.mkmsg_delivery(user_message_id=msg['message_id'])
        yield self.cache.add_event(self.batch_id, ack)
        yield self.cache.add_event(self.batch_id, delivery)
        stats = yield self.cache.get_event_status(self.batch_id)
        self.assertEqual(stats, {
            'delivery_report': '1',
            'delivery_report.delivered': '1',
            'delivery_report.failed': '0',
            'delivery_report.pending': '0',
            'ack': '1',
            'sent': '1',
            })

    @inlineCallbacks
    def test_calculate_offsets(self):
        msg1 = self.mkmsg_out()
        msg2 = self.mkmsg_out()
        yield self.store.add_outbound_message(msg1, batch_id=self.batch_id)
        # the cache has one more than the message store
        yield self.cache.add_outbound_message(self.batch_id, msg2)
        offsets = yield self.cache.calculate_offsets(self.store, self.batch_id)
        # inbounds should be the same, outbounds the message store should
        # be one behind the cache (which is very unlikely)
        self.assertEqual(offsets, (0, -1))
