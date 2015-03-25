# -*- coding: utf-8 -*-

"""Tests for vumi.components.message_store_cache."""

from datetime import datetime, timedelta

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.tests.helpers import (
    VumiTestCase, MessageHelper, PersistenceHelper, import_skip,
)


class MessageStoreCacheTestCase(VumiTestCase):

    start_batch = True

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        try:
            from vumi.components.message_store import MessageStore
        except ImportError, e:
            import_skip(e, 'riak')
        self.redis = yield self.persistence_helper.get_redis_manager()
        self.manager = yield self.persistence_helper.get_riak_manager()
        self.store = yield MessageStore(self.manager, self.redis)
        self.cache = self.store.cache
        self.batch_id = 'a-batch-id'
        if self.start_batch:
            yield self.cache.batch_start(self.batch_id)
        self.msg_helper = self.add_helper(MessageHelper())

    @inlineCallbacks
    def add_messages(self, batch_id, callback, now=None, count=10):
        messages = []
        now = (datetime.now() if now is None else now)
        for i in range(count):
            msg = self.msg_helper.make_inbound(
                "inbound",
                from_addr='from-%s' % (i,),
                to_addr='to-%s' % (i,))
            msg['timestamp'] = now - timedelta(seconds=i)
            yield callback(batch_id, msg)
            messages.append(msg)
        returnValue(messages)

    @inlineCallbacks
    def add_event_pairs(self, batch_id, now=None, count=10):
        messages = []
        now = (datetime.now() if now is None else now)
        for i in range(count):
            msg = self.msg_helper.make_inbound(
                "inbound",
                from_addr='from-%s' % (i,),
                to_addr='to-%s' % (i,))
            msg['timestamp'] = now - timedelta(seconds=i)
            yield self.cache.add_outbound_message(batch_id, msg)
            ack = self.msg_helper.make_ack(msg)
            delivery = self.msg_helper.make_delivery_report(msg)
            yield self.cache.add_event(self.batch_id, ack)
            yield self.cache.add_event(self.batch_id, delivery)
            messages.extend((ack, delivery))
        returnValue(messages)


class TestMessageStoreCache(MessageStoreCacheTestCase):

    @inlineCallbacks
    def test_add_outbound_message(self):
        msg = self.msg_helper.make_outbound("outbound")
        yield self.cache.add_outbound_message(self.batch_id, msg)
        [msg_key] = yield self.cache.get_outbound_message_keys(self.batch_id)
        self.assertEqual(msg_key, msg['message_id'])

    @inlineCallbacks
    def test_get_outbound_message_keys(self):
        messages = yield self.add_messages(
            self.batch_id, self.cache.add_outbound_message)
        # make sure we get keys back ordered according to timestamp, which
        # means the reverse of how we put them in.
        keys = yield self.cache.get_outbound_message_keys(self.batch_id)
        self.assertEqual(len(keys), 10)
        self.assertEqual(keys, list([m['message_id'] for m in messages]))

    @inlineCallbacks
    def test_count_outbound_message_keys(self):
        yield self.add_messages(
            self.batch_id, self.cache.add_outbound_message)
        count = yield self.cache.count_outbound_message_keys(self.batch_id)
        self.assertEqual(count, 10)

    @inlineCallbacks
    def test_paged_get_outbound_message_keys(self):
        messages = yield self.add_messages(
            self.batch_id, self.cache.add_outbound_message)
        # make sure we get keys back ordered according to timestamp, which
        # means the reverse of how we put them in.
        keys = yield self.cache.get_outbound_message_keys(self.batch_id, 0, 4)
        self.assertEqual(len(keys), 5)
        self.assertEqual(keys, list([m['message_id'] for m in messages])[:5])

    @inlineCallbacks
    def test_get_batch_ids(self):
        yield self.cache.batch_start('batch-1')
        yield self.cache.add_outbound_message(
            'batch-1', self.msg_helper.make_outbound("outbound"))
        yield self.cache.batch_start('batch-2')
        yield self.cache.add_outbound_message(
            'batch-2', self.msg_helper.make_outbound("outbound"))
        self.assertEqual((yield self.cache.get_batch_ids()), set([
            self.batch_id, 'batch-1', 'batch-2']))

    @inlineCallbacks
    def test_add_inbound_message(self):
        msg = self.msg_helper.make_inbound("inbound")
        yield self.cache.add_inbound_message(self.batch_id, msg)
        [msg_key] = yield self.cache.get_inbound_message_keys(self.batch_id)
        self.assertEqual(msg_key, msg['message_id'])

    @inlineCallbacks
    def test_get_inbound_message_keys(self):
        messages = yield self.add_messages(
            self.batch_id, self.cache.add_inbound_message)
        keys = yield self.cache.get_inbound_message_keys(self.batch_id)
        self.assertEqual(len(keys), 10)
        self.assertEqual(keys, list([m['message_id'] for m in messages]))

    @inlineCallbacks
    def test_count_inbound_message_keys(self):
        yield self.add_messages(
            self.batch_id, self.cache.add_inbound_message)
        count = yield self.cache.count_inbound_message_keys(self.batch_id)
        self.assertEqual(count, 10)

    @inlineCallbacks
    def test_paged_get_inbound_message_keys(self):
        messages = yield self.add_messages(
            self.batch_id, self.cache.add_inbound_message)
        keys = yield self.cache.get_inbound_message_keys(self.batch_id, 0, 4)
        self.assertEqual(len(keys), 5)
        self.assertEqual(keys, list([m['message_id'] for m in messages])[:5])

    @inlineCallbacks
    def test_get_from_addrs(self):
        yield self.add_messages(
            self.batch_id, self.cache.add_inbound_message)
        from_addrs = yield self.cache.get_from_addrs(self.batch_id)
        # NOTE: This functionality is disabled for now.
        # self.assertEqual(from_addrs, ['from-%s' % i for i in range(10)])
        self.assertEqual(from_addrs, [])

    @inlineCallbacks
    def test_count_from_addrs(self):
        yield self.add_messages(
            self.batch_id, self.cache.add_inbound_message)
        count = yield self.cache.count_from_addrs(self.batch_id)
        self.assertEqual(count, 10)

    @inlineCallbacks
    def test_get_to_addrs(self):
        yield self.add_messages(
            self.batch_id, self.cache.add_outbound_message)
        to_addrs = yield self.cache.get_to_addrs(self.batch_id)
        # NOTE: This functionality is disabled for now.
        # self.assertEqual(to_addrs, ['to-%s' % i for i in range(10)])
        self.assertEqual(to_addrs, [])

    @inlineCallbacks
    def test_count_to_addrs(self):
        yield self.add_messages(
            self.batch_id, self.cache.add_outbound_message)
        count = yield self.cache.count_to_addrs(self.batch_id)
        self.assertEqual(count, 10)

    @inlineCallbacks
    def test_add_event(self):
        msg = self.msg_helper.make_outbound("outbound")
        self.cache.add_outbound_message(self.batch_id, msg)
        ack = self.msg_helper.make_ack(msg)
        delivery = self.msg_helper.make_delivery_report(msg)
        yield self.cache.add_event(self.batch_id, ack)
        yield self.cache.add_event(self.batch_id, delivery)
        event_count = yield self.cache.count_event_keys(self.batch_id)
        self.assertEqual(event_count, 2)
        status = yield self.cache.get_event_status(self.batch_id)
        self.assertEqual(status, {
            'delivery_report': 1,
            'delivery_report.delivered': 1,
            'delivery_report.failed': 0,
            'delivery_report.pending': 0,
            'ack': 1,
            'nack': 0,
            'sent': 1,
        })

    @inlineCallbacks
    def test_add_event_idempotence(self):
        msg = self.msg_helper.make_outbound("outbound")
        self.cache.add_outbound_message(self.batch_id, msg)
        acks = [self.msg_helper.make_ack(msg) for i in range(10)]
        for ack in acks:
            # send exact same event multiple times
            ack['event_id'] = 'identical'
            yield self.cache.add_event(self.batch_id, ack)
        event_count = yield self.cache.count_event_keys(self.batch_id)
        self.assertEqual(event_count, 1)
        status = yield self.cache.get_event_status(self.batch_id)
        self.assertEqual(status, {
            'delivery_report': 0,
            'delivery_report.delivered': 0,
            'delivery_report.failed': 0,
            'delivery_report.pending': 0,
            'ack': 1,
            'nack': 0,
            'sent': 1,
        })

    @inlineCallbacks
    def test_add_outbound_message_idempotence(self):
        for i in range(10):
            msg = self.msg_helper.make_outbound("outbound")
            msg['message_id'] = 'the-same-thing'
            yield self.cache.add_outbound_message(self.batch_id, msg)
        outbound_count = yield self.cache.count_outbound_message_keys(
            self.batch_id)
        self.assertEqual(outbound_count, 1)
        status = yield self.cache.get_event_status(self.batch_id)
        self.assertEqual(status['sent'], 1)
        self.assertEqual(
            (yield self.cache.get_outbound_message_keys(self.batch_id)),
            ['the-same-thing'])

    @inlineCallbacks
    def test_add_inbound_message_idempotence(self):
        for i in range(10):
            msg = self.msg_helper.make_inbound("inbound")
            msg['message_id'] = 'the-same-thing'
            yield self.cache.add_inbound_message(self.batch_id, msg)
        inbound_count = yield self.cache.count_inbound_message_keys(
            self.batch_id)
        self.assertEqual(inbound_count, 1)
        self.assertEqual(
            (yield self.cache.get_inbound_message_keys(self.batch_id)),
            ['the-same-thing'])

    @inlineCallbacks
    def test_clear_batch(self):
        msg_in = self.msg_helper.make_inbound("inbound")
        msg_out = self.msg_helper.make_outbound("outbound")
        ack = self.msg_helper.make_ack(msg_out)
        dr = self.msg_helper.make_delivery_report(
            msg_out, delivery_status='delivered')
        yield self.cache.add_inbound_message(self.batch_id, msg_in)
        yield self.cache.add_outbound_message(self.batch_id, msg_out)
        yield self.cache.add_event(self.batch_id, ack)
        yield self.cache.add_event(self.batch_id, dr)

        self.assertEqual(
            (yield self.cache.get_event_status(self.batch_id)),
            {
                'ack': 1,
                'delivery_report': 1,
                'delivery_report.delivered': 1,
                'delivery_report.failed': 0,
                'delivery_report.pending': 0,
                'nack': 0,
                'sent': 1,
            })
        yield self.cache.clear_batch(self.batch_id)
        yield self.cache.batch_start(self.batch_id)
        self.assertEqual(
            (yield self.cache.get_event_status(self.batch_id)),
            {
                'ack': 0,
                'delivery_report': 0,
                'delivery_report.delivered': 0,
                'delivery_report.failed': 0,
                'delivery_report.pending': 0,
                'nack': 0,
                'sent': 0,
            })
        self.assertEqual(
            (yield self.cache.count_from_addrs(self.batch_id)), 0)
        self.assertEqual(
            (yield self.cache.count_to_addrs(self.batch_id)), 0)
        self.assertEqual(
            (yield self.cache.count_inbound_message_keys(self.batch_id)), 0)
        self.assertEqual(
            (yield self.cache.count_outbound_message_keys(self.batch_id)), 0)

    @inlineCallbacks
    def test_count_inbound_throughput(self):
        # test for empty batches.
        self.assertEqual(
            (yield self.cache.count_inbound_throughput(self.batch_id)), 0)

        now = datetime.now()
        for i in range(10):
            msg_in = self.msg_helper.make_inbound("inbound")
            msg_in['timestamp'] = now - timedelta(seconds=i * 10)
            yield self.cache.add_inbound_message(self.batch_id, msg_in)

        self.assertEqual(
            (yield self.cache.count_inbound_throughput(self.batch_id)), 10)
        self.assertEqual(
            (yield self.cache.count_inbound_throughput(
                self.batch_id, sample_time=1)), 1)
        self.assertEqual(
            (yield self.cache.count_inbound_throughput(
                self.batch_id, sample_time=10)), 2)

    @inlineCallbacks
    def test_count_outbound_throughput(self):
        # test for empty batches.
        self.assertEqual(
            (yield self.cache.count_outbound_throughput(self.batch_id)), 0)

        now = datetime.now()
        for i in range(10):
            msg_out = self.msg_helper.make_outbound("outbound")
            msg_out['timestamp'] = now - timedelta(seconds=i * 10)
            yield self.cache.add_outbound_message(self.batch_id, msg_out)

        self.assertEqual(
            (yield self.cache.count_outbound_throughput(self.batch_id)), 10)
        self.assertEqual(
            (yield self.cache.count_outbound_throughput(
                self.batch_id, sample_time=1)), 1)
        self.assertEqual(
            (yield self.cache.count_outbound_throughput(
                self.batch_id, sample_time=10)), 2)

    def test_get_query_token(self):
        cache = self.store.cache
        # different ordering in the dict should result in the same token.
        token1 = cache.get_query_token('inbound', [{'a': 'b', 'c': 'd'}])
        token2 = cache.get_query_token('inbound', [{'c': 'd', 'a': 'b'}])
        self.assertEqual(token1, token2)

    @inlineCallbacks
    def test_start_query(self):
        token = yield self.cache.start_query(self.batch_id, 'inbound', [
            {'key': 'key', 'pattern': 'pattern', 'flags': 'flags'}])
        self.assertTrue(
            (yield self.cache.is_query_in_progress(self.batch_id, token)))

    @inlineCallbacks
    def test_store_query_results(self):
        now = datetime.now()
        message_ids = []
        # NOTE: we're writing these messages oldest first so we can check
        #       that the cache is returning them in the correct order
        #       when we pull out the search results.
        for i in range(10):
            msg_in = self.msg_helper.make_inbound('hello-%s' % (i,))
            msg_in['timestamp'] = now + timedelta(seconds=i * 10)
            yield self.cache.add_inbound_message(self.batch_id, msg_in)
            message_ids.append(msg_in['message_id'])

        token = yield self.cache.start_query(self.batch_id, 'inbound', [
            {'key': 'msg.content', 'pattern': 'hello', 'flags': ''}])
        yield self.cache.store_query_results(
            self.batch_id, token, message_ids, 'inbound', 120)
        self.assertFalse(
            (yield self.cache.is_query_in_progress(self.batch_id, token)))
        self.assertEqual(
            (yield self.cache.get_query_results(self.batch_id, token)),
            list(reversed(message_ids)))
        self.assertEqual(
            (yield self.cache.count_query_results(self.batch_id, token)),
            10)


class TestMessageStoreCacheWithCounters(MessageStoreCacheTestCase):

    start_batch = False

    @inlineCallbacks
    def test_switching_to_counters_outbound(self):
        self.cache.TRUNCATE_MESSAGE_KEY_COUNT_AT = 7

        for i in range(10):
            msg = self.msg_helper.make_outbound("outbound")
            yield self.cache.add_outbound_message(self.batch_id, msg)

        self.assertFalse((yield self.cache.uses_counters(self.batch_id)))
        self.assertEqual(
            (yield self.cache.count_outbound_message_keys(self.batch_id)),
            10)
        yield self.cache.switch_to_counters(self.batch_id)
        self.assertTrue((yield self.cache.uses_counters(self.batch_id)))
        self.assertEqual(
            (yield self.cache.count_outbound_message_keys(self.batch_id)),
            10)

        outbound = yield self.cache.get_outbound_message_keys(self.batch_id)
        self.assertEqual(len(outbound), 7)

    @inlineCallbacks
    def test_switching_to_counters_inbound(self):
        self.cache.TRUNCATE_MESSAGE_KEY_COUNT_AT = 7

        for i in range(10):
            msg = self.msg_helper.make_inbound("inbound")
            yield self.cache.add_inbound_message(self.batch_id, msg)

        self.assertFalse((yield self.cache.uses_counters(self.batch_id)))
        self.assertEqual(
            (yield self.cache.count_inbound_message_keys(self.batch_id)),
            10)
        yield self.cache.switch_to_counters(self.batch_id)
        self.assertTrue((yield self.cache.uses_counters(self.batch_id)))
        self.assertEqual(
            (yield self.cache.count_inbound_message_keys(self.batch_id)),
            10)

        inbound = yield self.cache.get_inbound_message_keys(self.batch_id)
        self.assertEqual(len(inbound), 7)

    @inlineCallbacks
    def test_inbound_truncate_at_within_limits(self):
        yield self.add_messages(
            self.batch_id, self.cache.add_inbound_message, count=10)
        count = yield self.cache.truncate_inbound_message_keys(
            self.batch_id, truncate_at=11)
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_inbound_truncate_at_over_limits(self):
        yield self.add_messages(
            self.batch_id, self.cache.add_inbound_message, count=10)
        count = yield self.cache.truncate_inbound_message_keys(
            self.batch_id, truncate_at=7)
        self.assertEqual(count, 3)
        self.assertEqual((yield self.cache.count_inbound_message_keys(
            self.batch_id)), 7)

    @inlineCallbacks
    def test_outbound_truncate_at_within_limits(self):
        yield self.add_messages(
            self.batch_id, self.cache.add_outbound_message, count=10)
        count = yield self.cache.truncate_outbound_message_keys(
            self.batch_id, truncate_at=11)
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_outbound_truncate_at_over_limits(self):
        yield self.add_messages(
            self.batch_id, self.cache.add_outbound_message, count=10)
        count = yield self.cache.truncate_outbound_message_keys(
            self.batch_id, truncate_at=7)
        self.assertEqual(count, 3)
        self.assertEqual((yield self.cache.count_outbound_message_keys(
            self.batch_id)), 7)

    @inlineCallbacks
    def test_event_truncate_at_within_limits(self):
        # We need to use counters here, but have a default truncation limit
        # higher than what we're testing.
        self.cache.TRUNCATE_MESSAGE_KEY_COUNT_AT = 100
        yield self.cache.batch_start(self.batch_id, use_counters=True)

        yield self.add_event_pairs(self.batch_id, count=5)
        removed_count = yield self.cache.truncate_event_keys(
            self.batch_id, truncate_at=11)
        self.assertEqual(removed_count, 0)
        key_count = yield self.cache.redis.zcard(
            self.cache.event_key(self.batch_id))
        self.assertEqual(key_count, 10)

    @inlineCallbacks
    def test_event_truncate_at_over_limits(self):
        # We need to use counters here, but have a default truncation limit
        # higher than what we're testing.
        self.cache.TRUNCATE_MESSAGE_KEY_COUNT_AT = 100
        yield self.cache.batch_start(self.batch_id, use_counters=True)

        yield self.add_event_pairs(self.batch_id, count=5)
        removed_count = yield self.cache.truncate_event_keys(
            self.batch_id, truncate_at=7)
        self.assertEqual(removed_count, 3)
        key_count = yield self.cache.redis.zcard(
            self.cache.event_key(self.batch_id))
        self.assertEqual(key_count, 7)

    @inlineCallbacks
    def test_truncation_after_hitting_limit(self):
        truncate_at = 10
        # Check we're actually truncating in the messages
        self.assertTrue(self.cache.uses_counters(self.batch_id))

        start = datetime.now()
        received_messages = []
        for i in range(20):
            now = start + timedelta(seconds=i)
            # populate in ascending timestamp
            [msg] = yield self.add_messages(
                self.batch_id, self.cache.add_inbound_message,
                now=now, count=1)
            received_messages.append(msg)
            # Manually truncate
            yield self.cache.truncate_inbound_message_keys(
                self.batch_id, truncate_at=truncate_at)

        # Get latest 20 messages from cache (there should be 10)
        cached_message_keys = yield self.cache.get_inbound_message_keys(
            self.batch_id, 0, 19)
        # Make sure we're not storing more than we expect to be
        self.assertEqual(len(cached_message_keys), truncate_at)
        # Make sure we're storing the most recent ones
        self.assertEqual(
            set(cached_message_keys),
            set([m['message_id'] for m in received_messages[-truncate_at:]]))
