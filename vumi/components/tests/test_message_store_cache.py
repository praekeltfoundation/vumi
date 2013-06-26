# -*- coding: utf-8 -*-

"""Tests for vumi.components.message_store_cache."""

from datetime import datetime, timedelta

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import TransportMessage
from vumi.application.tests.test_base import ApplicationTestCase
from vumi.components.message_store import MessageStore


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
        now = datetime.now()
        for i in range(count):
            msg = self.mkmsg_in(from_addr='from-%s' % (i,),
                to_addr='to-%s' % (i,))
            msg['timestamp'] = now - timedelta(seconds=i)
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
        self.assertEqual(keys, list([m['message_id'] for m in messages]))

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
        self.assertEqual(keys, list([m['message_id'] for m in messages])[:5])

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
        self.assertEqual(keys, list([m['message_id'] for m in messages]))

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
        self.assertEqual(keys, list([m['message_id'] for m in messages])[:5])

    @inlineCallbacks
    def test_get_from_addrs(self):
        yield self.add_messages(self.batch_id,
            self.cache.add_inbound_message)
        from_addrs = yield self.cache.get_from_addrs(self.batch_id)
        self.assertEqual(from_addrs, ['from-%s' % i for i in range(10)])

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
        self.assertEqual(to_addrs, ['to-%s' % i for i in range(10)])

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
        msg = self.mkmsg_out()
        self.cache.add_outbound_message(self.batch_id, msg)
        acks = [self.mkmsg_ack(user_message_id=msg['message_id'],
                                sent_message_id=msg['message_id'])
                                for i in range(10)]
        for ack in acks:
            # send exact same event multiple times
            ack['event_id'] = 'identical'
            yield self.cache.add_event(self.batch_id, ack)
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
            msg = self.mkmsg_out()
            msg['message_id'] = 'the-same-thing'
            yield self.cache.add_outbound_message(self.batch_id, msg)
        status = yield self.cache.get_event_status(self.batch_id)
        self.assertEqual(status['sent'], 1)
        self.assertEqual(
            (yield self.cache.get_outbound_message_keys(self.batch_id)),
            ['the-same-thing'])

    @inlineCallbacks
    def test_add_inbound_message_idempotence(self):
        for i in range(10):
            msg = self.mkmsg_in()
            msg['message_id'] = 'the-same-thing'
            self.cache.add_inbound_message(self.batch_id, msg)
        self.assertEqual(
            (yield self.cache.get_inbound_message_keys(self.batch_id)),
            ['the-same-thing'])

    @inlineCallbacks
    def test_clear_batch(self):
        msg_in = self.mkmsg_in()
        msg_out = self.mkmsg_out()
        ack = self.mkmsg_ack(user_message_id=msg_out['message_id'],
            sent_message_id=msg_out['message_id'])
        dr = self.mkmsg_delivery(user_message_id=msg_out['message_id'],
            status='delivered')
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
            msg_in = self.mkmsg_in()
            msg_in['timestamp'] = now - timedelta(seconds=i * 10)
            yield self.cache.add_inbound_message(self.batch_id, msg_in)

        self.assertEqual(
            (yield self.cache.count_inbound_throughput(self.batch_id)), 10)
        self.assertEqual(
            (yield self.cache.count_inbound_throughput(self.batch_id,
                sample_time=1)), 1)
        self.assertEqual(
            (yield self.cache.count_inbound_throughput(self.batch_id,
                sample_time=10)), 2)

    @inlineCallbacks
    def test_count_outbound_throughput(self):
        # test for empty batches.
        self.assertEqual(
            (yield self.cache.count_outbound_throughput(self.batch_id)), 0)

        now = datetime.now()
        for i in range(10):
            msg_out = self.mkmsg_out()
            msg_out['timestamp'] = now - timedelta(seconds=i * 10)
            yield self.cache.add_outbound_message(self.batch_id, msg_out)

        self.assertEqual(
            (yield self.cache.count_outbound_throughput(self.batch_id)), 10)
        self.assertEqual(
            (yield self.cache.count_outbound_throughput(self.batch_id,
                sample_time=1)), 1)
        self.assertEqual(
            (yield self.cache.count_outbound_throughput(self.batch_id,
                sample_time=10)), 2)

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
            msg_in = self.mkmsg_in(content='hello-%s' % (i,))
            msg_in['timestamp'] = now + timedelta(seconds=i * 10)
            yield self.cache.add_inbound_message(self.batch_id, msg_in)
            message_ids.append(msg_in['message_id'])

        token = yield self.cache.start_query(self.batch_id, 'inbound', [
            {'key': 'msg.content', 'pattern': 'hello', 'flags': ''}])
        yield self.cache.store_query_results(self.batch_id, token, message_ids,
            'inbound', 120)
        self.assertFalse(
            (yield self.cache.is_query_in_progress(self.batch_id, token)))
        self.assertEqual(
            (yield self.cache.get_query_results(self.batch_id, token)),
            list(reversed(message_ids)))
        self.assertEqual(
            (yield self.cache.count_query_results(self.batch_id, token)),
            10)
