# -*- coding: utf-8 -*-

"""Tests for vumi.components.message_store."""
import time
from datetime import datetime, timedelta

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import TransportEvent
from vumi.application.tests.test_base import ApplicationTestCase
from vumi.components import MessageStore


class TestMessageStoreBase(ApplicationTestCase):
    # inherits from ApplicationTestCase for .mkmsg_in and .mkmsg_out

    use_riak = True

    @inlineCallbacks
    def setUp(self):
        yield super(TestMessageStoreBase, self).setUp()
        self.redis = yield self.get_redis_manager()
        self.manager = self.get_riak_manager()
        self.store = MessageStore(self.manager, self.redis)

    @inlineCallbacks
    def _maybe_batch(self, tag, by_batch):
        add_kw, batch_id = {}, None
        if tag is not None:
            batch_id = yield self.store.batch_start([tag])
            if by_batch:
                add_kw['batch_id'] = batch_id
            else:
                add_kw['tag'] = tag
        returnValue((add_kw, batch_id))

    @inlineCallbacks
    def _create_outbound(self, tag=("pool", "tag"), by_batch=False,
                            content='outbound foo'):
        """Create and store an outbound message."""
        add_kw, batch_id = yield self._maybe_batch(tag, by_batch)
        msg = self.mkmsg_out(content=content,
                             message_id=TransportEvent.generate_id())
        msg_id = msg['message_id']
        yield self.store.add_outbound_message(msg, **add_kw)
        returnValue((msg_id, msg, batch_id))

    @inlineCallbacks
    def _create_inbound(self, tag=("pool", "tag"), by_batch=False,
                            content='inbound foo'):
        """Create and store an inbound message."""
        add_kw, batch_id = yield self._maybe_batch(tag, by_batch)
        msg = self.mkmsg_in(content=content, to_addr="+1234567810001",
                            transport_type="sms",
                            message_id=TransportEvent.generate_id())
        msg_id = msg['message_id']
        yield self.store.add_inbound_message(msg, **add_kw)
        returnValue((msg_id, msg, batch_id))

    @inlineCallbacks
    def create_outbound_messages(self, batch_id, count, start_timestamp=None,
                                    time_multiplier=10):
        # Store via message_store
        now = start_timestamp or datetime.now()
        messages = []
        for i in range(count):
            msg = self.mkmsg_out(message_id=TransportEvent.generate_id())
            msg['timestamp'] = now - timedelta(i * time_multiplier)
            yield self.store.add_outbound_message(msg, batch_id=batch_id)
            messages.append(msg)
        returnValue(messages)

    @inlineCallbacks
    def create_inbound_messages(self, batch_id, count, start_timestamp=None,
                                    time_multiplier=10):
        # Store via message_store
        now = start_timestamp or datetime.now()
        messages = []
        for i in range(count):
            msg = self.mkmsg_in(message_id=TransportEvent.generate_id())
            msg['timestamp'] = now - timedelta(i * time_multiplier)
            yield self.store.add_inbound_message(msg, batch_id=batch_id)
            messages.append(msg)
        returnValue(messages)

    def _batch_status(self, ack=0, nack=0, delivered=0, failed=0, pending=0,
                        sent=0):
        return {
            'ack': ack, 'nack': nack, 'sent': sent,
            'delivery_report': sum([delivered, failed, pending]),
            'delivery_report.delivered': delivered,
            'delivery_report.failed': failed,
            'delivery_report.pending': pending,
            }


class TestMessageStore(TestMessageStoreBase):

    @inlineCallbacks
    def test_batch_start(self):
        tag1 = ("poolA", "tag1")
        batch_id = yield self.store.batch_start([tag1])
        batch = yield self.store.get_batch(batch_id)
        tag_info = yield self.store.get_tag_info(tag1)
        outbound_keys = yield self.store.batch_outbound_keys(batch_id)
        batch_status = yield self.store.batch_status(batch_id)
        self.assertEqual(outbound_keys, [])
        self.assertEqual(list(batch.tags), [tag1])
        self.assertEqual(tag_info.current_batch.key, batch_id)
        self.assertEqual(batch_status, self._batch_status())

    @inlineCallbacks
    def test_batch_start_with_metadata(self):
        batch_id = yield self.store.batch_start([], key1=u"foo", key2=u"bar")
        batch = yield self.store.get_batch(batch_id)
        self.assertEqual(batch.metadata['key1'], "foo")
        self.assertEqual(batch.metadata['key2'], "bar")

    @inlineCallbacks
    def test_batch_done(self):
        tag1 = ("poolA", "tag1")
        batch_id = yield self.store.batch_start([tag1])
        yield self.store.batch_done(batch_id)
        batch = yield self.store.get_batch(batch_id)
        tag_info = yield self.store.get_tag_info(tag1)
        self.assertEqual(list(batch.tags), [tag1])
        self.assertEqual(tag_info.current_batch.key, None)

    @inlineCallbacks
    def test_add_outbound_message(self):
        msg_id, msg, _batch_id = yield self._create_outbound(tag=None)

        stored_msg = yield self.store.get_outbound_message(msg_id)
        self.assertEqual(stored_msg, msg)
        event_keys = yield self.store.message_event_keys(msg_id)
        self.assertEqual(event_keys, [])

    @inlineCallbacks
    def test_add_outbound_message_with_batch_id(self):
        msg_id, msg, batch_id = yield self._create_outbound(by_batch=True)

        stored_msg = yield self.store.get_outbound_message(msg_id)
        outbound_keys = yield self.store.batch_outbound_keys(batch_id)
        event_keys = yield self.store.message_event_keys(msg_id)
        batch_status = yield self.store.batch_status(batch_id)

        self.assertEqual(stored_msg, msg)
        self.assertEqual(outbound_keys, [msg_id])
        self.assertEqual(event_keys, [])
        self.assertEqual(batch_status, self._batch_status(sent=1))

    @inlineCallbacks
    def test_add_outbound_message_with_tag(self):
        msg_id, msg, batch_id = yield self._create_outbound()

        stored_msg = yield self.store.get_outbound_message(msg_id)
        outbound_keys = yield self.store.batch_outbound_keys(batch_id)
        event_keys = yield self.store.message_event_keys(msg_id)
        batch_status = yield self.store.batch_status(batch_id)

        self.assertEqual(stored_msg, msg)
        self.assertEqual(outbound_keys, [msg_id])
        self.assertEqual(event_keys, [])
        self.assertEqual(batch_status, self._batch_status(sent=1))

    @inlineCallbacks
    def test_add_ack_event(self):
        msg_id, msg, batch_id = yield self._create_outbound()
        ack = self.mkmsg_ack(user_message_id=msg_id)
        ack_id = ack['event_id']
        yield self.store.add_event(ack)

        stored_ack = yield self.store.get_event(ack_id)
        event_keys = yield self.store.message_event_keys(msg_id)
        batch_status = yield self.store.batch_status(batch_id)

        self.assertEqual(stored_ack, ack)
        self.assertEqual(event_keys, [ack_id])
        self.assertEqual(batch_status, self._batch_status(sent=1, ack=1))

    @inlineCallbacks
    def test_add_nack_event(self):
        msg_id, msg, batch_id = yield self._create_outbound()
        nack = self.mkmsg_nack(user_message_id=msg_id)
        nack_id = nack['event_id']
        yield self.store.add_event(nack)

        stored_nack = yield self.store.get_event(nack_id)
        event_keys = yield self.store.message_event_keys(msg_id)
        batch_status = yield self.store.batch_status(batch_id)

        self.assertEqual(stored_nack, nack)
        self.assertEqual(event_keys, [nack_id])
        self.assertEqual(batch_status, self._batch_status(sent=1, nack=1))

    @inlineCallbacks
    def test_add_ack_event_without_batch(self):
        msg_id, msg, _batch_id = yield self._create_outbound(tag=None)
        ack = self.mkmsg_ack(user_message_id=msg_id)
        ack_id = ack['event_id']
        yield self.store.add_event(ack)

        stored_ack = yield self.store.get_event(ack_id)
        event_keys = yield self.store.message_event_keys(msg_id)

        self.assertEqual(stored_ack, ack)
        self.assertEqual(event_keys, [ack_id])

    @inlineCallbacks
    def test_add_nack_event_without_batch(self):
        msg_id, msg, _batch_id = yield self._create_outbound(tag=None)
        nack = self.mkmsg_nack(user_message_id=msg_id)
        nack_id = nack['event_id']
        yield self.store.add_event(nack)

        stored_nack = yield self.store.get_event(nack_id)
        event_keys = yield self.store.message_event_keys(msg_id)

        self.assertEqual(stored_nack, nack)
        self.assertEqual(event_keys, [nack_id])

    @inlineCallbacks
    def test_add_delivery_report_events(self):
        msg_id, msg, batch_id = yield self._create_outbound()

        dr_ids = []
        for status in TransportEvent.DELIVERY_STATUSES:
            dr = self.mkmsg_delivery(user_message_id=msg_id,
                                        status=status)
            dr_id = dr['event_id']
            dr_ids.append(dr_id)
            yield self.store.add_event(dr)
            stored_dr = yield self.store.get_event(dr_id)
            self.assertEqual(stored_dr, dr)

        event_keys = yield self.store.message_event_keys(msg_id)
        self.assertEqual(sorted(event_keys), sorted(dr_ids))
        dr_counts = dict((status, 1)
                         for status in TransportEvent.DELIVERY_STATUSES)
        batch_status = yield self.store.batch_status(batch_id)
        self.assertEqual(batch_status, self._batch_status(sent=1, **dr_counts))

    @inlineCallbacks
    def test_add_inbound_message(self):
        msg_id, msg, _batch_id = yield self._create_inbound(tag=None)
        stored_msg = yield self.store.get_inbound_message(msg_id)
        self.assertEqual(stored_msg, msg)

    @inlineCallbacks
    def test_add_inbound_message_with_batch_id(self):
        msg_id, msg, batch_id = yield self._create_inbound(by_batch=True)

        stored_msg = yield self.store.get_inbound_message(msg_id)
        inbound_keys = yield self.store.batch_inbound_keys(batch_id)

        self.assertEqual(stored_msg, msg)
        self.assertEqual(inbound_keys, [msg_id])

    @inlineCallbacks
    def test_add_inbound_message_with_tag(self):
        msg_id, msg, batch_id = yield self._create_inbound()

        stored_msg = yield self.store.get_inbound_message(msg_id)
        inbound_keys = yield self.store.batch_inbound_keys(batch_id)

        self.assertEqual(stored_msg, msg)
        self.assertEqual(inbound_keys, [msg_id])

    @inlineCallbacks
    def test_inbound_counts(self):
        _msg_id, _msg, batch_id = yield self._create_inbound(by_batch=True)
        self.assertEqual(1, (yield self.store.batch_inbound_count(batch_id)))
        yield self.store.add_inbound_message(self.mkmsg_in(
                message_id=TransportEvent.generate_id()), batch_id=batch_id)
        self.assertEqual(2, (yield self.store.batch_inbound_count(batch_id)))

    @inlineCallbacks
    def test_outbound_counts(self):
        _msg_id, _msg, batch_id = yield self._create_outbound(by_batch=True)
        self.assertEqual(1, (yield self.store.batch_outbound_count(batch_id)))
        yield self.store.add_outbound_message(self.mkmsg_out(
                message_id=TransportEvent.generate_id()), batch_id=batch_id)
        self.assertEqual(2, (yield self.store.batch_outbound_count(batch_id)))

    @inlineCallbacks
    def test_inbound_keys_matching(self):
        msg_id, msg, batch_id = yield self._create_inbound(content='hello')
        self.assertEqual([msg_id],
            (yield self.store.batch_inbound_keys_matching(batch_id, query=[{
                'key': 'msg.content',
                'pattern': 'hell.+',
                'flags': 'i',
            }])))
        # test case sensitivity
        self.assertEqual([],
            (yield self.store.batch_inbound_keys_matching(batch_id, query=[{
                'key': 'msg.content',
                'pattern': 'HELLO',
                'flags': '',
            }])))
        # the inbound from_addr has a leading +, it needs to be escaped
        self.assertEqual([msg_id],
            (yield self.store.batch_inbound_keys_matching(batch_id, query=[{
                'key': 'msg.from_addr',
                'pattern': "\%s" % (msg.payload['from_addr'],),
                'flags': 'i',
            }])))
        # the outbound to_addr has a leading +, it needs to be escaped
        self.assertEqual([msg_id],
            (yield self.store.batch_inbound_keys_matching(batch_id, query=[{
                'key': 'msg.to_addr',
                'pattern': "\%s" % (msg.payload['to_addr'],),
                'flags': 'i',
            }])))

    @inlineCallbacks
    def test_outbound_keys_matching(self):
        msg_id, msg, batch_id = yield self._create_outbound(content='hello')
        self.assertEqual([msg_id],
            (yield self.store.batch_outbound_keys_matching(batch_id, query=[{
                'key': 'msg.content',
                'pattern': 'hell.+',
                'flags': 'i',
            }])))
        # test case sensitivity
        self.assertEqual([],
            (yield self.store.batch_outbound_keys_matching(batch_id, query=[{
                'key': 'msg.content',
                'pattern': 'HELLO',
                'flags': '',
            }])))
        self.assertEqual([msg_id],
            (yield self.store.batch_outbound_keys_matching(batch_id, query=[{
                'key': 'msg.from_addr',
                'pattern': msg.payload['from_addr'],
                'flags': 'i',
            }])))
        # the outbound to_addr has a leading +, it needs to be escaped
        self.assertEqual([msg_id],
            (yield self.store.batch_outbound_keys_matching(batch_id, query=[{
                'key': 'msg.to_addr',
                'pattern': "\%s" % (msg.payload['to_addr'],),
                'flags': 'i',
            }])))


class TestMessageStoreCache(TestMessageStoreBase):

    def clear_cache(self, message_store):
        # FakeRedis provides a flushdb() function but TxRedisManager doesn't
        # and I'm not sure what the intended behaviour of flushdb on a
        # submanager is
        message_store.cache.redis._client._data = {}

    @inlineCallbacks
    def test_cache_batch_start(self):
        batch_id = yield self.store.batch_start([("poolA", "tag1")])
        self.assertTrue((yield self.store.cache.batch_exists(batch_id)))
        self.assertTrue(batch_id in (yield self.store.cache.get_batch_ids()))

    @inlineCallbacks
    def test_cache_add_outbound_message(self):
        msg_id, msg, batch_id = yield self._create_outbound()
        [cached_msg_id] = (yield
            self.store.cache.get_outbound_message_keys(batch_id))
        [cached_to_addr] = (yield
            self.store.cache.get_to_addrs(batch_id))
        self.assertEqual(msg_id, cached_msg_id)
        self.assertEqual(msg['to_addr'], cached_to_addr)

    @inlineCallbacks
    def test_cache_add_inbound_message(self):
        msg_id, msg, batch_id = yield self._create_inbound()
        [cached_msg_id] = (yield
            self.store.cache.get_inbound_message_keys(batch_id))
        [cached_from_addr] = (yield
            self.store.cache.get_from_addrs(batch_id))
        self.assertEqual(msg_id, cached_msg_id)
        self.assertEqual(msg['from_addr'], cached_from_addr)

    @inlineCallbacks
    def test_cache_add_event(self):
        msg_id, msg, batch_id = yield self._create_outbound()
        ack = TransportEvent(user_message_id=msg_id, event_type='ack',
                             sent_message_id='xyz')
        yield self.store.add_event(ack)
        self.assertEqual((yield self.store.cache.get_event_status(batch_id)), {
            'delivery_report': 0,
            'delivery_report.delivered': 0,
            'delivery_report.failed': 0,
            'delivery_report.pending': 0,
            'ack': 1,
            'nack': 0,
            'sent': 1,
        })

    @inlineCallbacks
    def test_needs_reconciliation(self):
        msg_id, msg, batch_id = yield self._create_outbound()
        self.assertFalse((yield self.store.needs_reconciliation(batch_id)))

        msg_id, msg, batch_id = yield self._create_outbound()

        # Store via message_store
        yield self.create_outbound_messages(batch_id, 10)

        # Store one extra in the cache to throw off the allow threshold delta
        recon_msg = self.mkmsg_out(message_id=TransportEvent.generate_id())
        yield self.store.cache.add_outbound_message(batch_id, recon_msg)

        # Default reconciliation delta should return True
        self.assertTrue((yield self.store.needs_reconciliation(batch_id)))
        # More liberal reconciliation delta should return False
        self.assertFalse((
            yield self.store.needs_reconciliation(batch_id, delta=0.1)))

    @inlineCallbacks
    def test_reconcile_cache(self):
        batch_id = yield self.store.batch_start([("pool", "tag")])

        # Store via message_store
        messages = yield self.create_outbound_messages(batch_id, 10)
        for msg in messages:
            ack = self.mkmsg_ack(user_message_id=msg['message_id'],
                sent_message_id=msg['message_id'])
            yield self.store.add_event(ack)

        self.clear_cache(self.store)
        batch_status = yield self.store.batch_status(batch_id)
        self.assertEqual(batch_status, {})
        # Default reconciliation delta should return True
        self.assertTrue((yield self.store.needs_reconciliation(batch_id)))
        yield self.store.reconcile_cache(batch_id)
        # Default reconciliation delta should return True
        self.assertFalse((yield self.store.needs_reconciliation(batch_id)))
        # Stricted possible reconciliation delta should return True
        self.assertFalse((yield self.store.needs_reconciliation(batch_id,
            delta=0)))
        batch_status = yield self.store.batch_status(batch_id)
        self.assertEqual(batch_status['ack'], 10)
        self.assertEqual(batch_status['sent'], 10)

    @inlineCallbacks
    def test_find_inbound_keys_matching(self):
        batch_id = yield self.store.batch_start([("pool", "tag")])

        # Store via message_store
        messages = yield self.create_inbound_messages(batch_id, 10)

        token = yield self.store.find_inbound_keys_matching(batch_id, [{
                'key': 'msg.content',
                'pattern': '.*',
                'flags': 'i',
            }], wait=True)

        keys = yield self.store.get_keys_for_token(batch_id, token)
        in_progress = yield self.store.cache.is_query_in_progress(batch_id,
                                                                    token)
        self.assertEqual(len(keys), 10)
        self.assertEqual(10,
            (yield self.store.count_keys_for_token(batch_id, token)))
        self.assertEqual(keys, [msg['message_id'] for msg in messages])
        self.assertFalse(in_progress)

    @inlineCallbacks
    def test_find_outbound_keys_matching(self):
        batch_id = yield self.store.batch_start([("pool", "tag")])

        # Store via message_store
        messages = yield self.create_outbound_messages(batch_id, 10)

        token = yield self.store.find_outbound_keys_matching(batch_id, [{
                'key': 'msg.content',
                'pattern': '.*',
                'flags': 'i',
            }], wait=True)

        keys = yield self.store.get_keys_for_token(batch_id, token)
        in_progress = yield self.store.cache.is_query_in_progress(batch_id,
                                                                    token)
        self.assertEqual(len(keys), 10)
        self.assertEqual(10,
            (yield self.store.count_keys_for_token(batch_id, token)))
        self.assertEqual(keys, [msg['message_id'] for msg in messages])
        self.assertFalse(in_progress)

    @inlineCallbacks
    def test_get_inbound_message_keys(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 10)

        keys = yield self.store.get_inbound_message_keys(batch_id)
        self.assertEqual(keys, [msg['message_id'] for msg in messages])

    @inlineCallbacks
    def test_get_inbound_message_keys_with_timestamp(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 10)

        results = dict((yield self.store.get_inbound_message_keys(
                                batch_id, with_timestamp=True)))
        for msg in messages:
            found = results[msg['message_id']]
            expected = time.mktime(msg['timestamp'].timetuple())
            self.assertAlmostEqual(found, expected)

    @inlineCallbacks
    def test_get_outbound_message_keys(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 10)

        keys = yield self.store.get_outbound_message_keys(batch_id)
        self.assertEqual(keys, [msg['message_id'] for msg in messages])

    @inlineCallbacks
    def test_get_outbound_message_keys_with_timestamp(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 10)

        results = dict((yield self.store.get_outbound_message_keys(
                                batch_id, with_timestamp=True)))
        for msg in messages:
            found = results[msg['message_id']]
            expected = time.mktime(msg['timestamp'].timetuple())
            self.assertAlmostEqual(found, expected)
