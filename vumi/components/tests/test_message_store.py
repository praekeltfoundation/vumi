# -*- coding: utf-8 -*-

"""Tests for vumi.components.message_store."""
import time
from datetime import datetime, timedelta

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import TransportEvent, format_vumi_date
from vumi.tests.helpers import (
    VumiTestCase, MessageHelper, PersistenceHelper, import_skip)

try:
    from vumi.components.message_store import (
        MessageStore, to_reverse_timestamp, from_reverse_timestamp,
        add_batches_to_event)
except ImportError, e:
    import_skip(e, 'riak')


def zero_ms(timestamp):
    dt, dot, ms = format_vumi_date(timestamp).partition(".")
    return dot.join([dt, "0" * len(ms)])


class TestReverseTimestampUtils(VumiTestCase):

    def test_to_reverse_timestamp(self):
        """
        to_reverse_timestamp() turns a vumi_date-formatted string into a
        reverse timestamp.
        """
        self.assertEqual(
            "FFAAE41F25", to_reverse_timestamp("2015-04-01 12:13:14"))
        self.assertEqual(
            "FFAAE41F25", to_reverse_timestamp("2015-04-01 12:13:14.000000"))
        self.assertEqual(
            "FFAAE41F25", to_reverse_timestamp("2015-04-01 12:13:14.999999"))
        self.assertEqual(
            "FFAAE41F24", to_reverse_timestamp("2015-04-01 12:13:15"))
        self.assertEqual(
            "F0F9025FA5", to_reverse_timestamp("4015-04-01 12:13:14"))

    def test_from_reverse_timestamp(self):
        """
        from_reverse_timestamp() is the inverse of to_reverse_timestamp().
        """
        self.assertEqual(
            "2015-04-01 12:13:14.000000", from_reverse_timestamp("FFAAE41F25"))
        self.assertEqual(
            "2015-04-01 12:13:13.000000", from_reverse_timestamp("FFAAE41F26"))
        self.assertEqual(
            "4015-04-01 12:13:14.000000", from_reverse_timestamp("F0F9025FA5"))


class TestMessageStoreBase(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        self.redis = yield self.persistence_helper.get_redis_manager()
        self.manager = self.persistence_helper.get_riak_manager()
        self.store = MessageStore(self.manager, self.redis)
        self.msg_helper = self.add_helper(MessageHelper())

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
        msg = self.msg_helper.make_outbound(content)
        msg_id = msg['message_id']
        yield self.store.add_outbound_message(msg, **add_kw)
        returnValue((msg_id, msg, batch_id))

    @inlineCallbacks
    def _create_inbound(self, tag=("pool", "tag"), by_batch=False,
                        content='inbound foo'):
        """Create and store an inbound message."""
        add_kw, batch_id = yield self._maybe_batch(tag, by_batch)
        msg = self.msg_helper.make_inbound(
            content, to_addr="+1234567810001", transport_type="sms")
        msg_id = msg['message_id']
        yield self.store.add_inbound_message(msg, **add_kw)
        returnValue((msg_id, msg, batch_id))

    @inlineCallbacks
    def create_outbound_messages(self, batch_id, count, start_timestamp=None,
                                 time_multiplier=10, to_addr=None):
        # Store via message_store
        now = start_timestamp or datetime.now()
        messages = []
        for i in range(count):
            msg = self.msg_helper.make_outbound(
                "foo", timestamp=(now - timedelta(i * time_multiplier)))
            if to_addr is not None:
                msg['to_addr'] = to_addr
            yield self.store.add_outbound_message(msg, batch_id=batch_id)
            messages.append(msg)
        returnValue(messages)

    @inlineCallbacks
    def create_inbound_messages(self, batch_id, count, start_timestamp=None,
                                time_multiplier=10, from_addr=None):
        # Store via message_store
        now = start_timestamp or datetime.now()
        messages = []
        for i in range(count):
            msg = self.msg_helper.make_inbound(
                "foo", timestamp=(now - timedelta(i * time_multiplier)))
            if from_addr is not None:
                msg['from_addr'] = from_addr
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
    def test_add_outbound_message_again(self):
        msg_id, msg, _batch_id = yield self._create_outbound(tag=None)

        old_stored_msg = yield self.store.get_outbound_message(msg_id)
        self.assertEqual(old_stored_msg, msg)

        msg['helper_metadata']['foo'] = {'bar': 'baz'}
        yield self.store.add_outbound_message(msg)
        new_stored_msg = yield self.store.get_outbound_message(msg_id)
        self.assertEqual(new_stored_msg, msg)
        self.assertNotEqual(old_stored_msg, new_stored_msg)

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
    def test_add_outbound_message_to_multiple_batches(self):
        msg_id, msg, batch_id_1 = yield self._create_outbound()
        batch_id_2 = yield self.store.batch_start()
        yield self.store.add_outbound_message(msg, batch_id=batch_id_2)

        self.assertEqual(
            (yield self.store.batch_outbound_keys(batch_id_1)), [msg_id])
        self.assertEqual(
            (yield self.store.batch_outbound_keys(batch_id_2)), [msg_id])
        # Make sure we're writing the right indexes.
        stored_msg = yield self.store.outbound_messages.load(msg_id)
        timestamp = format_vumi_date(msg['timestamp'])
        reverse_ts = to_reverse_timestamp(timestamp)
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', batch_id_1),
            ('batches_bin', batch_id_2),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % (batch_id_1, timestamp, msg['to_addr'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % (batch_id_2, timestamp, msg['to_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % (batch_id_1, reverse_ts, msg['to_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % (batch_id_2, reverse_ts, msg['to_addr'])),
        ]))

    @inlineCallbacks
    def test_get_events_for_message(self):
        msg_id, msg, batch_id = yield self._create_outbound()
        ack = self.msg_helper.make_ack(msg)
        ack_id = ack['event_id']
        yield self.store.add_event(ack)

        dr = self.msg_helper.make_delivery_report(msg)
        dr_id = ack['event_id']
        yield self.store.add_event(dr)

        stored_ack = yield self.store.get_event(ack_id)
        stored_dr = yield self.store.get_event(dr_id)

        events = yield self.store.get_events_for_message(msg_id)

        self.assertTrue(len(events), 2)
        self.assertTrue(
            all(isinstance(event, TransportEvent) for event in events))
        self.assertTrue(stored_ack in events)
        self.assertTrue(stored_dr in events)

    @inlineCallbacks
    def test_add_ack_event_batch_ids_from_outbound(self):
        """
        If the `batch_ids` param is not given, and the event doesn't exist,
        batch ids are looked up on the outbound message.
        """
        msg_id, msg, batch_id = yield self._create_outbound()
        ack = self.msg_helper.make_ack(msg)
        ack_id = ack['event_id']
        yield self.store.add_event(ack)

        stored_ack = yield self.store.get_event(ack_id)
        event_keys = yield self.store.message_event_keys(msg_id)
        batch_status = yield self.store.batch_status(batch_id)

        self.assertEqual(stored_ack, ack)
        self.assertEqual(event_keys, [ack_id])
        self.assertEqual(batch_status, self._batch_status(sent=1, ack=1))

        event = yield self.store.events.load(ack_id)
        self.assertEqual(event.batches.keys(), [batch_id])
        timestamp = format_vumi_date(ack["timestamp"])
        self.assertEqual(event.message_with_status, "%s$%s$ack" % (
            msg_id, timestamp))
        self.assertEqual(set(event.batches_with_statuses_reverse), set([
            "%s$%s$ack" % (batch_id, to_reverse_timestamp(timestamp)),
        ]))

    @inlineCallbacks
    def test_add_ack_event_uses_existing_batches(self):
        """
        If the `batch_ids` param is not given, and the event already
        exists, batch ids should not be looked up on the outbound message.
        """
        # create a message but don't store it
        msg = self.msg_helper.make_outbound('outbound text')
        msg_id = msg['message_id']
        batch_id = yield self.store.batch_start([('pool', 'tag')])

        # create an event and store it
        ack = self.msg_helper.make_ack(msg)
        ack_id = ack['event_id']
        yield self.store.add_event(ack, [batch_id])

        # now store the event again without specifying batches
        yield self.store.add_event(ack)

        stored_ack = yield self.store.get_event(ack_id)
        event_keys = yield self.store.message_event_keys(msg_id)
        batch_status = yield self.store.batch_status(batch_id)

        self.assertEqual(stored_ack, ack)
        self.assertEqual(event_keys, [ack_id])
        self.assertEqual(batch_status, self._batch_status(sent=0, ack=1))

        event = yield self.store.events.load(ack_id)
        self.assertEqual(event.batches.keys(), [batch_id])
        timestamp = format_vumi_date(ack["timestamp"])
        self.assertEqual(event.message_with_status, "%s$%s$ack" % (
            msg_id, timestamp))
        self.assertEqual(set(event.batches_with_statuses_reverse), set([
            "%s$%s$ack" % (batch_id, to_reverse_timestamp(timestamp)),
        ]))

    @inlineCallbacks
    def test_add_ack_event_with_batch_ids(self):
        """
        If an event is added with batch_ids provided, those batch_ids are used.
        """
        msg_id, msg, batch_id = yield self._create_outbound()
        batch_1 = yield self.store.batch_start([])
        batch_2 = yield self.store.batch_start([])

        ack = self.msg_helper.make_ack(msg)
        ack_id = ack['event_id']
        yield self.store.add_event(ack, batch_ids=[batch_1, batch_2])

        stored_ack = yield self.store.get_event(ack_id)
        event_keys = yield self.store.message_event_keys(msg_id)
        batch_status = yield self.store.batch_status(batch_id)
        batch_1_status = yield self.store.batch_status(batch_1)
        batch_2_status = yield self.store.batch_status(batch_2)

        self.assertEqual(stored_ack, ack)
        self.assertEqual(event_keys, [ack_id])
        self.assertEqual(batch_status, self._batch_status(sent=1))
        self.assertEqual(batch_1_status, self._batch_status(ack=1))
        self.assertEqual(batch_2_status, self._batch_status(ack=1))

        event = yield self.store.events.load(ack_id)
        timestamp = format_vumi_date(ack["timestamp"])
        self.assertEqual(event.message_with_status, "%s$%s$ack" % (
            msg_id, timestamp))
        self.assertEqual(set(event.batches_with_statuses_reverse), set([
            "%s$%s$ack" % (batch_1, to_reverse_timestamp(timestamp)),
            "%s$%s$ack" % (batch_2, to_reverse_timestamp(timestamp)),
        ]))

    @inlineCallbacks
    def test_add_ack_event_without_batch_ids_no_outbound(self):
        """
        If an event is added without batch_ids and no outbound message is
        found, no batch_ids will be used.
        """
        msg_id, msg, batch_id = yield self._create_outbound()

        ack = self.msg_helper.make_ack(msg)
        ack_id = ack['event_id']
        ack['user_message_id'] = "no-message"
        yield self.store.add_event(ack)

        stored_ack = yield self.store.get_event(ack_id)
        event_keys = yield self.store.message_event_keys("no-message")
        batch_status = yield self.store.batch_status(batch_id)

        self.assertEqual(stored_ack, ack)
        self.assertEqual(event_keys, [ack_id])
        self.assertEqual(batch_status, self._batch_status(sent=1))

        event = yield self.store.events.load(ack_id)
        timestamp = format_vumi_date(ack["timestamp"])
        self.assertEqual(event.message_with_status, "%s$%s$ack" % (
            "no-message", timestamp))
        self.assertEqual(set(event.batches_with_statuses_reverse), set())

    @inlineCallbacks
    def test_add_ack_event_with_empty_batch_ids(self):
        """
        If an event is added with an empty list of batch_ids, no batch_ids will
        be used.
        """
        msg_id, msg, batch_id = yield self._create_outbound()
        ack = self.msg_helper.make_ack(msg)
        ack_id = ack['event_id']
        yield self.store.add_event(ack, batch_ids=[])

        stored_ack = yield self.store.get_event(ack_id)
        event_keys = yield self.store.message_event_keys(msg_id)
        batch_status = yield self.store.batch_status(batch_id)

        self.assertEqual(stored_ack, ack)
        self.assertEqual(event_keys, [ack_id])
        self.assertEqual(batch_status, self._batch_status(sent=1))

        event = yield self.store.events.load(ack_id)
        timestamp = format_vumi_date(ack["timestamp"])
        self.assertEqual(event.message_with_status, "%s$%s$ack" % (
            msg_id, timestamp))
        self.assertEqual(set(event.batches_with_statuses_reverse), set())

    @inlineCallbacks
    def test_add_ack_event_again(self):
        msg_id, msg, batch_id = yield self._create_outbound()
        ack = self.msg_helper.make_ack(msg)
        ack_id = ack['event_id']
        yield self.store.add_event(ack)
        old_stored_ack = yield self.store.get_event(ack_id)
        self.assertEqual(old_stored_ack, ack)

        ack['helper_metadata']['foo'] = {'bar': 'baz'}
        yield self.store.add_event(ack)
        new_stored_ack = yield self.store.get_event(ack_id)
        self.assertEqual(new_stored_ack, ack)
        self.assertNotEqual(old_stored_ack, new_stored_ack)

        event_keys = yield self.store.message_event_keys(msg_id)
        batch_status = yield self.store.batch_status(batch_id)

        self.assertEqual(event_keys, [ack_id])
        self.assertEqual(batch_status, self._batch_status(sent=1, ack=1))

    @inlineCallbacks
    def test_add_nack_event(self):
        msg_id, msg, batch_id = yield self._create_outbound()
        nack = self.msg_helper.make_nack(msg)
        nack_id = nack['event_id']
        yield self.store.add_event(nack)

        stored_nack = yield self.store.get_event(nack_id)
        event_keys = yield self.store.message_event_keys(msg_id)
        batch_status = yield self.store.batch_status(batch_id)

        self.assertEqual(stored_nack, nack)
        self.assertEqual(event_keys, [nack_id])
        self.assertEqual(batch_status, self._batch_status(sent=1, nack=1))

        event = yield self.store.events.load(nack_id)
        self.assertEqual(event.message_with_status, "%s$%s$nack" % (
            msg_id, nack["timestamp"]))

    @inlineCallbacks
    def test_add_ack_event_without_batch(self):
        msg_id, msg, _batch_id = yield self._create_outbound(tag=None)
        ack = self.msg_helper.make_ack(msg)
        ack_id = ack['event_id']
        yield self.store.add_event(ack)

        stored_ack = yield self.store.get_event(ack_id)
        event_keys = yield self.store.message_event_keys(msg_id)

        self.assertEqual(stored_ack, ack)
        self.assertEqual(event_keys, [ack_id])

    @inlineCallbacks
    def test_add_nack_event_without_batch(self):
        msg_id, msg, _batch_id = yield self._create_outbound(tag=None)
        nack = self.msg_helper.make_nack(msg)
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
            dr = self.msg_helper.make_delivery_report(
                msg, delivery_status=status)
            dr_id = dr['event_id']
            dr_ids.append(dr_id)
            yield self.store.add_event(dr)
            stored_dr = yield self.store.get_event(dr_id)
            self.assertEqual(stored_dr, dr)

            event = yield self.store.events.load(dr_id)
            self.assertEqual(event.message_with_status, "%s$%s$%s" % (
                msg_id, dr["timestamp"], "delivery_report.%s" % (status,)))

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
    def test_add_inbound_message_again(self):
        msg_id, msg, _batch_id = yield self._create_inbound(tag=None)

        old_stored_msg = yield self.store.get_inbound_message(msg_id)
        self.assertEqual(old_stored_msg, msg)

        msg['helper_metadata']['foo'] = {'bar': 'baz'}
        yield self.store.add_inbound_message(msg)
        new_stored_msg = yield self.store.get_inbound_message(msg_id)
        self.assertEqual(new_stored_msg, msg)
        self.assertNotEqual(old_stored_msg, new_stored_msg)

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
    def test_add_inbound_message_to_multiple_batches(self):
        msg_id, msg, batch_id_1 = yield self._create_inbound()
        batch_id_2 = yield self.store.batch_start()
        yield self.store.add_inbound_message(msg, batch_id=batch_id_2)

        self.assertEqual((yield self.store.batch_inbound_keys(batch_id_1)),
                         [msg_id])
        self.assertEqual((yield self.store.batch_inbound_keys(batch_id_2)),
                         [msg_id])
        # Make sure we're writing the right indexes.
        stored_msg = yield self.store.inbound_messages.load(msg_id)
        timestamp = format_vumi_date(msg['timestamp'])
        reverse_ts = to_reverse_timestamp(timestamp)
        self.assertEqual(stored_msg._riak_object.get_indexes(), set([
            ('batches_bin', batch_id_1),
            ('batches_bin', batch_id_2),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % (batch_id_1, timestamp, msg['from_addr'])),
            ('batches_with_addresses_bin',
             "%s$%s$%s" % (batch_id_2, timestamp, msg['from_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % (batch_id_1, reverse_ts, msg['from_addr'])),
            ('batches_with_addresses_reverse_bin',
             "%s$%s$%s" % (batch_id_2, reverse_ts, msg['from_addr'])),
        ]))

    @inlineCallbacks
    def test_inbound_counts(self):
        _msg_id, _msg, batch_id = yield self._create_inbound(by_batch=True)
        self.assertEqual(1, (yield self.store.batch_inbound_count(batch_id)))
        yield self.store.add_inbound_message(
            self.msg_helper.make_inbound("foo"), batch_id=batch_id)
        self.assertEqual(2, (yield self.store.batch_inbound_count(batch_id)))

    @inlineCallbacks
    def test_outbound_counts(self):
        _msg_id, _msg, batch_id = yield self._create_outbound(by_batch=True)
        self.assertEqual(1, (yield self.store.batch_outbound_count(batch_id)))
        yield self.store.add_outbound_message(
            self.msg_helper.make_outbound("foo"), batch_id=batch_id)
        self.assertEqual(2, (yield self.store.batch_outbound_count(batch_id)))

    @inlineCallbacks
    def test_inbound_keys_matching(self):
        msg_id, msg, batch_id = yield self._create_inbound(content='hello')
        self.assertEqual(
            [msg_id],
            (yield self.store.batch_inbound_keys_matching(batch_id, query=[{
                'key': 'msg.content',
                'pattern': 'hell.+',
                'flags': 'i',
            }])))
        # test case sensitivity
        self.assertEqual(
            [],
            (yield self.store.batch_inbound_keys_matching(batch_id, query=[{
                'key': 'msg.content',
                'pattern': 'HELLO',
                'flags': '',
            }])))
        # the inbound from_addr has a leading +, it needs to be escaped
        self.assertEqual(
            [msg_id],
            (yield self.store.batch_inbound_keys_matching(batch_id, query=[{
                'key': 'msg.from_addr',
                'pattern': "\%s" % (msg.payload['from_addr'],),
                'flags': 'i',
            }])))
        # the outbound to_addr has a leading +, it needs to be escaped
        self.assertEqual(
            [msg_id],
            (yield self.store.batch_inbound_keys_matching(batch_id, query=[{
                'key': 'msg.to_addr',
                'pattern': "\%s" % (msg.payload['to_addr'],),
                'flags': 'i',
            }])))

    @inlineCallbacks
    def test_outbound_keys_matching(self):
        msg_id, msg, batch_id = yield self._create_outbound(content='hello')
        self.assertEqual(
            [msg_id],
            (yield self.store.batch_outbound_keys_matching(batch_id, query=[{
                'key': 'msg.content',
                'pattern': 'hell.+',
                'flags': 'i',
            }])))
        # test case sensitivity
        self.assertEqual(
            [],
            (yield self.store.batch_outbound_keys_matching(batch_id, query=[{
                'key': 'msg.content',
                'pattern': 'HELLO',
                'flags': '',
            }])))
        self.assertEqual(
            [msg_id],
            (yield self.store.batch_outbound_keys_matching(batch_id, query=[{
                'key': 'msg.from_addr',
                'pattern': msg.payload['from_addr'],
                'flags': 'i',
            }])))
        # the outbound to_addr has a leading +, it needs to be escaped
        self.assertEqual(
            [msg_id],
            (yield self.store.batch_outbound_keys_matching(batch_id, query=[{
                'key': 'msg.to_addr',
                'pattern': "\%s" % (msg.payload['to_addr'],),
                'flags': 'i',
            }])))

    @inlineCallbacks
    def test_add_inbound_message_with_batch_ids(self):
        batch_id1 = yield self.store.batch_start([])
        batch_id2 = yield self.store.batch_start([])
        msg = self.msg_helper.make_inbound("hi")

        yield self.store.add_inbound_message(
            msg, batch_ids=[batch_id1, batch_id2])

        stored_msg = yield self.store.get_inbound_message(msg['message_id'])
        inbound_keys1 = yield self.store.batch_inbound_keys(batch_id1)
        inbound_keys2 = yield self.store.batch_inbound_keys(batch_id2)

        self.assertEqual(stored_msg, msg)
        self.assertEqual(inbound_keys1, [msg['message_id']])
        self.assertEqual(inbound_keys2, [msg['message_id']])

    @inlineCallbacks
    def test_add_inbound_message_with_batch_id_and_batch_ids(self):
        batch_id1 = yield self.store.batch_start([])
        batch_id2 = yield self.store.batch_start([])
        msg = self.msg_helper.make_inbound("hi")

        yield self.store.add_inbound_message(
            msg, batch_id=batch_id1, batch_ids=[batch_id2])

        stored_msg = yield self.store.get_inbound_message(msg['message_id'])
        inbound_keys1 = yield self.store.batch_inbound_keys(batch_id1)
        inbound_keys2 = yield self.store.batch_inbound_keys(batch_id2)

        self.assertEqual(stored_msg, msg)
        self.assertEqual(inbound_keys1, [msg['message_id']])
        self.assertEqual(inbound_keys2, [msg['message_id']])

    @inlineCallbacks
    def test_add_outbound_message_with_batch_ids(self):
        batch_id1 = yield self.store.batch_start([])
        batch_id2 = yield self.store.batch_start([])
        msg = self.msg_helper.make_outbound("hi")

        yield self.store.add_outbound_message(
            msg, batch_ids=[batch_id1, batch_id2])

        stored_msg = yield self.store.get_outbound_message(msg['message_id'])
        outbound_keys1 = yield self.store.batch_outbound_keys(batch_id1)
        outbound_keys2 = yield self.store.batch_outbound_keys(batch_id2)

        self.assertEqual(stored_msg, msg)
        self.assertEqual(outbound_keys1, [msg['message_id']])
        self.assertEqual(outbound_keys2, [msg['message_id']])

    @inlineCallbacks
    def test_add_outbound_message_with_batch_id_and_batch_ids(self):
        batch_id1 = yield self.store.batch_start([])
        batch_id2 = yield self.store.batch_start([])
        msg = self.msg_helper.make_outbound("hi")

        yield self.store.add_outbound_message(
            msg, batch_id=batch_id1, batch_ids=[batch_id2])

        stored_msg = yield self.store.get_outbound_message(msg['message_id'])
        outbound_keys1 = yield self.store.batch_outbound_keys(batch_id1)
        outbound_keys2 = yield self.store.batch_outbound_keys(batch_id2)

        self.assertEqual(stored_msg, msg)
        self.assertEqual(outbound_keys1, [msg['message_id']])
        self.assertEqual(outbound_keys2, [msg['message_id']])

    @inlineCallbacks
    def test_batch_inbound_keys_page(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 10)
        all_keys = sorted(msg['message_id'] for msg in messages)

        keys_p1 = yield self.store.batch_inbound_keys_page(batch_id, 6)
        # Paginated results are sorted by key.
        self.assertEqual(sorted(keys_p1), all_keys[:6])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(sorted(keys_p2), all_keys[6:])

    @inlineCallbacks
    def test_batch_outbound_keys_page(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 10)
        all_keys = sorted(msg['message_id'] for msg in messages)

        keys_p1 = yield self.store.batch_outbound_keys_page(batch_id, 6)
        # Paginated results are sorted by key.
        self.assertEqual(sorted(keys_p1), all_keys[:6])

        keys_p2 = yield keys_p1.next_page()
        self.assertEqual(sorted(keys_p2), all_keys[6:])

    @inlineCallbacks
    def test_batch_inbound_keys_with_timestamp(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 10)
        sorted_keys = sorted((msg['timestamp'], msg['message_id'])
                             for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp))
                    for (timestamp, key) in sorted_keys]

        first_page = yield self.store.batch_inbound_keys_with_timestamps(
            batch_id, max_results=6)

        results = list(first_page)
        self.assertEqual(len(results), 6)
        self.assertEqual(first_page.has_next_page(), True)

        next_page = yield first_page.next_page()
        results.extend(next_page)
        self.assertEqual(len(results), 10)
        self.assertEqual(next_page.has_next_page(), False)

        self.assertEqual(results, all_keys)

    @inlineCallbacks
    def test_batch_inbound_keys_with_timestamp_start(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 5)
        sorted_keys = sorted((msg['timestamp'], msg['message_id'])
                             for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp))
                    for (timestamp, key) in sorted_keys]

        index_page = yield self.store.batch_inbound_keys_with_timestamps(
            batch_id, max_results=6, start=all_keys[1][1])
        self.assertEqual(list(index_page), all_keys[1:])

    @inlineCallbacks
    def test_batch_inbound_keys_with_timestamp_without_timestamps(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 5)
        sorted_keys = sorted((msg['timestamp'], msg['message_id'])
                             for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp))
                    for (timestamp, key) in sorted_keys]

        index_page = yield self.store.batch_inbound_keys_with_timestamps(
            batch_id, with_timestamps=False)
        self.assertEqual(list(index_page), [k for k, _ in all_keys])

    @inlineCallbacks
    def test_batch_inbound_keys_with_timestamp_end(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 5)
        sorted_keys = sorted((msg['timestamp'], msg['message_id'])
                             for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp))
                    for (timestamp, key) in sorted_keys]

        index_page = yield self.store.batch_inbound_keys_with_timestamps(
            batch_id, max_results=6, end=all_keys[-2][1])
        self.assertEqual(list(index_page), all_keys[:-1])

    @inlineCallbacks
    def test_batch_inbound_keys_with_timestamp_range(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 5)
        sorted_keys = sorted((msg['timestamp'], msg['message_id'])
                             for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp))
                    for (timestamp, key) in sorted_keys]

        index_page = yield self.store.batch_inbound_keys_with_timestamps(
            batch_id, max_results=6, start=all_keys[1][1], end=all_keys[-2][1])
        self.assertEqual(list(index_page), all_keys[1:-1])

    @inlineCallbacks
    def test_batch_outbound_keys_with_timestamp(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 10)
        sorted_keys = sorted((msg['timestamp'], msg['message_id'])
                             for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp))
                    for (timestamp, key) in sorted_keys]

        first_page = yield self.store.batch_outbound_keys_with_timestamps(
            batch_id, max_results=6)

        results = list(first_page)
        self.assertEqual(len(results), 6)
        self.assertEqual(first_page.has_next_page(), True)

        next_page = yield first_page.next_page()
        results.extend(next_page)
        self.assertEqual(len(results), 10)
        self.assertEqual(next_page.has_next_page(), False)

        self.assertEqual(results, all_keys)

    @inlineCallbacks
    def test_batch_outbound_keys_with_timestamp_without_timestamps(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 5)
        sorted_keys = sorted((msg['timestamp'], msg['message_id'])
                             for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp))
                    for (timestamp, key) in sorted_keys]

        index_page = yield self.store.batch_outbound_keys_with_timestamps(
            batch_id, with_timestamps=False)
        self.assertEqual(list(index_page), [k for k, _ in all_keys])

    @inlineCallbacks
    def test_batch_outbound_keys_with_timestamp_start(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 5)
        sorted_keys = sorted((msg['timestamp'], msg['message_id'])
                             for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp))
                    for (timestamp, key) in sorted_keys]

        index_page = yield self.store.batch_outbound_keys_with_timestamps(
            batch_id, max_results=6, start=all_keys[1][1])
        self.assertEqual(list(index_page), all_keys[1:])

    @inlineCallbacks
    def test_batch_outbound_keys_with_timestamp_end(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 5)
        sorted_keys = sorted((msg['timestamp'], msg['message_id'])
                             for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp))
                    for (timestamp, key) in sorted_keys]

        index_page = yield self.store.batch_outbound_keys_with_timestamps(
            batch_id, max_results=6, end=all_keys[-2][1])
        self.assertEqual(list(index_page), all_keys[:-1])

    @inlineCallbacks
    def test_batch_outbound_keys_with_timestamp_range(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 5)
        sorted_keys = sorted((msg['timestamp'], msg['message_id'])
                             for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp))
                    for (timestamp, key) in sorted_keys]

        index_page = yield self.store.batch_outbound_keys_with_timestamps(
            batch_id, max_results=6, start=all_keys[1][1], end=all_keys[-2][1])
        self.assertEqual(list(index_page), all_keys[1:-1])

    @inlineCallbacks
    def test_batch_inbound_keys_with_addresses(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 10)
        sorted_keys = sorted(
            (msg['timestamp'], msg['from_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        first_page = yield self.store.batch_inbound_keys_with_addresses(
            batch_id, max_results=6)

        results = list(first_page)
        self.assertEqual(len(results), 6)
        self.assertEqual(first_page.has_next_page(), True)

        next_page = yield first_page.next_page()
        results.extend(next_page)
        self.assertEqual(len(results), 10)
        self.assertEqual(next_page.has_next_page(), False)

        self.assertEqual(results, all_keys)

    @inlineCallbacks
    def test_batch_inbound_keys_with_addresses_start(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 5)
        sorted_keys = sorted(
            (msg['timestamp'], msg['from_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        index_page = yield self.store.batch_inbound_keys_with_addresses(
            batch_id, max_results=6, start=all_keys[1][1])
        self.assertEqual(list(index_page), all_keys[1:])

    @inlineCallbacks
    def test_batch_inbound_keys_with_addresses_end(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 5)
        sorted_keys = sorted(
            (msg['timestamp'], msg['from_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        index_page = yield self.store.batch_inbound_keys_with_addresses(
            batch_id, max_results=6, end=all_keys[-2][1])
        self.assertEqual(list(index_page), all_keys[:-1])

    @inlineCallbacks
    def test_batch_inbound_keys_with_addresses_range(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 5)
        sorted_keys = sorted(
            (msg['timestamp'], msg['from_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        index_page = yield self.store.batch_inbound_keys_with_addresses(
            batch_id, max_results=6, start=all_keys[1][1], end=all_keys[-2][1])
        self.assertEqual(list(index_page), all_keys[1:-1])

    @inlineCallbacks
    def test_batch_outbound_keys_with_addresses(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 10)
        sorted_keys = sorted(
            (msg['timestamp'], msg['to_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        first_page = yield self.store.batch_outbound_keys_with_addresses(
            batch_id, max_results=6)

        results = list(first_page)
        self.assertEqual(len(results), 6)
        self.assertEqual(first_page.has_next_page(), True)

        next_page = yield first_page.next_page()
        results.extend(next_page)
        self.assertEqual(len(results), 10)
        self.assertEqual(next_page.has_next_page(), False)

        self.assertEqual(results, all_keys)

    @inlineCallbacks
    def test_batch_outbound_keys_with_addresses_start(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 5)
        sorted_keys = sorted(
            (msg['timestamp'], msg['to_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        index_page = yield self.store.batch_outbound_keys_with_addresses(
            batch_id, max_results=6, start=all_keys[1][1])
        self.assertEqual(list(index_page), all_keys[1:])

    @inlineCallbacks
    def test_batch_outbound_keys_with_addresses_end(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 5)
        sorted_keys = sorted(
            (msg['timestamp'], msg['to_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        index_page = yield self.store.batch_outbound_keys_with_addresses(
            batch_id, max_results=6, end=all_keys[-2][1])
        self.assertEqual(list(index_page), all_keys[:-1])

    @inlineCallbacks
    def test_batch_outbound_keys_with_addresses_range(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 5)
        sorted_keys = sorted(
            (msg['timestamp'], msg['to_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        index_page = yield self.store.batch_outbound_keys_with_addresses(
            batch_id, max_results=6, start=all_keys[1][1], end=all_keys[-2][1])
        self.assertEqual(list(index_page), all_keys[1:-1])

    @inlineCallbacks
    def test_batch_inbound_keys_with_addresses_reverse(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 10)
        sorted_keys = sorted(
            [(zero_ms(msg['timestamp']), msg['from_addr'], msg['message_id'])
             for msg in messages], reverse=True)
        all_keys = [(key, timestamp, addr)
                    for (timestamp, addr, key) in sorted_keys]

        page = yield self.store.batch_inbound_keys_with_addresses_reverse(
            batch_id, max_results=6)

        results = list(page)
        self.assertEqual(len(results), 6)
        self.assertEqual(page.has_next_page(), True)

        next_page = yield page.next_page()
        results.extend(next_page)
        self.assertEqual(len(results), 10)
        self.assertEqual(next_page.has_next_page(), False)

        self.assertEqual(results, all_keys)

    @inlineCallbacks
    def test_batch_inbound_keys_with_addresses_reverse_start(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 5)
        sorted_keys = sorted(
            [(zero_ms(msg['timestamp']), msg['from_addr'], msg['message_id'])
             for msg in messages], reverse=True)
        all_keys = [(key, timestamp, addr)
                    for (timestamp, addr, key) in sorted_keys]

        page = yield self.store.batch_inbound_keys_with_addresses_reverse(
            batch_id, max_results=6, start=all_keys[-2][1])
        self.assertEqual(list(page), all_keys[:-1])

    @inlineCallbacks
    def test_batch_inbound_keys_with_addresses_reverse_end(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 5)
        sorted_keys = sorted(
            [(zero_ms(msg['timestamp']), msg['from_addr'], msg['message_id'])
             for msg in messages], reverse=True)
        all_keys = [(key, timestamp, addr)
                    for (timestamp, addr, key) in sorted_keys]

        page = yield self.store.batch_inbound_keys_with_addresses_reverse(
            batch_id, max_results=6, end=all_keys[1][1])
        self.assertEqual(list(page), all_keys[1:])

    @inlineCallbacks
    def test_batch_inbound_keys_with_addresses_reverse_range(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_inbound_messages(batch_id, 5)
        sorted_keys = sorted(
            [(zero_ms(msg['timestamp']), msg['from_addr'], msg['message_id'])
             for msg in messages], reverse=True)
        all_keys = [(key, timestamp, addr)
                    for (timestamp, addr, key) in sorted_keys]

        page = yield self.store.batch_inbound_keys_with_addresses_reverse(
            batch_id, max_results=6, start=all_keys[-2][1], end=all_keys[1][1])
        self.assertEqual(list(page), all_keys[1:-1])

    @inlineCallbacks
    def test_batch_outbound_keys_with_addresses_reverse(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 10)
        sorted_keys = sorted(
            [(zero_ms(msg['timestamp']), msg['to_addr'], msg['message_id'])
             for msg in messages], reverse=True)
        all_keys = [(key, timestamp, addr)
                    for (timestamp, addr, key) in sorted_keys]

        page = yield self.store.batch_outbound_keys_with_addresses_reverse(
            batch_id, max_results=6)

        results = list(page)
        self.assertEqual(len(results), 6)
        self.assertEqual(page.has_next_page(), True)

        next_page = yield page.next_page()
        results.extend(next_page)
        self.assertEqual(len(results), 10)
        self.assertEqual(next_page.has_next_page(), False)

        self.assertEqual(results, all_keys)

    @inlineCallbacks
    def test_batch_outbound_keys_with_addresses_reverse_start(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 5)
        sorted_keys = sorted(
            [(zero_ms(msg['timestamp']), msg['to_addr'], msg['message_id'])
             for msg in messages], reverse=True)
        all_keys = [(key, timestamp, addr)
                    for (timestamp, addr, key) in sorted_keys]

        page = yield self.store.batch_outbound_keys_with_addresses_reverse(
            batch_id, max_results=6, start=all_keys[-2][1])
        self.assertEqual(list(page), all_keys[:-1])

    @inlineCallbacks
    def test_batch_outbound_keys_with_addresses_reverse_end(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 5)
        sorted_keys = sorted(
            [(zero_ms(msg['timestamp']), msg['to_addr'], msg['message_id'])
             for msg in messages], reverse=True)
        all_keys = [(key, timestamp, addr)
                    for (timestamp, addr, key) in sorted_keys]

        page = yield self.store.batch_outbound_keys_with_addresses_reverse(
            batch_id, max_results=6, end=all_keys[1][1])
        self.assertEqual(list(page), all_keys[1:])

    @inlineCallbacks
    def test_batch_outbound_keys_with_addresses_reverse_range(self):
        batch_id = yield self.store.batch_start([('pool', 'tag')])
        messages = yield self.create_outbound_messages(batch_id, 5)
        sorted_keys = sorted(
            [(zero_ms(msg['timestamp']), msg['to_addr'], msg['message_id'])
             for msg in messages], reverse=True)
        all_keys = [(key, timestamp, addr)
                    for (timestamp, addr, key) in sorted_keys]

        page = yield self.store.batch_outbound_keys_with_addresses_reverse(
            batch_id, max_results=6, start=all_keys[-2][1], end=all_keys[1][1])
        self.assertEqual(list(page), all_keys[1:-1])

    @inlineCallbacks
    def test_message_event_keys_with_statuses(self):
        """
        Event keys and statuses for a message can be retrieved by index.
        """
        msg_id, msg, batch_id = yield self._create_outbound()

        ack = self.msg_helper.make_ack(msg)
        yield self.store.add_event(ack)
        drs = []
        for status in TransportEvent.DELIVERY_STATUSES:
            dr = self.msg_helper.make_delivery_report(
                msg, delivery_status=status)
            drs.append(dr)
            yield self.store.add_event(dr)

        mk_tuple = lambda e, status: (
            e["event_id"], format_vumi_date(e["timestamp"]), status)

        all_keys = [mk_tuple(ack, "ack")] + [
            mk_tuple(e, "delivery_report.%s" % (e["delivery_status"],))
            for e in drs]

        first_page = yield self.store.message_event_keys_with_statuses(
            msg_id, max_results=3)

        results = list(first_page)
        self.assertEqual(len(results), 3)
        self.assertEqual(first_page.has_next_page(), True)

        next_page = yield first_page.next_page()
        results.extend(next_page)
        self.assertEqual(len(results), 4)
        self.assertEqual(next_page.has_next_page(), False)

        self.assertEqual(results, all_keys)

    @inlineCallbacks
    def test_batch_inbound_stats(self):
        """
        batch_inbound_stats returns total and unique address counts for the
        whole batch if no time range is specified.
        """
        batch_id = yield self.store.batch_start([('pool', 'tag')])

        now = datetime.now()
        start_3 = now - timedelta(5)
        start_2 = now - timedelta(35)
        yield self.create_inbound_messages(
            batch_id, 5, start_timestamp=now, from_addr=u'00005')
        yield self.create_inbound_messages(
            batch_id, 3, start_timestamp=start_3, from_addr=u'00003')
        yield self.create_inbound_messages(
            batch_id, 2, start_timestamp=start_2, from_addr=u'00002')

        inbound_stats = yield self.store.batch_inbound_stats(
            batch_id, max_results=6)
        self.assertEqual(inbound_stats, {"total": 10, "unique_addresses": 3})

    @inlineCallbacks
    def test_batch_inbound_stats_start(self):
        """
        batch_inbound_stats returns total and unique address counts for all
        messages newer than the start date if only the start date is specified.
        """
        batch_id = yield self.store.batch_start([('pool', 'tag')])

        now = datetime.now()
        start_3 = now - timedelta(5)
        start_2 = now - timedelta(35)
        messages_5 = yield self.create_inbound_messages(
            batch_id, 5, start_timestamp=now, from_addr=u'00005')
        messages_3 = yield self.create_inbound_messages(
            batch_id, 3, start_timestamp=start_3, from_addr=u'00003')
        messages_2 = yield self.create_inbound_messages(
            batch_id, 2, start_timestamp=start_2, from_addr=u'00002')
        messages = messages_5 + messages_3 + messages_2

        sorted_keys = sorted(
            (msg['timestamp'], msg['from_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        inbound_stats_1 = yield self.store.batch_inbound_stats(
            batch_id, start=all_keys[2][1])

        self.assertEqual(inbound_stats_1, {"total": 8, "unique_addresses": 3})

        inbound_stats_2 = yield self.store.batch_inbound_stats(
            batch_id, start=all_keys[6][1])

        self.assertEqual(inbound_stats_2, {"total": 4, "unique_addresses": 2})

    @inlineCallbacks
    def test_batch_inbound_stats_end(self):
        """
        batch_inbound_stats returns total and unique address counts for all
        messages older than the end date if only the end date is specified.
        """
        batch_id = yield self.store.batch_start([('pool', 'tag')])

        now = datetime.now()
        start_3 = now - timedelta(5)
        start_2 = now - timedelta(35)
        messages_5 = yield self.create_inbound_messages(
            batch_id, 5, start_timestamp=now, from_addr=u'00005')
        messages_3 = yield self.create_inbound_messages(
            batch_id, 3, start_timestamp=start_3, from_addr=u'00003')
        messages_2 = yield self.create_inbound_messages(
            batch_id, 2, start_timestamp=start_2, from_addr=u'00002')
        messages = messages_5 + messages_3 + messages_2

        sorted_keys = sorted(
            (msg['timestamp'], msg['from_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        inbound_stats_1 = yield self.store.batch_inbound_stats(
            batch_id, end=all_keys[-3][1])

        self.assertEqual(inbound_stats_1, {"total": 8, "unique_addresses": 3})

        inbound_stats_2 = yield self.store.batch_inbound_stats(
            batch_id, end=all_keys[-7][1])

        self.assertEqual(inbound_stats_2, {"total": 4, "unique_addresses": 2})

    @inlineCallbacks
    def test_batch_inbound_stats_range(self):
        """
        batch_inbound_stats returns total and unique address counts for all
        messages newer than the start date and older than the end date if both
        are specified.
        """
        batch_id = yield self.store.batch_start([('pool', 'tag')])

        now = datetime.now()
        start_3 = now - timedelta(5)
        start_2 = now - timedelta(35)
        messages_5 = yield self.create_inbound_messages(
            batch_id, 5, start_timestamp=now, from_addr=u'00005')
        messages_3 = yield self.create_inbound_messages(
            batch_id, 3, start_timestamp=start_3, from_addr=u'00003')
        messages_2 = yield self.create_inbound_messages(
            batch_id, 2, start_timestamp=start_2, from_addr=u'00002')
        messages = messages_5 + messages_3 + messages_2

        sorted_keys = sorted(
            (msg['timestamp'], msg['from_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        inbound_stats_1 = yield self.store.batch_inbound_stats(
            batch_id, start=all_keys[2][1], end=all_keys[-3][1])

        self.assertEqual(inbound_stats_1, {"total": 6, "unique_addresses": 3})

        inbound_stats_2 = yield self.store.batch_inbound_stats(
            batch_id, start=all_keys[2][1], end=all_keys[-7][1])

        self.assertEqual(inbound_stats_2, {"total": 2, "unique_addresses": 2})

    @inlineCallbacks
    def test_batch_outbound_stats(self):
        """
        batch_outbound_stats returns total and unique address counts for the
        whole batch if no time range is specified.
        """
        batch_id = yield self.store.batch_start([('pool', 'tag')])

        now = datetime.now()
        start_3 = now - timedelta(5)
        start_2 = now - timedelta(35)
        yield self.create_outbound_messages(
            batch_id, 5, start_timestamp=now, to_addr=u'00005')
        yield self.create_outbound_messages(
            batch_id, 3, start_timestamp=start_3, to_addr=u'00003')
        yield self.create_outbound_messages(
            batch_id, 2, start_timestamp=start_2, to_addr=u'00002')

        outbound_stats = yield self.store.batch_outbound_stats(
            batch_id, max_results=6)
        self.assertEqual(outbound_stats, {"total": 10, "unique_addresses": 3})

    @inlineCallbacks
    def test_batch_outbound_stats_start(self):
        """
        batch_outbound_stats returns total and unique address counts for all
        messages newer than the start date if only the start date is specified.
        """
        batch_id = yield self.store.batch_start([('pool', 'tag')])

        now = datetime.now()
        start_3 = now - timedelta(5)
        start_2 = now - timedelta(35)
        messages_5 = yield self.create_outbound_messages(
            batch_id, 5, start_timestamp=now, to_addr=u'00005')
        messages_3 = yield self.create_outbound_messages(
            batch_id, 3, start_timestamp=start_3, to_addr=u'00003')
        messages_2 = yield self.create_outbound_messages(
            batch_id, 2, start_timestamp=start_2, to_addr=u'00002')
        messages = messages_5 + messages_3 + messages_2

        sorted_keys = sorted(
            (msg['timestamp'], msg['to_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        outbound_stats_1 = yield self.store.batch_outbound_stats(
            batch_id, start=all_keys[2][1])

        self.assertEqual(outbound_stats_1, {"total": 8, "unique_addresses": 3})

        outbound_stats_2 = yield self.store.batch_outbound_stats(
            batch_id, start=all_keys[6][1])

        self.assertEqual(outbound_stats_2, {"total": 4, "unique_addresses": 2})

    @inlineCallbacks
    def test_batch_outbound_stats_end(self):
        """
        batch_outbound_stats returns total and unique address counts for all
        messages older than the end date if only the end date is specified.
        """
        batch_id = yield self.store.batch_start([('pool', 'tag')])

        now = datetime.now()
        start_3 = now - timedelta(5)
        start_2 = now - timedelta(35)
        messages_5 = yield self.create_outbound_messages(
            batch_id, 5, start_timestamp=now, to_addr=u'00005')
        messages_3 = yield self.create_outbound_messages(
            batch_id, 3, start_timestamp=start_3, to_addr=u'00003')
        messages_2 = yield self.create_outbound_messages(
            batch_id, 2, start_timestamp=start_2, to_addr=u'00002')
        messages = messages_5 + messages_3 + messages_2

        sorted_keys = sorted(
            (msg['timestamp'], msg['to_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        outbound_stats_1 = yield self.store.batch_outbound_stats(
            batch_id, end=all_keys[-3][1])

        self.assertEqual(outbound_stats_1, {"total": 8, "unique_addresses": 3})

        outbound_stats_2 = yield self.store.batch_outbound_stats(
            batch_id, end=all_keys[-7][1])

        self.assertEqual(outbound_stats_2, {"total": 4, "unique_addresses": 2})

    @inlineCallbacks
    def test_batch_outbound_stats_range(self):
        """
        batch_outbound_stats returns total and unique address counts for all
        messages newer than the start date and older than the end date if both
        are specified.
        """
        batch_id = yield self.store.batch_start([('pool', 'tag')])

        now = datetime.now()
        start_3 = now - timedelta(5)
        start_2 = now - timedelta(35)
        messages_5 = yield self.create_outbound_messages(
            batch_id, 5, start_timestamp=now, to_addr=u'00005')
        messages_3 = yield self.create_outbound_messages(
            batch_id, 3, start_timestamp=start_3, to_addr=u'00003')
        messages_2 = yield self.create_outbound_messages(
            batch_id, 2, start_timestamp=start_2, to_addr=u'00002')
        messages = messages_5 + messages_3 + messages_2

        sorted_keys = sorted(
            (msg['timestamp'], msg['to_addr'], msg['message_id'])
            for msg in messages)
        all_keys = [(key, format_vumi_date(timestamp), addr)
                    for (timestamp, addr, key) in sorted_keys]

        outbound_stats_1 = yield self.store.batch_outbound_stats(
            batch_id, start=all_keys[2][1], end=all_keys[-3][1])

        self.assertEqual(outbound_stats_1, {"total": 6, "unique_addresses": 3})

        outbound_stats_2 = yield self.store.batch_outbound_stats(
            batch_id, start=all_keys[2][1], end=all_keys[-7][1])

        self.assertEqual(outbound_stats_2, {"total": 2, "unique_addresses": 2})


class TestMessageStoreCache(TestMessageStoreBase):

    def clear_cache(self, message_store):
        # FakeRedis provides a flushdb() function but TxRedisManager doesn't
        # and I'm not sure what the intended behaviour of flushdb on a
        # submanager is
        return message_store.cache.redis._purge_all()

    @inlineCallbacks
    def test_cache_batch_start(self):
        batch_id = yield self.store.batch_start([("poolA", "tag1")])
        self.assertTrue((yield self.store.cache.batch_exists(batch_id)))
        self.assertTrue(batch_id in (yield self.store.cache.get_batch_ids()))

    @inlineCallbacks
    def test_cache_add_outbound_message(self):
        msg_id, msg, batch_id = yield self._create_outbound()
        [cached_msg_id] = (
            yield self.store.cache.get_outbound_message_keys(batch_id))
        cached_to_addrs = yield self.store.cache.get_to_addrs(batch_id)
        self.assertEqual(msg_id, cached_msg_id)
        # NOTE: This functionality is disabled for now.
        # self.assertEqual([msg['to_addr']], cached_to_addrs)
        self.assertEqual([], cached_to_addrs)

    @inlineCallbacks
    def test_cache_add_inbound_message(self):
        msg_id, msg, batch_id = yield self._create_inbound()
        [cached_msg_id] = (
            yield self.store.cache.get_inbound_message_keys(batch_id))
        cached_from_addrs = yield self.store.cache.get_from_addrs(batch_id)
        self.assertEqual(msg_id, cached_msg_id)
        # NOTE: This functionality is disabled for now.
        # self.assertEqual([msg['from_addr']], cached_from_addrs)
        self.assertEqual([], cached_from_addrs)

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
        recon_msg = self.msg_helper.make_outbound("foo")
        yield self.store.cache.add_outbound_message(batch_id, recon_msg)

        # Default reconciliation delta should return True
        self.assertTrue((yield self.store.needs_reconciliation(batch_id)))
        # More liberal reconciliation delta should return False
        self.assertFalse((
            yield self.store.needs_reconciliation(batch_id, delta=0.1)))

    @inlineCallbacks
    def test_reconcile_cache(self):
        cache = self.store.cache
        batch_id = yield self.store.batch_start([("pool", "tag")])

        # Store via message_store
        yield self.create_inbound_messages(batch_id, 1, from_addr='from1')
        yield self.create_inbound_messages(batch_id, 2, from_addr='from2')
        yield self.create_inbound_messages(batch_id, 3, from_addr='from3')

        outbound_messages = []
        outbound_messages.extend((yield self.create_outbound_messages(
            batch_id, 4, to_addr='to1')))
        outbound_messages.extend((yield self.create_outbound_messages(
            batch_id, 6, to_addr='to2')))

        for msg in outbound_messages:
            ack = self.msg_helper.make_ack(msg)
            yield self.store.add_event(ack)

        yield self.clear_cache(self.store)
        batch_status = yield self.store.batch_status(batch_id)
        self.assertEqual(batch_status, {})
        # Default reconciliation delta should return True
        self.assertTrue((yield self.store.needs_reconciliation(batch_id)))
        yield self.store.reconcile_cache(batch_id)
        # Reconciliation check should return False after recon.
        self.assertFalse((yield self.store.needs_reconciliation(batch_id)))
        self.assertFalse(
            (yield self.store.needs_reconciliation(batch_id, delta=0)))

        inbound_count = yield cache.count_inbound_message_keys(batch_id)
        self.assertEqual(inbound_count, 6)
        outbound_count = yield cache.count_outbound_message_keys(batch_id)
        self.assertEqual(outbound_count, 10)

        inbound_uniques = yield cache.count_from_addrs(batch_id)
        self.assertEqual(inbound_uniques, 3)
        outbound_uniques = yield cache.count_to_addrs(batch_id)
        self.assertEqual(outbound_uniques, 2)

        batch_status = yield self.store.batch_status(batch_id)
        self.assertEqual(batch_status['ack'], 10)
        self.assertEqual(batch_status['sent'], 10)

    @inlineCallbacks
    def test_reconcile_cache_with_old_and_new_messages(self):
        """
        If we're reconciling a batch that contains messages older than the
        truncation threshold and newer than the start of the recon, we still
        end up with the correct numbers.
        """
        cache = self.store.cache
        cache.TRUNCATE_MESSAGE_KEY_COUNT_AT = 5
        batch_id = yield self.store.batch_start([("pool", "tag")])

        # Store via message_store
        inbound_messages = []
        inbound_messages.extend((yield self.create_inbound_messages(
            batch_id, 1, from_addr='from1')))
        inbound_messages.extend((yield self.create_inbound_messages(
            batch_id, 2, from_addr='from2')))
        inbound_messages.extend((yield self.create_inbound_messages(
            batch_id, 3, from_addr='from3')))

        outbound_messages = []
        outbound_messages.extend((yield self.create_outbound_messages(
            batch_id, 4, to_addr='to1')))
        outbound_messages.extend((yield self.create_outbound_messages(
            batch_id, 6, to_addr='to2')))

        for msg in outbound_messages:
            ack = self.msg_helper.make_ack(msg)
            yield self.store.add_event(ack)
            dr = self.msg_helper.make_delivery_report(
                msg, delivery_status="delivered")
            yield self.store.add_event(dr)

        # We want one message newer than the start of the recon, and they're
        # ordered from newest to oldest.
        start_timestamp = format_vumi_date(inbound_messages[1]["timestamp"])

        yield self.store.reconcile_cache(batch_id, start_timestamp)

        inbound_count = yield cache.count_inbound_message_keys(batch_id)
        self.assertEqual(inbound_count, 6)
        outbound_count = yield cache.count_outbound_message_keys(batch_id)
        self.assertEqual(outbound_count, 10)

        inbound_uniques = yield self.store.cache.count_from_addrs(batch_id)
        self.assertEqual(inbound_uniques, 3)
        outbound_uniques = yield self.store.cache.count_to_addrs(batch_id)
        self.assertEqual(outbound_uniques, 2)

        batch_status = yield self.store.batch_status(batch_id)
        self.assertEqual(batch_status["sent"], 10)
        self.assertEqual(batch_status["ack"], 10)
        self.assertEqual(batch_status["delivery_report"], 10)
        self.assertEqual(batch_status["delivery_report.delivered"], 10)

    @inlineCallbacks
    def test_reconcile_cache_and_switch_to_counters(self):
        batch_id = yield self.store.batch_start([("pool", "tag")])
        cache = self.store.cache

        # Clear the cache and restart the batch without counters.
        yield cache.clear_batch(batch_id)
        yield cache.batch_start(batch_id, use_counters=False)

        # Store via message_store
        yield self.create_inbound_messages(batch_id, 1, from_addr='from1')
        yield self.create_inbound_messages(batch_id, 2, from_addr='from2')
        yield self.create_inbound_messages(batch_id, 3, from_addr='from3')

        outbound_messages = []
        outbound_messages.extend((yield self.create_outbound_messages(
            batch_id, 4, to_addr='to1')))
        outbound_messages.extend((yield self.create_outbound_messages(
            batch_id, 6, to_addr='to2')))

        for msg in outbound_messages:
            ack = self.msg_helper.make_ack(msg)
            yield self.store.add_event(ack)

        # This will fail if we're using counter-based events with a ZSET.
        events_scard = yield cache.redis.scard(cache.event_key(batch_id))
        # HACK: We're not tracking these in the SET anymore.
        #       See HACK comment in message_store_cache.py.
        # self.assertEqual(events_scard, 10)
        self.assertEqual(events_scard, 0)

        yield self.clear_cache(self.store)
        batch_status = yield self.store.batch_status(batch_id)
        self.assertEqual(batch_status, {})
        # Default reconciliation delta should return True
        self.assertTrue((yield self.store.needs_reconciliation(batch_id)))
        yield self.store.reconcile_cache(batch_id)
        # Reconciliation check should return False after recon.
        self.assertFalse((yield self.store.needs_reconciliation(batch_id)))
        self.assertFalse(
            (yield self.store.needs_reconciliation(batch_id, delta=0)))

        inbound_count = yield cache.count_inbound_message_keys(batch_id)
        self.assertEqual(inbound_count, 6)
        outbound_count = yield cache.count_outbound_message_keys(batch_id)
        self.assertEqual(outbound_count, 10)

        inbound_uniques = yield self.store.cache.count_from_addrs(batch_id)
        self.assertEqual(inbound_uniques, 3)
        outbound_uniques = yield self.store.cache.count_to_addrs(batch_id)
        self.assertEqual(outbound_uniques, 2)

        batch_status = yield self.store.batch_status(batch_id)
        self.assertEqual(batch_status['ack'], 10)
        self.assertEqual(batch_status['sent'], 10)

        # This will fail if we're using old-style events with a SET.
        events_zcard = yield cache.redis.zcard(cache.event_key(batch_id))
        self.assertEqual(events_zcard, 10)

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
        in_progress = yield self.store.cache.is_query_in_progress(
            batch_id, token)
        self.assertEqual(len(keys), 10)
        self.assertEqual(
            10, (yield self.store.count_keys_for_token(batch_id, token)))
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
        in_progress = yield self.store.cache.is_query_in_progress(
            batch_id, token)
        self.assertEqual(len(keys), 10)
        self.assertEqual(
            10, (yield self.store.count_keys_for_token(batch_id, token)))
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


class TestMigrationFunctions(TestMessageStoreBase):

    @inlineCallbacks
    def test_add_batches_to_event_no_batches(self):
        """
        If the stored event has no batches, they're looked up from the outbound
        message and added to the event.
        """
        msg_id, msg, batch_id = yield self._create_outbound()

        ack = self.msg_helper.make_ack(msg)
        ack_id = ack['event_id']
        yield self.store.add_event(ack, batch_ids=[])

        event = yield self.store.events.load(ack_id)
        self.assertEqual(event.batches.keys(), [])

        updated = yield add_batches_to_event(event)
        self.assertEqual(updated, True)
        self.assertEqual(event.batches.keys(), [batch_id])

    @inlineCallbacks
    def test_add_batches_to_event_with_batches(self):
        """
        If the stored event already has batches, we do nothing.
        """
        msg_id, msg, batch_id = yield self._create_outbound()

        ack = self.msg_helper.make_ack(msg)
        ack_id = ack['event_id']
        yield self.store.add_event(ack, batch_ids=[batch_id])

        event = yield self.store.events.load(ack_id)
        self.assertEqual(event.batches.keys(), [batch_id])

        updated = yield add_batches_to_event(event)
        self.assertEqual(updated, False)
        self.assertEqual(event.batches.keys(), [batch_id])
