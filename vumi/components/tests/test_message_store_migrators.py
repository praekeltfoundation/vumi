"""Tests for go.components.message_store_migrators."""

from twisted.internet.defer import inlineCallbacks

from vumi.tests.helpers import (
    VumiTestCase, MessageHelper, PersistenceHelper, import_skip,
)

try:
    from vumi.components.tests.message_store_old_models import (
        OutboundMessageVNone, InboundMessageVNone, BatchVNone,
        OutboundMessageV1, InboundMessageV1)
    from vumi.components.message_store import (
        OutboundMessage as OutboundMessageV2,
        InboundMessage as InboundMessageV2)
    riak_import_error = None
except ImportError, e:
    riak_import_error = e


class TestMigratorBase(VumiTestCase):
    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        if riak_import_error is not None:
            import_skip(riak_import_error, 'riak')
        self.manager = self.persistence_helper.get_riak_manager()
        self.msg_helper = self.add_helper(MessageHelper())


class TestOutboundMessageMigrator(TestMigratorBase):
    @inlineCallbacks
    def setUp(self):
        yield super(TestOutboundMessageMigrator, self).setUp()
        self.outbound_vnone = self.manager.proxy(OutboundMessageVNone)
        self.outbound_v1 = self.manager.proxy(OutboundMessageV1)
        self.outbound_v2 = self.manager.proxy(OutboundMessageV2)
        self.batch_vnone = self.manager.proxy(BatchVNone)

    @inlineCallbacks
    def test_migrate_vnone_to_v1(self):
        msg = self.msg_helper.make_outbound("outbound")
        old_batch = self.batch_vnone(key=u"batch-1")
        old_record = self.outbound_vnone(msg["message_id"],
                                         msg=msg, batch=old_batch)
        yield old_record.save()
        new_record = yield self.outbound_v1.load(old_record.key)
        self.assertEqual(new_record.msg, msg)
        self.assertEqual(new_record.batches.keys(), [old_batch.key])

    @inlineCallbacks
    def test_migrate_vnone_to_v1_without_batch(self):
        msg = self.msg_helper.make_outbound("outbound")
        old_record = self.outbound_vnone(msg["message_id"],
                                         msg=msg, batch=None)
        yield old_record.save()
        new_record = yield self.outbound_v1.load(old_record.key)
        self.assertEqual(new_record.msg, msg)
        self.assertEqual(new_record.batches.keys(), [])

    @inlineCallbacks
    def test_migrate_v1_to_v2_no_batches(self):
        msg = self.msg_helper.make_outbound("outbound")
        old_record = self.outbound_v1(msg["message_id"], msg=msg)
        yield old_record.save()
        new_record = yield self.outbound_v2.load(old_record.key)
        self.assertEqual(new_record.msg, msg)
        self.assertEqual(new_record.batches.keys(), [])

        # The migration doesn't set the new fields and indexes, that only
        # happens at save time.
        self.assertEqual(set(new_record.batches_with_timestamps), set([]))
        self.assertEqual(new_record._riak_object.get_indexes(), set([]))

        yield new_record.save()
        self.assertEqual(set(new_record.batches_with_timestamps), set([]))
        self.assertEqual(new_record._riak_object.get_indexes(), set([]))

    @inlineCallbacks
    def test_migrate_v1_to_v2_one_batch(self):
        msg = self.msg_helper.make_outbound("outbound")
        old_batch = self.batch_vnone(key=u"batch-1")
        old_record = self.outbound_v1(msg["message_id"], msg=msg)
        old_record.batches.add_key(old_batch.key)
        yield old_record.save()
        new_record = yield self.outbound_v2.load(old_record.key)
        self.assertEqual(new_record.msg, msg)
        self.assertEqual(new_record.batches.keys(), [old_batch.key])

        # The migration doesn't set the new fields and indexes, that only
        # happens at save time.

        self.assertEqual(set(new_record.batches_with_timestamps), set([]))
        self.assertEqual(new_record._riak_object.get_indexes(), set([
            ('batches_bin', "batch-1"),
        ]))

        yield new_record.save()
        batches_with_timestamps = [
            "batch-1$%s" % (msg['timestamp'],),
        ]
        self.assertEqual(
            set(new_record.batches_with_timestamps),
            set(batches_with_timestamps))
        self.assertEqual(new_record._riak_object.get_indexes(), set([
            ('batches_bin', "batch-1"),
        ] + [('batches_with_timestamps_bin', value)
             for value in batches_with_timestamps]))

    @inlineCallbacks
    def test_migrate_v1_to_v2_two_batches(self):
        msg = self.msg_helper.make_outbound("outbound")
        batch_1 = self.batch_vnone(key=u"batch-1")
        batch_2 = self.batch_vnone(key=u"batch-2")
        old_record = self.outbound_v1(msg["message_id"], msg=msg)
        old_record.batches.add_key(batch_1.key)
        old_record.batches.add_key(batch_2.key)
        yield old_record.save()
        new_record = yield self.outbound_v2.load(old_record.key)
        self.assertEqual(new_record.msg, msg)
        self.assertEqual(new_record.batches.keys(), [batch_1.key, batch_2.key])

        # The migration doesn't set the new fields and indexes, that only
        # happens at save time.
        self.assertEqual(new_record._riak_object.get_indexes(), set([
            ('batches_bin', "batch-1"),
            ('batches_bin', "batch-2"),
        ]))
        yield new_record.save()
        batches_with_timestamps = [
            "batch-1$%s" % (msg['timestamp'],),
            "batch-2$%s" % (msg['timestamp'],),
        ]
        self.assertEqual(new_record._riak_object.get_indexes(), set([
            ('batches_bin', "batch-1"),
            ('batches_bin', "batch-2"),
        ] + [('batches_with_timestamps_bin', value)
             for value in batches_with_timestamps]))


class TestInboundMessageMigrator(TestMigratorBase):

    @inlineCallbacks
    def setUp(self):
        yield super(TestInboundMessageMigrator, self).setUp()
        self.inbound_vnone = self.manager.proxy(InboundMessageVNone)
        self.inbound_v1 = self.manager.proxy(InboundMessageV1)
        self.inbound_v2 = self.manager.proxy(InboundMessageV2)
        self.batch_vnone = self.manager.proxy(BatchVNone)

    @inlineCallbacks
    def test_migrate_vnone_to_v1(self):
        msg = self.msg_helper.make_inbound("inbound")
        old_batch = self.batch_vnone(key=u"batch-1")
        old_record = self.inbound_vnone(msg["message_id"],
                                        msg=msg, batch=old_batch)
        yield old_record.save()
        new_record = yield self.inbound_v1.load(old_record.key)
        self.assertEqual(new_record.msg, msg)
        self.assertEqual(new_record.batches.keys(), [old_batch.key])

    @inlineCallbacks
    def test_migrate_vnone_to_v1_without_batch(self):
        msg = self.msg_helper.make_inbound("inbound")
        old_record = self.inbound_vnone(msg["message_id"],
                                        msg=msg, batch=None)
        yield old_record.save()
        new_record = yield self.inbound_v1.load(old_record.key)
        self.assertEqual(new_record.msg, msg)
        self.assertEqual(new_record.batches.keys(), [])

    @inlineCallbacks
    def test_migrate_v1_to_v2_no_batches(self):
        msg = self.msg_helper.make_inbound("inbound")
        old_record = self.inbound_v1(msg["message_id"], msg=msg)
        yield old_record.save()
        new_record = yield self.inbound_v2.load(old_record.key)
        self.assertEqual(new_record.msg, msg)
        self.assertEqual(new_record.batches.keys(), [])

        # The migration doesn't set the new fields and indexes, that only
        # happens at save time.
        self.assertEqual(set(new_record.batches_with_timestamps), set([]))
        self.assertEqual(new_record._riak_object.get_indexes(), set([]))

        yield new_record.save()
        self.assertEqual(set(new_record.batches_with_timestamps), set([]))
        self.assertEqual(new_record._riak_object.get_indexes(), set([]))

    @inlineCallbacks
    def test_migrate_v1_to_v2_one_batch(self):
        msg = self.msg_helper.make_inbound("inbound")
        old_batch = self.batch_vnone(key=u"batch-1")
        old_record = self.inbound_v1(msg["message_id"], msg=msg)
        old_record.batches.add_key(old_batch.key)
        yield old_record.save()
        new_record = yield self.inbound_v2.load(old_record.key)
        self.assertEqual(new_record.msg, msg)
        self.assertEqual(new_record.batches.keys(), [old_batch.key])

        # The migration doesn't set the new fields and indexes, that only
        # happens at save time.

        self.assertEqual(set(new_record.batches_with_timestamps), set([]))
        self.assertEqual(new_record._riak_object.get_indexes(), set([
            ('batches_bin', "batch-1"),
        ]))

        yield new_record.save()
        batches_with_timestamps = [
            "batch-1$%s" % (msg['timestamp'],),
        ]
        self.assertEqual(
            set(new_record.batches_with_timestamps),
            set(batches_with_timestamps))
        self.assertEqual(new_record._riak_object.get_indexes(), set([
            ('batches_bin', "batch-1"),
        ] + [('batches_with_timestamps_bin', value)
             for value in batches_with_timestamps]))

    @inlineCallbacks
    def test_migrate_v1_to_v2_two_batches(self):
        msg = self.msg_helper.make_inbound("inbound")
        batch_1 = self.batch_vnone(key=u"batch-1")
        batch_2 = self.batch_vnone(key=u"batch-2")
        old_record = self.inbound_v1(msg["message_id"], msg=msg)
        old_record.batches.add_key(batch_1.key)
        old_record.batches.add_key(batch_2.key)
        yield old_record.save()
        new_record = yield self.inbound_v2.load(old_record.key)
        self.assertEqual(new_record.msg, msg)
        self.assertEqual(new_record.batches.keys(), [batch_1.key, batch_2.key])

        # The migration doesn't set the new fields and indexes, that only
        # happens at save time.
        self.assertEqual(new_record._riak_object.get_indexes(), set([
            ('batches_bin', "batch-1"),
            ('batches_bin', "batch-2"),
        ]))
        yield new_record.save()
        batches_with_timestamps = [
            "batch-1$%s" % (msg['timestamp'],),
            "batch-2$%s" % (msg['timestamp'],),
        ]
        self.assertEqual(new_record._riak_object.get_indexes(), set([
            ('batches_bin', "batch-1"),
            ('batches_bin', "batch-2"),
        ] + [('batches_with_timestamps_bin', value)
             for value in batches_with_timestamps]))
