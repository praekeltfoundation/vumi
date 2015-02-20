"""Tests for vumi.scripts.vumi_list_messages."""

import sys
from datetime import datetime, timedelta
from uuid import uuid4
from StringIO import StringIO

from twisted.internet.defer import inlineCallbacks
from twisted.python import usage

from vumi.components.message_store import MessageStore
from vumi.scripts.vumi_list_messages import MessageLister, Options, main
from vumi.tests.helpers import VumiTestCase, PersistenceHelper, MessageHelper


class StubbedMessageLister(MessageLister):
    def __init__(self, testcase, *args, **kwargs):
        self.testcase = testcase
        self.output = []
        super(StubbedMessageLister, self).__init__(*args, **kwargs)

    def emit(self, s):
        self.output.append(s)

    def get_riak_manager(self, riak_config):
        return self.testcase.get_sub_riak(riak_config)


class TestMessageLister(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True, is_sync=False))
        self.msg_helper = self.add_helper(MessageHelper())
        # Since we're never loading the actual objects, we can't detect
        # tombstones. Therefore, each test needs its own bucket prefix.
        config = self.persistence_helper.mk_config({})["riak_manager"].copy()
        config["bucket_prefix"] = "%s-%s" % (
            uuid4().hex, config["bucket_prefix"])
        self.riak_manager = self.persistence_helper.get_riak_manager(config)
        self.redis_manager = yield self.persistence_helper.get_redis_manager()
        self.mdb = MessageStore(self.riak_manager, self.redis_manager)
        self.expected_bucket_prefix = "bucket"
        self.default_args = [
            "-b", self.expected_bucket_prefix,
        ]

    def make_lister(self, args=None, batch=None, direction=None,
                    index_page_size=None):
        if args is None:
            args = self.default_args
        if batch is not None:
            args.extend(["--batch", batch])
        if direction is not None:
            args.extend(["--direction", direction])
        if index_page_size is not None:
            args.extend(["--index-page-size", str(index_page_size)])
        options = Options()
        options.parseOptions(args)
        return StubbedMessageLister(self, options)

    def get_sub_riak(self, config):
        self.assertEqual(config.get('bucket_prefix'),
                         self.expected_bucket_prefix)
        return self.riak_manager

    def make_inbound(self, batch_id, from_addr, timestamp=None):
        if timestamp is None:
            timestamp = datetime.utcnow()
        msg = self.msg_helper.make_inbound(
            None, from_addr=from_addr, timestamp=timestamp)
        d = self.mdb.add_inbound_message(msg, batch_id=batch_id)
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
        d.addCallback(
            lambda _: (timestamp_str, from_addr, msg["message_id"]))
        return d

    def make_outbound(self, batch_id, to_addr, timestamp=None):
        if timestamp is None:
            timestamp = datetime.utcnow()
        msg = self.msg_helper.make_outbound(
            None, to_addr=to_addr, timestamp=timestamp)
        d = self.mdb.add_outbound_message(msg, batch_id=batch_id)
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
        d.addCallback(
            lambda _: (timestamp_str, to_addr, msg["message_id"]))
        return d

    def test_batch_required(self):
        self.assertRaises(usage.UsageError, self.make_lister, [
            "--direction", "inbound",
            "-b", self.expected_bucket_prefix,
        ])

    def test_valid_direction_required(self):
        self.assertRaises(usage.UsageError, self.make_lister, [
            "--batch", "gingercoookies",
            "-b", self.expected_bucket_prefix,
        ])
        self.assertRaises(usage.UsageError, self.make_lister, [
            "--batch", "gingercoookies",
            "--direction", "widdershins",
            "-b", self.expected_bucket_prefix,
        ])

    def test_bucket_required(self):
        self.assertRaises(usage.UsageError, self.make_lister, [
            "--batch", "gingercoookies",
            "--direction", "inbound",
        ])

    @inlineCallbacks
    def test_main(self):
        """
        The lister runs via `main()`.
        """
        msg_data = yield self.make_inbound("gingercookies", "12345")
        self.patch(sys, "stdout", StringIO())
        yield main(
            None, "name",
            "--batch", "gingercookies",
            "--direction", "inbound",
            "-b", self.riak_manager.bucket_prefix)
        self.assertEqual(
            sys.stdout.getvalue(),
            "%s\n" % (",".join(msg_data),))

    @inlineCallbacks
    def test_list_inbound(self):
        """
        Inbound messages can be listed.
        """
        start = datetime.utcnow() - timedelta(seconds=10)
        msg_datas = [
            (yield self.make_inbound(
                "gingercookies", "1234%d" % i, start + timedelta(seconds=i)))
            for i in range(5)
        ]
        lister = self.make_lister(batch="gingercookies", direction="inbound")
        yield lister.run()
        self.assertEqual(
            lister.output, [",".join(msg_data) for msg_data in msg_datas])

    @inlineCallbacks
    def test_list_inbound_small_pages(self):
        """
        Inbound messages can be listed.
        """
        start = datetime.utcnow() - timedelta(seconds=10)
        msg_datas = [
            (yield self.make_inbound(
                "gingercookies", "1234%d" % i, start + timedelta(seconds=i)))
            for i in range(5)
        ]
        lister = self.make_lister(
            batch="gingercookies", direction="inbound", index_page_size=2)
        yield lister.run()
        self.assertEqual(
            lister.output, [",".join(msg_data) for msg_data in msg_datas])

    @inlineCallbacks
    def test_list_outbound(self):
        """
        Outbound messages can be listed.
        """
        start = datetime.utcnow() - timedelta(seconds=10)
        msg_datas = [
            (yield self.make_outbound(
                "gingercookies", "1234%d" % i, start + timedelta(seconds=i)))
            for i in range(5)
        ]
        lister = self.make_lister(batch="gingercookies", direction="outbound")
        yield lister.run()
        self.assertEqual(
            lister.output, [",".join(msg_data) for msg_data in msg_datas])
