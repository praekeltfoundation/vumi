"""Tests for vumi.scripts.vumi_model_migrator."""

import sys
from uuid import uuid4
from StringIO import StringIO

from twisted.internet.defer import inlineCallbacks
from twisted.python import usage

from vumi.persist.model import Model
from vumi.persist.fields import Unicode
from vumi.scripts.vumi_count_models import ModelCounter, Options, main
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


class SimpleModel(Model):
    VERSION = 1
    a = Unicode(index=True)
    even_odd = Unicode(index=True)


class StubbedModelCounter(ModelCounter):
    def __init__(self, testcase, *args, **kwargs):
        self.testcase = testcase
        self.output = []
        super(StubbedModelCounter, self).__init__(*args, **kwargs)

    def emit(self, s):
        self.output.append(s)

    def get_riak_manager(self, riak_config):
        return self.testcase.get_sub_riak(riak_config)


class TestModelCounter(VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True, is_sync=False))
        # Since we're never loading the actual objects, we can't detect
        # tombstones. Therefore, each test needs its own bucket prefix.
        config = self.persistence_helper.mk_config({})["riak_manager"].copy()
        config["bucket_prefix"] = "%s-%s" % (
            uuid4().hex, config["bucket_prefix"])
        self.riak_manager = self.persistence_helper.get_riak_manager(config)
        self.model = self.riak_manager.proxy(SimpleModel)
        self.model_cls_path = ".".join([
            SimpleModel.__module__, SimpleModel.__name__])
        self.expected_bucket_prefix = "bucket"
        self.default_args = [
            "-m", self.model_cls_path,
            "-b", self.expected_bucket_prefix,
        ]

    def make_counter(self, args=None, index_page_size=None, index_field=None,
                     index_value=None, index_value_end=None,
                     index_value_regex=None):
        if args is None:
            args = self.default_args
        if index_field is not None:
            args.extend(["--index-field", index_field])
        if index_value is not None:
            args.extend(["--index-value", index_value])
        if index_value_end is not None:
            args.extend(["--index-value-end", index_value_end])
        if index_value_regex is not None:
            args.extend(["--index-value-regex", index_value_regex])
        if index_page_size is not None:
            args.extend(["--index-page-size", str(index_page_size)])
        options = Options()
        options.parseOptions(args)
        return StubbedModelCounter(self, options)

    def get_sub_riak(self, config):
        self.assertEqual(config.get('bucket_prefix'),
                         self.expected_bucket_prefix)
        return self.riak_manager

    @inlineCallbacks
    def mk_simple_models(self, n, start=0):
        for i in range(start, start + n):
            even_odd = {0: u"even", 1: u"odd"}[i % 2]
            obj = self.model(
                u"key-%d" % i, a=u"value-%d" % i, even_odd=even_odd)
            yield obj.save()

    def test_model_class_required(self):
        self.assertRaises(usage.UsageError, self.make_counter, [
            "-b", self.expected_bucket_prefix,
        ])

    def test_bucket_required(self):
        self.assertRaises(usage.UsageError, self.make_counter, [
            "-m", self.model_cls_path,
        ])

    def test_index_value_requires_index(self):
        """
        index-value without index-field is invalid.
        """
        self.assertRaises(
            usage.UsageError, self.make_counter, index_value="foo")

    def test_index_requires_index_value(self):
        """
        index-field without index-value is invalid.
        """
        self.assertRaises(
            usage.UsageError, self.make_counter, index_field="foo")

    def test_index_value_end_requires_index_value(self):
        """
        index-value-end without index-value is invalid.
        """
        self.assertRaises(
            usage.UsageError, self.make_counter, index_value_end="foo")

    def test_index_value_regex_requires_index_value_end(self):
        """
        index-value-regex without a range query is pointless.
        """
        self.assertRaises(
            usage.UsageError, self.make_counter, index_field="foo",
            index_value="foo", index_value_regex="foo")

    @inlineCallbacks
    def test_main(self):
        """
        The counter runs via `main()`.
        """
        yield self.mk_simple_models(3)
        self.patch(sys, "stdout", StringIO())
        yield main(
            None, "name",
            "-m", self.model_cls_path,
            "-b", self.riak_manager.bucket_prefix)
        self.assertEqual(
            sys.stdout.getvalue(),
            "Counting all keys ...\nDone, 3 objects found.\n")

    @inlineCallbacks
    def test_count_all_keys(self):
        """
        All keys are counted
        """
        yield self.mk_simple_models(5)
        counter = self.make_counter()
        yield counter.run()
        self.assertEqual(counter.output, [
            "Counting all keys ...",
            "Done, 5 objects found.",
        ])

    @inlineCallbacks
    def test_count_by_index_value(self):
        yield self.mk_simple_models(5)
        counter = self.make_counter(index_field="even_odd", index_value="odd")
        yield counter.run()
        self.assertEqual(counter.output, [
            "Counting ...",
            "Done, 2 objects found.",
        ])

    @inlineCallbacks
    def test_count_by_index_range(self):
        yield self.mk_simple_models(5)
        counter = self.make_counter(
            index_field="a", index_value="value-1", index_value_end="value-3")
        yield counter.run()
        self.assertEqual(counter.output, [
            "Counting ...",
            "Done, 3 objects found.",
        ])

    @inlineCallbacks
    def test_count_with_filter_range(self):
        yield self.mk_simple_models(5)
        counter = self.make_counter(
            index_field="a", index_value="value-1", index_value_end="value-3",
            index_value_regex=r"value-[0134]")
        yield counter.run()
        self.assertEqual(counter.output, [
            "Counting ...",
            "Done, 2 objects found.",
        ])

    @inlineCallbacks
    def test_count_all_small_pages(self):
        yield self.mk_simple_models(3)
        counter = self.make_counter(index_page_size=2)
        yield counter.run()
        self.assertEqual(counter.output, [
            "Counting all keys ...",
            "2 objects counted.",
            "Done, 3 objects found.",
        ])

    @inlineCallbacks
    def test_count_range_small_pages(self):
        yield self.mk_simple_models(5)
        counter = self.make_counter(
            index_field="a", index_value="value-1", index_value_end="value-3",
            index_page_size=2)
        yield counter.run()
        self.assertEqual(counter.output, [
            "Counting ...",
            "2 objects counted.",
            "Done, 3 objects found.",
        ])

    @inlineCallbacks
    def test_count_all_tiny_pages(self):
        yield self.mk_simple_models(3)
        counter = self.make_counter(index_page_size=1)
        yield counter.run()
        self.assertEqual(counter.output, [
            "Counting all keys ...",
            "1 object counted.",
            "2 objects counted.",
            "3 objects counted.",
            "Done, 3 objects found.",
        ])
