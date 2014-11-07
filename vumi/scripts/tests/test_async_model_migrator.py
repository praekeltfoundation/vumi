"""Tests for vumi.scripts.model_migrator."""

import sys
from StringIO import StringIO

from twisted.internet.defer import inlineCallbacks, succeed
from twisted.python import usage

from vumi.persist.model import Model
from vumi.persist.fields import Unicode
from vumi.scripts.async_model_migrator import ModelMigrator, Options, main
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


class SimpleModel(Model):
    a = Unicode()


class StubbedModelMigrator(ModelMigrator):
    def __init__(self, testcase, *args, **kwargs):
        self.testcase = testcase
        self.output = []
        super(StubbedModelMigrator, self).__init__(*args, **kwargs)

    def emit(self, s):
        self.output.append(s)

    def get_riak_manager(self, riak_config):
        return self.testcase.get_sub_riak(riak_config)


class TestModelMigrator(VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True, is_sync=False))
        self.riak_manager = self.persistence_helper.get_riak_manager()
        self.model = self.riak_manager.proxy(SimpleModel)
        self.model_cls_path = ".".join([
            SimpleModel.__module__, SimpleModel.__name__])
        self.expected_bucket_prefix = "bucket"
        self.default_args = [
            "-m", self.model_cls_path,
            "-b", self.expected_bucket_prefix,
        ]

    def make_migrator(self, args=None, batch_size=None):
        if args is None:
            args = self.default_args
        if batch_size is not None:
            args.extend(["--batch-size", str(batch_size)])
        options = Options()
        options.parseOptions(args)
        return StubbedModelMigrator(self, options)

    def get_sub_riak(self, config):
        self.assertEqual(config.get('bucket_prefix'),
                         self.expected_bucket_prefix)
        return self.riak_manager

    @inlineCallbacks
    def mk_simple_models(self, n):
        for i in range(n):
            obj = self.model(u"key-%d" % i, a=u"value-%d" % i)
            yield obj.save()

    def record_load_and_store(self):
        loads, stores = [], []
        orig_load = self.riak_manager.load
        orig_store = self.riak_manager.store

        def record_load(modelcls, key, result=None):
            loads.append(key)
            return orig_load(modelcls, key, result=result)

        def record_store(obj):
            stores.append(obj.key)
            return orig_store(obj)

        self.patch(self.riak_manager, 'load', record_load)
        self.patch(self.riak_manager, 'store', record_store)
        return loads, stores

    def test_model_class_required(self):
        self.assertRaises(usage.UsageError, self.make_migrator, [
            "-b", self.expected_bucket_prefix,
        ])

    def test_bucket_required(self):
        self.assertRaises(usage.UsageError, self.make_migrator, [
            "-m", self.model_cls_path,
        ])

    @inlineCallbacks
    def test_main(self):
        yield self.mk_simple_models(3)
        self.patch(sys, "stdout", StringIO())
        yield main(
            None, "name",
            "-m", self.model_cls_path,
            "-b", self.riak_manager.bucket_prefix)
        self.assertEqual(
            sys.stdout.getvalue(),
            "Migrating ...\nDone, 3 objects migrated.\n")

    @inlineCallbacks
    def test_successful_migration(self):
        yield self.mk_simple_models(3)
        loads, stores = self.record_load_and_store()
        model_migrator = self.make_migrator()
        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [u"key-%d" % i for i in range(3)])

    @inlineCallbacks
    def test_successful_migration_small_batches(self):
        yield self.mk_simple_models(3)
        loads, stores = self.record_load_and_store()
        model_migrator = self.make_migrator(batch_size=2)
        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "2 objects migrated.",
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [u"key-%d" % i for i in range(3)])

    @inlineCallbacks
    def test_successful_migration_tiny_batches(self):
        yield self.mk_simple_models(3)
        loads, stores = self.record_load_and_store()
        model_migrator = self.make_migrator(batch_size=1)
        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "1 object migrated.",
            "2 objects migrated.",
            "3 objects migrated.",
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [u"key-%d" % i for i in range(3)])

    @inlineCallbacks
    def test_migration_with_tombstones(self):
        yield self.mk_simple_models(3)

        def tombstone_load(modelcls, key, result=None):
            return succeed(None)

        self.patch(self.riak_manager, 'load', tombstone_load)

        model_migrator = self.make_migrator()
        yield model_migrator.run()
        for i in range(3):
            self.assertTrue(("Skipping tombstone key u'key-%d'." % i)
                            in model_migrator.output)
        self.assertEqual(model_migrator.output[:1], [
            "Migrating ...",
        ])
        self.assertEqual(model_migrator.output[-1:], [
            "Done, 3 objects migrated.",
        ])

    @inlineCallbacks
    def test_migration_with_failures(self):
        yield self.mk_simple_models(3)

        def error_load(modelcls, key, result=None):
            raise ValueError("Failed to load.")

        self.patch(self.riak_manager, 'load', error_load)

        model_migrator = self.make_migrator()
        yield model_migrator.run()
        line_pairs = zip(model_migrator.output, model_migrator.output[1:])
        for i in range(3):
            self.assertTrue((
                "Failed to migrate key u'key-0':",
                "  ValueError: Failed to load.",
            ) in line_pairs)
        self.assertEqual(model_migrator.output[:1], [
            "Migrating ...",
        ])
        self.assertEqual(model_migrator.output[-1:], [
            "Done, 3 objects migrated.",
        ])

    @inlineCallbacks
    def test_migrating_specific_keys(self):
        yield self.mk_simple_models(3)
        loads, stores = self.record_load_and_store()
        model_migrator = self.make_migrator(
            self.default_args + ["--keys", "key-1,key-2"])
        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating 2 specified keys ...",
            "Done, 2 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-1", u"key-2"])
        self.assertEqual(sorted(stores), [u"key-1", u"key-2"])

    @inlineCallbacks
    def test_dry_run(self):
        yield self.mk_simple_models(3)
        loads, stores = self.record_load_and_store()
        model_migrator = self.make_migrator(self.default_args + ["--dry-run"])
        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [])
