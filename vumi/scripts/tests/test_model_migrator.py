"""Tests for vumi.scripts.model_migrator."""

from twisted.python import usage

from vumi.persist.model import Model
from vumi.persist.fields import Unicode
from vumi.scripts.model_migrator import ModelMigrator, Options
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


class SimpleModel(Model):
    a = Unicode()


class StubbedModelMigrator(ModelMigrator):
    def __init__(self, testcase, *args, **kwargs):
        # So we can patch the manager's load function to simulate failures.
        self._manager_load_func = kwargs.pop("manager_load_func", None)
        self.testcase = testcase
        self.output = []
        self.recorded_loads = []
        self.recorded_stores = []
        super(StubbedModelMigrator, self).__init__(*args, **kwargs)

    def emit(self, s):
        self.output.append(s)

    def get_riak_manager(self, riak_config):
        manager = self.testcase.get_riak_manager(riak_config)
        if self._manager_load_func is not None:
            self.testcase.patch(manager, "load", self._manager_load_func)
        self.testcase.persistence_helper.record_load_and_store(
            manager, self.recorded_loads, self.recorded_stores)
        return manager


class TestModelMigrator(VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True, is_sync=True))
        self.expected_bucket_prefix = "bucket"
        self.riak_manager = self.persistence_helper.get_riak_manager({
            "bucket_prefix": self.expected_bucket_prefix,
        })
        self.add_cleanup(self.riak_manager.close_manager)
        self.model = self.riak_manager.proxy(SimpleModel)
        self.model_cls_path = ".".join([
            SimpleModel.__module__, SimpleModel.__name__])
        self.default_args = [
            "-m", self.model_cls_path,
            "-b", self.expected_bucket_prefix,
        ]

    def make_migrator(self, args=None, manager_load_func=None):
        if args is None:
            args = self.default_args
        options = Options()
        options.parseOptions(args)
        return StubbedModelMigrator(
            self, options, manager_load_func=manager_load_func)

    def get_riak_manager(self, config):
        self.assertEqual(config["bucket_prefix"], self.expected_bucket_prefix)
        return self.persistence_helper.get_riak_manager(config)

    def recorded_loads_and_stores(self, model_migrator):
        return model_migrator.recorded_loads, model_migrator.recorded_stores

    def mk_simple_models(self, n):
        for i in range(n):
            obj = self.model(u"key-%d" % i, a=u"value-%d" % i)
            obj.save()

    def test_model_class_required(self):
        self.assertRaises(usage.UsageError, self.make_migrator, [
            "-b", self.expected_bucket_prefix,
        ])

    def test_bucket_required(self):
        self.assertRaises(usage.UsageError, self.make_migrator, [
            "-m", self.model_cls_path,
        ])

    def test_successful_migration(self):
        self.mk_simple_models(3)
        cfg = self.make_migrator()
        loads, stores = self.recorded_loads_and_stores(cfg)
        cfg.run()
        self.assertEqual(cfg.output, [
            "3 keys found. Migrating ...",
            "33% complete.", "66% complete.",
            "Done.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [u"key-%d" % i for i in range(3)])

    def test_migration_with_tombstones(self):
        self.mk_simple_models(3)

        def tombstone_load(modelcls, key, result=None):
            return None

        cfg = self.make_migrator(manager_load_func=tombstone_load)
        cfg.run()
        for i in range(3):
            self.assertTrue(("Skipping tombstone key u'key-%d'." % i)
                            in cfg.output)
        self.assertEqual(cfg.output[:1], [
            "3 keys found. Migrating ...",
        ])
        self.assertEqual(cfg.output[-2:], [
            "66% complete.",
            "Done.",
        ])

    def test_migration_with_failures(self):
        self.mk_simple_models(3)

        def error_load(modelcls, key, result=None):
            raise ValueError("Failed to load.")

        cfg = self.make_migrator(manager_load_func=error_load)
        cfg.run()
        line_pairs = zip(cfg.output, cfg.output[1:])
        for i in range(3):
            self.assertTrue((
                "Failed to migrate key u'key-0':",
                "  ValueError: Failed to load.",
            ) in line_pairs)
        self.assertEqual(cfg.output[:1], [
            "3 keys found. Migrating ...",
        ])
        self.assertEqual(cfg.output[-2:], [
            "66% complete.",
            "Done.",
        ])

    def test_migrating_specific_keys(self):
        self.mk_simple_models(3)
        cfg = self.make_migrator(self.default_args + ["--keys", "key-1,key-2"])
        loads, stores = self.recorded_loads_and_stores(cfg)
        cfg.run()
        self.assertEqual(cfg.output, [
            "Migrating 2 specified keys ...",
            "50% complete.",
            "Done.",
        ])
        self.assertEqual(sorted(loads), [u"key-1", u"key-2"])
        self.assertEqual(sorted(stores), [u"key-1", u"key-2"])

    def test_dry_run(self):
        self.mk_simple_models(3)
        cfg = self.make_migrator(self.default_args + ["--dry-run"])
        loads, stores = self.recorded_loads_and_stores(cfg)
        cfg.run()
        self.assertEqual(cfg.output, [
            "3 keys found. Migrating ...",
            "33% complete.", "66% complete.",
            "Done.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [])
