"""Tests for vumi.scripts.model_migrator."""

from twisted.python import usage
from twisted.trial.unittest import TestCase

from vumi.persist.model import Model
from vumi.persist.fields import Unicode
from vumi.scripts.model_migrator import ConfigHolder, Options
from vumi.tests.utils import PersistenceMixin


class SimpleModel(Model):
    a = Unicode()


class TestConfigHolder(ConfigHolder):
    def __init__(self, testcase, *args, **kwargs):
        self.testcase = testcase
        self.output = []
        super(TestConfigHolder, self).__init__(*args, **kwargs)

    def emit(self, s):
        self.output.append(s)

    def get_riak_manager(self, riak_config):
        return self.testcase.get_sub_riak(riak_config)


class ModelMigratorTestCase(TestCase, PersistenceMixin):
    sync_persistence = True
    use_riak = True

    def setUp(self):
        self._persist_setUp()
        self.riak_manager = self.get_riak_manager()
        self.model = self.riak_manager.proxy(SimpleModel)
        self.model_cls_path = ".".join([
            SimpleModel.__module__, SimpleModel.__name__])
        self.expected_bucket_prefix = "bucket"

    def tearDown(self):
        return self._persist_tearDown()

    def make_cfg(self, args=None):
        if args is None:
            args = [
                "-m", self.model_cls_path,
                "-b", self.expected_bucket_prefix,
            ]
        options = Options()
        options.parseOptions(args)
        return TestConfigHolder(self, options)

    def get_sub_riak(self, config):
        self.assertEqual(config.get('bucket_prefix'),
                         self.expected_bucket_prefix)
        return self.riak_manager

    def mk_simple_models(self, n):
        for i in range(n):
            obj = self.model(u"key-%d" % i, a=u"value-%d" % i)
            obj.save()

    def test_model_class_required(self):
        self.assertRaises(usage.UsageError, self.make_cfg, [
            "-b", self.expected_bucket_prefix,
        ])

    def test_bucket_required(self):
        self.assertRaises(usage.UsageError, self.make_cfg, [
            "-m", self.model_cls_path,
        ])

    def test_successful_migration(self):
        self.mk_simple_models(3)

        loads, stores = [], []
        orig_load = self.riak_manager.load

        def record_load(modelcls, key, result=None):
            loads.append(key)
            return orig_load(modelcls, key, result=result)

        self.patch(self.riak_manager, 'load', record_load)

        def record_store(obj):
            stores.append(obj.key)

        self.patch(self.riak_manager, 'store', record_store)

        cfg = self.make_cfg()
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

        self.patch(self.riak_manager, 'load', tombstone_load)

        cfg = self.make_cfg()
        cfg.run()
        for i in range(3):
            self.assertTrue(("Skipping tombstone key 'key-%d'." % i)
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

        self.patch(self.riak_manager, 'load', error_load)

        cfg = self.make_cfg()
        cfg.run()
        line_pairs = zip(cfg.output, cfg.output[1:])
        for i in range(3):
            self.assertTrue((
                "Failed to migrate key 'key-0':",
                "  ValueError: Failed to load.",
            ) in line_pairs)
        self.assertEqual(cfg.output[:1], [
            "3 keys found. Migrating ...",
        ])
        self.assertEqual(cfg.output[-2:], [
            "66% complete.",
            "Done.",
        ])
