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

    def make_cfg(self, args):
        options = Options()
        options.parseOptions(args)
        return TestConfigHolder(self, options)

    def get_sub_riak(self, config):
        self.assertEqual(config.get('bucket_prefix'),
                         self.expected_bucket_prefix)
        return self.riak_manager

    def test_model_class_required(self):
        self.assertRaises(usage.UsageError, self.make_cfg, [
            "-b", self.expected_bucket_prefix,
        ])

    def test_bucket_required(self):
        self.assertRaises(usage.UsageError, self.make_cfg, [
            "-m", self.model_cls_path,
        ])

    def test_successful_migration(self):
        for i in range(3):
            obj = self.model(u"key-%d" % i, a=u"value-%d" % i)
            obj.save()
        cfg = self.make_cfg([
            "-m", self.model_cls_path,
            "-b", self.expected_bucket_prefix,
        ])
        cfg.run()
        self.assertEqual(cfg.output, [
            "3 keys found. Migrating ...",
            "33% complete.", "66% complete.",
            "Done.",
        ])

    def test_migration_with_failures(self):
        pass
