"""Tests for vumi.scripts.vumi_model_migrator."""

import sys
from StringIO import StringIO

from twisted.internet.defer import inlineCallbacks, succeed
from twisted.internet.task import deferLater
from twisted.python import usage

from vumi.persist import model
from vumi.persist.fields import Unicode
from vumi.scripts.vumi_model_migrator import ModelMigrator, Options, main
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


def post_migrate_function(obj):
    """
    Post-migrate-function for use in tests.
    """
    obj.a = obj.a + u"-modified"
    return True


def post_migrate_function_deferred(obj):
    """
    Post-migrate-function for use in tests.
    """
    from twisted.internet import reactor
    return deferLater(reactor, 0.1, post_migrate_function, obj)


def post_migrate_function_new_only(obj):
    """
    Post-migrate-function for use in tests.
    """
    if obj.was_migrated:
        return post_migrate_function(obj)
    return False


def fqpn(thing):
    """
    Get the fully-qualified name of a thing.
    """
    return ".".join([thing.__module__, thing.__name__])


class SimpleModelMigrator(model.ModelMigrator):
    def migrate_from_1(self, migration_data):
        migration_data.set_value('$VERSION', 2)
        migration_data.copy_values("a")
        return migration_data


class SimpleModelOld(model.Model):
    VERSION = 1
    bucket = 'simplemodel'
    a = Unicode()


class SimpleModel(model.Model):
    VERSION = 2
    MIGRATOR = SimpleModelMigrator
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


class TestVumiModelMigrator(VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True, is_sync=False))
        self.expected_bucket_prefix = "bucket"
        self.riak_manager = self.persistence_helper.get_riak_manager({
            "bucket_prefix": self.expected_bucket_prefix,
        })
        self.add_cleanup(self.riak_manager.close_manager)
        self.old_model = self.riak_manager.proxy(SimpleModelOld)
        self.model = self.riak_manager.proxy(SimpleModel)
        self.model_cls_path = fqpn(SimpleModel)
        self.default_args = [
            "-m", self.model_cls_path,
            "-b", self.expected_bucket_prefix,
        ]

    def make_migrator(self, args=None, index_page_size=None,
                      concurrent_migrations=None, continuation_token=None,
                      post_migrate_function=None, manager_load_func=None):
        if args is None:
            args = self.default_args
        if index_page_size is not None:
            args.extend(
                ["--index-page-size", str(index_page_size)])
        if concurrent_migrations is not None:
            args.extend(
                ["--concurrent-migrations", str(concurrent_migrations)])
        if continuation_token is not None:
            args.extend(
                ["--continuation-token", continuation_token])
        if post_migrate_function is not None:
            args.extend(
                ["--post-migrate-function", post_migrate_function])
        options = Options()
        options.parseOptions(args)
        return StubbedModelMigrator(
            self, options, manager_load_func=manager_load_func)

    def get_riak_manager(self, config):
        self.assertEqual(config["bucket_prefix"], self.expected_bucket_prefix)
        return self.persistence_helper.get_riak_manager(config)

    def recorded_loads_and_stores(self, model_migrator):
        return model_migrator.recorded_loads, model_migrator.recorded_stores

    @inlineCallbacks
    def mk_simple_models_old(self, n, start=0):
        for i in range(start, start + n):
            obj = self.old_model(u"key-%d" % i, a=u"value-%d" % i)
            yield obj.save()

    @inlineCallbacks
    def mk_simple_models_new(self, n, start=0):
        for i in range(start, start + n):
            obj = self.model(u"key-%d" % i, a=u"value-%d" % i)
            yield obj.save()

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
        yield self.mk_simple_models_old(3)
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
        yield self.mk_simple_models_old(3)
        model_migrator = self.make_migrator()
        loads, stores = self.recorded_loads_and_stores(model_migrator)
        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [u"key-%d" % i for i in range(3)])

    @inlineCallbacks
    def test_successful_migration_small_pages(self):
        yield self.mk_simple_models_old(3)
        model_migrator = self.make_migrator(index_page_size=2)
        loads, stores = self.recorded_loads_and_stores(model_migrator)
        yield model_migrator.run()
        [continuation] = [line for line in model_migrator.output
                          if line.startswith("Continuation token:")]
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "2 objects migrated.",
            continuation,
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [u"key-%d" % i for i in range(3)])

    @inlineCallbacks
    def test_successful_migration_tiny_pages(self):
        yield self.mk_simple_models_old(3)
        model_migrator = self.make_migrator(index_page_size=1)
        loads, stores = self.recorded_loads_and_stores(model_migrator)
        yield model_migrator.run()
        [ct1, ct2, ct3] = [line for line in model_migrator.output
                           if line.startswith("Continuation token:")]
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "1 object migrated.",
            ct1,
            "2 objects migrated.",
            ct2,
            "3 objects migrated.",
            ct3,
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [u"key-%d" % i for i in range(3)])

    @inlineCallbacks
    def test_successful_migration_with_continuation(self):
        yield self.mk_simple_models_old(3)

        # Run a migration all the way through to get a continuation token
        model_migrator = self.make_migrator(index_page_size=2)
        loads, stores = self.recorded_loads_and_stores(model_migrator)
        yield model_migrator.run()
        [continuation] = [line for line in model_migrator.output
                          if line.startswith("Continuation token:")]
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "2 objects migrated.",
            continuation,
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [u"key-%d" % i for i in range(3)])

        # Recreate key-2 because it was already migrated and would otherwise be
        # skipped.
        yield self.mk_simple_models_old(1, start=2)
        # Run a migration starting from the continuation point.
        loads[:] = []
        stores[:] = []
        continuation_token = continuation.split()[-1][1:-1]
        cont_model_migrator = self.make_migrator(
            index_page_size=2, continuation_token=continuation_token)
        cloads, cstores = self.recorded_loads_and_stores(cont_model_migrator)
        yield cont_model_migrator.run()
        self.assertEqual(cont_model_migrator.output, [
            "Migrating ...",
            "Done, 1 object migrated.",
        ])
        self.assertEqual(cloads, [u"key-2"])
        self.assertEqual(cstores, [u"key-2"])

    @inlineCallbacks
    def test_migration_with_tombstones(self):
        yield self.mk_simple_models_old(3)

        def tombstone_load(modelcls, key, result=None):
            return succeed(None)

        model_migrator = self.make_migrator(manager_load_func=tombstone_load)
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
        yield self.mk_simple_models_old(3)

        def error_load(modelcls, key, result=None):
            raise ValueError("Failed to load.")

        model_migrator = self.make_migrator(manager_load_func=error_load)
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
        yield self.mk_simple_models_old(3)
        model_migrator = self.make_migrator(
            self.default_args + ["--keys", "key-1,key-2"])
        loads, stores = self.recorded_loads_and_stores(model_migrator)
        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating 2 specified keys ...",
            "Done, 2 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-1", u"key-2"])
        self.assertEqual(sorted(stores), [u"key-1", u"key-2"])

    @inlineCallbacks
    def test_dry_run(self):
        yield self.mk_simple_models_old(3)
        model_migrator = self.make_migrator(self.default_args + ["--dry-run"])
        loads, stores = self.recorded_loads_and_stores(model_migrator)
        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [])

    @inlineCallbacks
    def test_migrating_old_and_new_keys(self):
        """
        Models that haven't been migrated don't need to be stored.
        """
        yield self.mk_simple_models_old(1)
        yield self.mk_simple_models_new(1, start=1)
        yield self.mk_simple_models_old(1, start=2)
        model_migrator = self.make_migrator(self.default_args)
        loads, stores = self.recorded_loads_and_stores(model_migrator)

        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-0", u"key-1", u"key-2"])
        self.assertEqual(sorted(stores), [u"key-0", u"key-2"])

    @inlineCallbacks
    def test_migrating_with_post_migrate_function(self):
        """
        If post-migrate-function is provided, it should be called for every
        object.
        """
        yield self.mk_simple_models_old(3)
        model_migrator = self.make_migrator(
            post_migrate_function=fqpn(post_migrate_function))
        loads, stores = self.recorded_loads_and_stores(model_migrator)

        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [u"key-%d" % i for i in range(3)])
        for i in range(3):
            obj = yield self.model.load(u"key-%d" % i)
            self.assertEqual(obj.a, u"value-%d-modified" % i)

    @inlineCallbacks
    def test_migrating_with_deferred_post_migrate_function(self):
        """
        A post-migrate-function may return a Deferred.
        """
        yield self.mk_simple_models_old(3)
        model_migrator = self.make_migrator(
            post_migrate_function=fqpn(post_migrate_function_deferred))
        loads, stores = self.recorded_loads_and_stores(model_migrator)

        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [u"key-%d" % i for i in range(3)])
        for i in range(3):
            obj = yield self.model.load(u"key-%d" % i)
            self.assertEqual(obj.a, u"value-%d-modified" % i)

    @inlineCallbacks
    def test_migrating_old_and_new_with_post_migrate_function(self):
        """
        A post-migrate-function may choose to modify objects that were not
        migrated.
        """
        yield self.mk_simple_models_old(1)
        yield self.mk_simple_models_new(1, start=1)
        yield self.mk_simple_models_old(1, start=2)
        model_migrator = self.make_migrator(
            post_migrate_function=fqpn(post_migrate_function))
        loads, stores = self.recorded_loads_and_stores(model_migrator)

        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-%d" % i for i in range(3)])
        self.assertEqual(sorted(stores), [u"key-%d" % i for i in range(3)])
        for i in range(3):
            obj = yield self.model.load(u"key-%d" % i)
            self.assertEqual(obj.a, u"value-%d-modified" % i)

    @inlineCallbacks
    def test_migrating_old_and_new_with_new_only_post_migrate_function(self):
        """
        A post-migrate-function may choose to leave objects that were not
        migrated unmodified.
        """
        yield self.mk_simple_models_old(1)
        yield self.mk_simple_models_new(1, start=1)
        yield self.mk_simple_models_old(1, start=2)
        model_migrator = self.make_migrator(
            post_migrate_function=fqpn(post_migrate_function_new_only))
        loads, stores = self.recorded_loads_and_stores(model_migrator)

        yield model_migrator.run()
        self.assertEqual(model_migrator.output, [
            "Migrating ...",
            "Done, 3 objects migrated.",
        ])
        self.assertEqual(sorted(loads), [u"key-0", u"key-1", u"key-2"])
        self.assertEqual(sorted(stores), [u"key-0", u"key-2"])

        obj_0 = yield self.model.load(u"key-0")
        self.assertEqual(obj_0.a, u"value-0-modified")
        obj_1 = yield self.model.load(u"key-1")
        self.assertEqual(obj_1.a, u"value-1")
        obj_2 = yield self.model.load(u"key-2")
        self.assertEqual(obj_2.a, u"value-2-modified")
