# -*- coding: utf-8 -*-

"""Tests for vumi.persist.model."""

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.persist.model import (
    Model, Manager, ModelMigrator, ModelMigrationError, VumiRiakError)
from vumi.persist import fields
from vumi.persist.fields import ValidationError, Integer, Unicode, Dynamic
from vumi.tests.helpers import VumiTestCase, import_skip


class SimpleModel(Model):
    a = Integer()
    b = Unicode()


class IndexedModel(Model):
    a = Integer(index=True)
    b = Unicode(index=True, null=True)


class InheritedModel(SimpleModel):
    c = Integer()


class OverriddenModel(InheritedModel):
    c = Integer(min=0, max=5)


class VersionedModelMigrator(ModelMigrator):
    def migrate_from_unversioned(self, migration_data):
        # Migrator assertions
        assert self.data_version is None
        assert self.model_class is VersionedModel
        assert isinstance(self.manager, Manager)

        # Data assertions
        assert set(migration_data.old_data.keys()) == set(['$VERSION', 'a'])
        assert migration_data.old_data['$VERSION'] is None
        assert migration_data.old_index == {}

        # Actual migration
        migration_data.set_value('$VERSION', 1)
        migration_data.set_value('b', migration_data.old_data['a'])
        return migration_data

    def reverse_from_1(self, migration_data):
        # Migrator assertions
        assert self.data_version == 1
        assert self.model_class is VersionedModel
        assert isinstance(self.manager, Manager)

        # Data assertions
        assert set(migration_data.old_data.keys()) == set(['$VERSION', 'b'])
        assert migration_data.old_data['$VERSION'] == 1
        assert migration_data.old_index == {}

        # Actual migration
        migration_data.set_value('$VERSION', None)
        migration_data.set_value('a', migration_data.old_data['b'])
        return migration_data

    def migrate_from_1(self, migration_data):
        # Migrator assertions
        assert self.data_version == 1
        assert self.model_class is VersionedModel
        assert isinstance(self.manager, Manager)

        # Data assertions
        assert set(migration_data.old_data.keys()) == set(['$VERSION', 'b'])
        assert migration_data.old_data['$VERSION'] == 1
        assert migration_data.old_index == {}

        # Actual migration
        migration_data.set_value('$VERSION', 2)
        migration_data.set_value('c', migration_data.old_data['b'])
        migration_data.set_value('text', 'hello')
        return migration_data

    def reverse_from_2(self, migration_data):
        # Migrator assertions
        assert self.data_version == 2
        assert self.model_class is VersionedModel
        assert isinstance(self.manager, Manager)

        # Data assertions
        assert set(migration_data.old_data.keys()) == set(
            ['$VERSION', 'c', 'text'])
        assert migration_data.old_data['$VERSION'] == 2
        assert migration_data.old_index == {}

        # Actual migration
        migration_data.set_value('$VERSION', 1)
        migration_data.set_value('b', migration_data.old_data['c'])
        # Drop the text field.
        return migration_data

    def migrate_from_2(self, migration_data):
        # Migrator assertions
        assert self.data_version == 2
        assert self.model_class is IndexedVersionedModel
        assert isinstance(self.manager, Manager)

        # Data assertions
        assert set(migration_data.old_data.keys()) == set(
            ['$VERSION', 'c', 'text'])
        assert migration_data.old_data['$VERSION'] == 2
        assert migration_data.old_index == {}

        # Actual migration
        migration_data.set_value('$VERSION', 3)
        migration_data.copy_values('c')
        migration_data.set_value(
            'text', migration_data.old_data['text'], index='text_bin')
        return migration_data

    def reverse_from_3(self, migration_data):
        # Migrator assertions
        assert self.data_version == 3
        assert self.model_class is IndexedVersionedModel
        assert isinstance(self.manager, Manager)

        # Data assertions
        assert set(migration_data.old_data.keys()) == set(
            ['$VERSION', 'c', 'text'])
        assert migration_data.old_data['$VERSION'] == 3
        assert migration_data.old_index == {"text_bin": ["hi"]}

        # Actual migration
        migration_data.set_value('$VERSION', 2)
        migration_data.copy_values('c')
        migration_data.set_value('text', migration_data.old_data['text'])
        return migration_data

    def migrate_from_3(self, migration_data):
        # Migrator assertions
        assert self.data_version == 3
        assert self.model_class is IndexRemovedVersionedModel
        assert isinstance(self.manager, Manager)

        # Data assertions
        assert set(migration_data.old_data.keys()) == set(
            ['$VERSION', 'c', 'text'])
        assert migration_data.old_data['$VERSION'] == 3
        assert migration_data.old_index == {"text_bin": ["hi"]}

        # Actual migration
        migration_data.set_value('$VERSION', 4)
        migration_data.copy_values('c')
        migration_data.set_value('text', migration_data.old_data['text'])
        return migration_data

    def reverse_from_4(self, migration_data):
        # Migrator assertions
        assert self.data_version == 4
        assert self.model_class is IndexRemovedVersionedModel
        assert isinstance(self.manager, Manager)

        # Data assertions
        assert set(migration_data.old_data.keys()) == set(
            ['$VERSION', 'c', 'text'])
        assert migration_data.old_data['$VERSION'] == 4
        assert migration_data.old_index == {}

        # Actual migration
        migration_data.set_value('$VERSION', 3)
        migration_data.copy_values('c')
        migration_data.set_value(
            'text', migration_data.old_data['text'], index='text_bin')
        return migration_data


class UnversionedModel(Model):
    bucket = 'versionedmodel'
    a = Integer()


class OldVersionedModel(Model):
    VERSION = 1
    bucket = 'versionedmodel'
    b = Integer()


class VersionedModel(Model):
    VERSION = 2
    MIGRATOR = VersionedModelMigrator
    c = Integer()
    text = Unicode(null=True)


class IndexedVersionedModel(Model):
    VERSION = 3
    MIGRATOR = VersionedModelMigrator
    bucket = 'versionedmodel'
    c = Integer()
    text = Unicode(null=True, index=True)


class IndexRemovedVersionedModel(Model):
    VERSION = 4
    MIGRATOR = VersionedModelMigrator
    bucket = 'versionedmodel'
    c = Integer()
    text = Unicode(null=True)


class UnknownVersionedModel(Model):
    VERSION = 5
    bucket = 'versionedmodel'
    d = Integer()


class VersionedDynamicModelMigrator(ModelMigrator):
    def migrate_from_unversioned(self, migration_data):
        migration_data.copy_dynamic_values('keep-')
        migration_data.set_value('$VERSION', 1)
        return migration_data


class UnversionedDynamicModel(Model):
    bucket = 'versioneddynamicmodel'

    drop = Dynamic(prefix='drop-')
    keep = Dynamic(prefix='keep-')


class VersionedDynamicModel(Model):
    bucket = 'versioneddynamicmodel'

    VERSION = 1
    MIGRATOR = VersionedDynamicModelMigrator

    drop = Dynamic(prefix='drop-')
    keep = Dynamic(prefix='keep-')


class ModelTestMixin(object):

    @Manager.calls_manager
    def filter_tombstones(self, model_cls, keys):
        live_keys = []
        for key in keys:
            model = yield model_cls.load(key)
            if model is not None:
                live_keys.append(key)
        returnValue(live_keys)

    def get_model_indexes(self, model):
        indexes = {}
        for name, value in model._riak_object.get_indexes():
            indexes.setdefault(name, []).append(value)
        return indexes

    def test_simple_class(self):
        field_names = SimpleModel.field_descriptors.keys()
        self.assertEqual(sorted(field_names), ['a', 'b'])
        self.assertTrue(isinstance(SimpleModel.a, Integer))
        self.assertTrue(isinstance(SimpleModel.b, Unicode))

    def test_repr(self):
        simple_model = self.manager.proxy(SimpleModel)
        s = simple_model("foo", a=1, b=u"bar")
        self.assertEqual(
            repr(s),
            "<SimpleModel $VERSION=None a=1 b=u'bar' key='foo'>")

    def test_get_data(self):
        simple_model = self.manager.proxy(SimpleModel)
        s = simple_model("foo", a=1, b=u"bar")
        self.assertEqual(s.get_data(), {
            '$VERSION': None,
            'key': 'foo',
            'a': 1,
            'b': 'bar',
            })

    def test_declare_backlinks(self):
        class TestModel(Model):
            pass

        TestModel.backlinks.declare_backlink("foo", lambda m, o: None)
        self.assertRaises(RuntimeError, TestModel.backlinks.declare_backlink,
                          "foo", lambda m, o: None)

        t = TestModel(self.manager, "key")
        self.assertTrue(callable(t.backlinks.foo))
        self.assertRaises(AttributeError, getattr, t.backlinks, 'bar')

    @inlineCallbacks
    def assert_mapreduce_results(self, expected_keys, mr_func, *args, **kw):
        keys = yield mr_func(*args, **kw).get_keys()
        count = yield mr_func(*args, **kw).get_count()
        self.assertEqual(expected_keys, sorted(keys))
        self.assertEqual(len(expected_keys), count)

    @inlineCallbacks
    def assert_search_results(self, expected_keys, func, *args, **kw):
        keys = yield func(*args, **kw)
        self.assertEqual(expected_keys, sorted(keys))

    @Manager.calls_manager
    def test_simple_search(self):
        simple_model = self.manager.proxy(SimpleModel)
        yield simple_model.enable_search()
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        yield simple_model("three", a=2, b=u'ghi').save()

        search = simple_model.search
        yield self.assert_mapreduce_results(["one"], search, a=1)
        yield self.assert_mapreduce_results(["two"], search, a=2, b='def')
        yield self.assert_mapreduce_results(["three", "two"], search, a=2)

    @Manager.calls_manager
    def test_simple_search_escaping(self):
        simple_model = self.manager.proxy(SimpleModel)
        search = simple_model.search
        yield simple_model.enable_search()
        yield simple_model("one", a=1, b=u'a\'bc').save()

        search = simple_model.search
        yield self.assert_mapreduce_results([], search, b=" OR a:1")
        yield self.assert_mapreduce_results([], search, b="b' OR a:1 '")
        yield self.assert_mapreduce_results(["one"], search, b="a\'bc")

    @Manager.calls_manager
    def test_simple_raw_search(self):
        simple_model = self.manager.proxy(SimpleModel)
        yield simple_model.enable_search()
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        yield simple_model("three", a=2, b=u'ghi').save()

        search = simple_model.raw_search
        yield self.assert_mapreduce_results(["one"], search, 'a:1')
        yield self.assert_mapreduce_results(["two"], search, 'a:2 AND b:def')
        yield self.assert_mapreduce_results(
            ["one", "two"], search, 'b:abc OR b:def')
        yield self.assert_mapreduce_results(["three", "two"], search, 'a:2')

    @Manager.calls_manager
    def test_simple_real_search(self):
        simple_model = self.manager.proxy(SimpleModel)
        yield simple_model.enable_search()
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        yield simple_model("three", a=2, b=u'ghi').save()

        search = simple_model.real_search
        yield self.assert_search_results(["one"], search, 'a:1')
        yield self.assert_search_results(["two"], search, 'a:2 AND b:def')
        yield self.assert_search_results(
            ["one", "two"], search, 'b:abc OR b:def')
        yield self.assert_search_results(["three", "two"], search, 'a:2')

    @Manager.calls_manager
    def test_big_real_search(self):
        simple_model = self.manager.proxy(SimpleModel)
        yield simple_model.enable_search()
        keys = []
        for i in range(100):
            key = "xx%06d" % (i + 1)
            keys.append(key)
            yield simple_model(key, a=99, b=u'abc').save()
        yield simple_model("yy000001", a=98, b=u'def').save()
        yield simple_model("yy000002", a=98, b=u'ghi').save()

        search = lambda q: simple_model.real_search(q, rows=11)
        yield self.assert_search_results(keys, search, 'a:99')

    @Manager.calls_manager
    def test_empty_real_search(self):
        simple_model = self.manager.proxy(SimpleModel)
        yield simple_model.enable_search()
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        yield simple_model("three", a=2, b=u'ghi').save()

        search = simple_model.real_search
        yield self.assert_search_results([], search, 'a:7')

    @Manager.calls_manager
    def test_limited_results_real_search(self):
        simple_model = self.manager.proxy(SimpleModel)
        yield simple_model.enable_search()
        yield simple_model("1one", a=1, b=u'abc').save()
        yield simple_model("2two", a=2, b=u'def').save()
        yield simple_model("3three", a=2, b=u'ghi').save()
        yield simple_model("4four", a=2, b=u'jkl').save()

        @inlineCallbacks
        def search(q):
            results = yield simple_model.real_search(q, rows=2, start=0)
            self.assertEqual(len(results), 2)
            results_new = yield simple_model.real_search(q, rows=2, start=2)
            self.assertEqual(len(results_new), 1)
            returnValue(results + results_new)

        yield self.assert_search_results(
            [u'2two', u'3three', u'4four'], search, 'a:2')

    @Manager.calls_manager
    def test_load_all_bunches(self):
        self.assertFalse(self.manager.USE_MAPREDUCE_BUNCH_LOADING)
        simple_model = self.manager.proxy(SimpleModel)
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        yield simple_model("three", a=2, b=u'ghi').save()

        objs_iter = simple_model.load_all_bunches(['one', 'two', 'bad'])
        objs = []
        for obj_bunch in objs_iter:
            objs.extend((yield obj_bunch))
        self.assertEqual(["one", "two"], sorted(obj.key for obj in objs))

    @Manager.calls_manager
    def test_load_all_bunches_skips_tombstones(self):
        self.assertFalse(self.manager.USE_MAPREDUCE_BUNCH_LOADING)
        simple_model = self.manager.proxy(SimpleModel)
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        tombstone = yield simple_model("tombstone", a=2, b=u'ghi').save()
        yield tombstone.delete()

        objs_iter = simple_model.load_all_bunches(['one', 'two', 'tombstone'])
        objs = []
        for obj_bunch in objs_iter:
            objs.extend((yield obj_bunch))
        self.assertEqual(["one", "two"], sorted(obj.key for obj in objs))

    @Manager.calls_manager
    def test_load_all_bunches_mapreduce(self):
        self.manager.USE_MAPREDUCE_BUNCH_LOADING = True
        simple_model = self.manager.proxy(SimpleModel)
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        yield simple_model("three", a=2, b=u'ghi').save()

        objs_iter = simple_model.load_all_bunches(['one', 'two', 'bad'])
        objs = []
        for obj_bunch in objs_iter:
            objs.extend((yield obj_bunch))
        self.assertEqual(["one", "two"], sorted(obj.key for obj in objs))

    @Manager.calls_manager
    def test_load_all_bunches_mapreduce_skips_tombstones(self):
        self.manager.USE_MAPREDUCE_BUNCH_LOADING = True
        simple_model = self.manager.proxy(SimpleModel)
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        tombstone = yield simple_model("tombstone", a=2, b=u'ghi').save()
        yield tombstone.delete()

        objs_iter = simple_model.load_all_bunches(['one', 'two', 'tombstone'])
        objs = []
        for obj_bunch in objs_iter:
            objs.extend((yield obj_bunch))
        self.assertEqual(["one", "two"], sorted(obj.key for obj in objs))

    @Manager.calls_manager
    def test_load_all_bunches_performance(self):
        """
        A performance test that is handy to occasionally but shouldn't happen
        on every test run.

        This should go away once we're happy with the non-mapreduce bunch
        loading.
        """
        import time
        start_setup = time.time()
        simple_model = self.manager.proxy(SimpleModel)
        keys = []
        for i in xrange(2000):
            obj = yield simple_model("item%s" % i, a=i, b=u'abc').save()
            keys.append(obj.key)

        end_setup = time.time()
        print "\n\nSetup time: %s" % (end_setup - start_setup,)

        start_mr = time.time()
        self.manager.USE_MAPREDUCE_BUNCH_LOADING = True
        objs_iter = simple_model.load_all_bunches(keys)
        objs = []
        for obj_bunch in objs_iter:
            objs.extend((yield obj_bunch))
        end_mr = time.time()
        print "Mapreduce time: %s" % (end_mr - start_mr,)

        start_mult = time.time()
        self.manager.USE_MAPREDUCE_BUNCH_LOADING = False
        objs_iter = simple_model.load_all_bunches(keys)
        objs = []
        for obj_bunch in objs_iter:
            objs.extend((yield obj_bunch))
        end_mult = time.time()
        print "Multiple time: %s\n" % (end_mult - start_mult,)

        self.assertEqual(sorted(keys), sorted(obj.key for obj in objs))
    test_load_all_bunches_performance.skip = (
        "This takes a long time to run. Enable it if you need it.")

    @Manager.calls_manager
    def test_simple_instance(self):
        simple_model = self.manager.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        yield s1.save()

        s2 = yield simple_model.load("foo")
        self.assertEqual(s2.a, 5)
        self.assertEqual(s2.b, u'3')
        self.assertEqual(s2.was_migrated, False)

    @Manager.calls_manager
    def test_simple_instance_delete(self):
        simple_model = self.manager.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        yield s1.save()

        s2 = yield simple_model.load("foo")
        yield s2.delete()

        s3 = yield simple_model.load("foo")
        self.assertEqual(s3, None)

    @Manager.calls_manager
    def test_nonexist_keys_return_none(self):
        simple_model = self.manager.proxy(SimpleModel)
        s = yield simple_model.load("foo")
        self.assertEqual(s, None)

    @Manager.calls_manager
    def test_all_keys(self):
        simple_model = self.manager.proxy(SimpleModel)

        keys = yield self.filter_tombstones(
            simple_model, (yield simple_model.all_keys()))
        self.assertEqual(keys, [])

        yield simple_model("foo-1", a=5, b=u'1').save()
        yield simple_model("foo-2", a=5, b=u'2').save()

        keys = yield self.filter_tombstones(
            simple_model, (yield simple_model.all_keys()))
        self.assertEqual(sorted(keys), [u"foo-1", u"foo-2"])

    @Manager.calls_manager
    def test_index_keys(self):
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        keys = yield indexed_model.index_keys('a', 1)
        self.assertEqual(keys, ["foo1"])
        # We should get a list object, not an IndexPage wrapper.
        self.assertTrue(isinstance(keys, list))

        keys = yield indexed_model.index_keys('b', u"one")
        self.assertEqual(sorted(keys), ["foo1", "foo2"])

        keys = yield indexed_model.index_keys('b', None)
        self.assertEqual(keys, [])

    @Manager.calls_manager
    def test_index_keys_store_none_for_empty(self):
        self.patch(fields, "STORE_NONE_FOR_EMPTY_INDEX", True)
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        keys = yield indexed_model.index_keys('a', 1)
        self.assertEqual(keys, ["foo1"])
        # We should get a list object, not an IndexPage wrapper.
        self.assertTrue(isinstance(keys, list))

        keys = yield indexed_model.index_keys('b', u"one")
        self.assertEqual(sorted(keys), ["foo1", "foo2"])

        keys = yield indexed_model.index_keys('b', None)
        self.assertEqual(keys, ["foo3"])

    @Manager.calls_manager
    def test_index_keys_return_terms(self):
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        keys = yield indexed_model.index_keys('a', 1, return_terms=True)
        self.assertEqual(keys, [("1", "foo1")])

        keys = yield indexed_model.index_keys('b', u"one", return_terms=True)
        self.assertEqual(sorted(keys), [(u"one", "foo1"), (u"one", "foo2")])

        keys = yield indexed_model.index_keys('b', None, return_terms=True)
        self.assertEqual(list(keys), [])

    @Manager.calls_manager
    def test_index_keys_return_terms_store_none_for_empty(self):
        self.patch(fields, "STORE_NONE_FOR_EMPTY_INDEX", True)
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        keys = yield indexed_model.index_keys('a', 1, return_terms=True)
        self.assertEqual(keys, [("1", "foo1")])

        keys = yield indexed_model.index_keys('b', u"one", return_terms=True)
        self.assertEqual(sorted(keys), [(u"one", "foo1"), (u"one", "foo2")])

        keys = yield indexed_model.index_keys('b', None, return_terms=True)
        self.assertEqual(list(keys), [(u"None", "foo3")])

    @Manager.calls_manager
    def test_index_keys_range(self):
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=3, b=None).save()

        keys = yield indexed_model.index_keys('a', 1, 2)
        self.assertEqual(sorted(keys), ["foo1", "foo2"])

        keys = yield indexed_model.index_keys('a', 2, 3)
        self.assertEqual(sorted(keys), ["foo2", "foo3"])

    @Manager.calls_manager
    def test_index_keys_range_return_terms(self):
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=3, b=None).save()

        keys = yield indexed_model.index_keys('a', 1, 2, return_terms=True)
        self.assertEqual(sorted(keys), [("1", "foo1"), ("2", "foo2")])

        keys = yield indexed_model.index_keys('a', 2, 3, return_terms=True)
        self.assertEqual(sorted(keys), [("2", "foo2"), ("3", "foo3")])

    @Manager.calls_manager
    def test_all_keys_page(self):
        simple_model = self.manager.proxy(SimpleModel)

        keys_page = yield simple_model.all_keys_page()
        keys = yield self.filter_tombstones(simple_model, list(keys_page))
        self.assertEqual(keys, [])

        yield simple_model("foo-1", a=5, b=u'1').save()
        yield simple_model("foo-2", a=5, b=u'2').save()

        keys_page = yield simple_model.all_keys_page()
        keys = yield self.filter_tombstones(simple_model, list(keys_page))
        self.assertEqual(sorted(keys), [u"foo-1", u"foo-2"])

    @Manager.calls_manager
    def test_all_keys_page_multiple_pages(self):
        simple_model = self.manager.proxy(SimpleModel)

        yield simple_model("foo-1", a=5, b=u'1').save()
        yield simple_model("foo-2", a=5, b=u'2').save()

        keys = []

        # We get results in arbitrary order and we may have tombstones left
        # over from prior tests. Therefore, we iterate through all index pages
        # and assert that we have exactly one result in each page except the
        # last.
        keys_page = yield simple_model.all_keys_page(max_results=1)
        while keys_page is not None:
            keys.extend(list(keys_page))
            if keys_page.has_next_page():
                self.assertEqual(len(list(keys_page)), 1)
                keys_page = yield keys_page.next_page()
            else:
                keys_page = None

        keys = yield self.filter_tombstones(simple_model, keys)
        self.assertEqual(sorted(keys), [u"foo-1", u"foo-2"])

    @Manager.calls_manager
    def test_index_keys_page(self):
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=1, b=u"one").save()
        yield indexed_model("foo3", a=1, b=None).save()
        yield indexed_model("foo4", a=1, b=None).save()

        keys1 = yield indexed_model.index_keys_page('a', 1, max_results=2)
        self.assertEqual(sorted(keys1), ["foo1", "foo2"])
        self.assertEqual(keys1.has_next_page(), True)

        keys2 = yield keys1.next_page()
        self.assertEqual(sorted(keys2), ["foo3", "foo4"])
        self.assertEqual(keys2.has_next_page(), True)

        keys3 = yield keys2.next_page()
        self.assertEqual(sorted(keys3), [])
        self.assertEqual(keys3.has_next_page(), False)

        no_keys = yield keys3.next_page()
        self.assertEqual(no_keys, None)

    @Manager.calls_manager
    def test_index_keys_page_explicit_continuation(self):
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=1, b=u"one").save()
        yield indexed_model("foo3", a=1, b=None).save()
        yield indexed_model("foo4", a=1, b=None).save()

        keys1 = yield indexed_model.index_keys_page('a', 1, max_results=1)
        self.assertEqual(sorted(keys1), ["foo1"])
        self.assertEqual(keys1.has_next_page(), True)
        self.assertTrue(isinstance(keys1.continuation, unicode))

        keys2 = yield indexed_model.index_keys_page(
            'a', 1, max_results=2, continuation=keys1.continuation)
        self.assertEqual(sorted(keys2), ["foo2", "foo3"])
        self.assertEqual(keys2.has_next_page(), True)

        keys3 = yield keys2.next_page()
        self.assertEqual(sorted(keys3), ["foo4"])
        self.assertEqual(keys3.has_next_page(), False)

    @Manager.calls_manager
    def test_index_keys_page_none_continuation(self):
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u'one').save()

        keys1 = yield indexed_model.index_keys_page('a', 1, max_results=2)
        self.assertEqual(sorted(keys1), ['foo1'])
        self.assertEqual(keys1.has_next_page(), False)
        self.assertEqual(keys1.continuation, None)

    @Manager.calls_manager
    def test_index_keys_page_bad_continutation(self):
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u'one').save()

        try:
            yield indexed_model.index_keys_page(
                'a', 1, max_results=1, continuation='bad-id')
            self.fail('Expected VumiRiakError.')
        except VumiRiakError:
            pass

    @Manager.calls_manager
    def test_index_keys_quoting(self):
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"+one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        keys = yield indexed_model.index_keys('b', u"+one")
        self.assertEqual(sorted(keys), ["foo1"])

        keys = yield indexed_model.index_keys('b', u"one")
        self.assertEqual(sorted(keys), ["foo2"])

        keys = yield indexed_model.index_keys('b', None)
        self.assertEqual(keys, [])

    @Manager.calls_manager
    def test_index_keys_quoting_store_none_for_empty(self):
        self.patch(fields, "STORE_NONE_FOR_EMPTY_INDEX", True)
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"+one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        keys = yield indexed_model.index_keys('b', u"+one")
        self.assertEqual(sorted(keys), ["foo1"])

        keys = yield indexed_model.index_keys('b', u"one")
        self.assertEqual(sorted(keys), ["foo2"])

        keys = yield indexed_model.index_keys('b', None)
        self.assertEqual(keys, ["foo3"])

    @Manager.calls_manager
    def test_index_lookup(self):
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        lookup = indexed_model.index_lookup
        yield self.assert_mapreduce_results(["foo1"], lookup, 'a', 1)
        yield self.assert_mapreduce_results(
            ["foo1", "foo2"], lookup, 'b', u"one")
        yield self.assert_mapreduce_results([], lookup, 'b', None)

    @Manager.calls_manager
    def test_index_lookup_store_none_for_empty(self):
        self.patch(fields, "STORE_NONE_FOR_EMPTY_INDEX", True)
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        lookup = indexed_model.index_lookup
        yield self.assert_mapreduce_results(["foo1"], lookup, 'a', 1)
        yield self.assert_mapreduce_results(
            ["foo1", "foo2"], lookup, 'b', u"one")
        yield self.assert_mapreduce_results(["foo3"], lookup, 'b', None)

    @Manager.calls_manager
    def test_index_match(self):
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        match = indexed_model.index_match
        yield self.assert_mapreduce_results(
            ["foo1"], match,
            [{'key': 'b', 'pattern': 'one', 'flags': 'i'}], 'a', 1)
        yield self.assert_mapreduce_results(
            ["foo1", "foo2"], match,
            [{'key': 'b', 'pattern': 'one', 'flags': 'i'}], 'b', u"one")
        yield self.assert_mapreduce_results(
            [], match,
            [{'key': 'a', 'pattern': '2', 'flags': 'i'}], 'b', None)
        # test with non-existent key
        yield self.assert_mapreduce_results(
            [], match,
            [{'key': 'foo', 'pattern': 'one', 'flags': 'i'}], 'a', 1)
        # test case sensitivity
        yield self.assert_mapreduce_results(
            ['foo1'], match,
            [{'key': 'b', 'pattern': 'ONE', 'flags': 'i'}], 'a', 1)
        yield self.assert_mapreduce_results(
            [], match,
            [{'key': 'b', 'pattern': 'ONE', 'flags': ''}], 'a', 1)

    @Manager.calls_manager
    def test_index_match_store_none_for_empty(self):
        self.patch(fields, "STORE_NONE_FOR_EMPTY_INDEX", True)
        indexed_model = self.manager.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        match = indexed_model.index_match
        yield self.assert_mapreduce_results(
            ["foo1"], match,
            [{'key': 'b', 'pattern': 'one', 'flags': 'i'}], 'a', 1)
        yield self.assert_mapreduce_results(
            ["foo1", "foo2"], match,
            [{'key': 'b', 'pattern': 'one', 'flags': 'i'}], 'b', u"one")
        yield self.assert_mapreduce_results(
            ["foo3"], match,
            [{'key': 'a', 'pattern': '2', 'flags': 'i'}], 'b', None)
        # test with non-existent key
        yield self.assert_mapreduce_results(
            [], match,
            [{'key': 'foo', 'pattern': 'one', 'flags': 'i'}], 'a', 1)
        # test case sensitivity
        yield self.assert_mapreduce_results(
            ['foo1'], match,
            [{'key': 'b', 'pattern': 'ONE', 'flags': 'i'}], 'a', 1)
        yield self.assert_mapreduce_results(
            [], match,
            [{'key': 'b', 'pattern': 'ONE', 'flags': ''}], 'a', 1)

    @Manager.calls_manager
    def test_inherited_model(self):
        field_names = InheritedModel.field_descriptors.keys()
        self.assertEqual(sorted(field_names), ["a", "b", "c"])

        inherited_model = self.manager.proxy(InheritedModel)

        im1 = inherited_model("foo", a=1, b=u"2", c=3)
        yield im1.save()

        im2 = yield inherited_model.load("foo")
        self.assertEqual(im2.a, 1)
        self.assertEqual(im2.b, u'2')
        self.assertEqual(im2.c, 3)

    def test_overriden_model(self):
        int_field = OverriddenModel.field_descriptors['c'].field
        self.assertEqual(int_field.max, 5)
        self.assertEqual(int_field.min, 0)

        overridden_model = self.manager.proxy(OverriddenModel)

        overridden_model("foo", a=1, b=u"2", c=3)
        self.assertRaises(ValidationError, overridden_model, "foo",
                          a=1, b=u"2", c=-1)

    @Manager.calls_manager
    def test_unversioned_migration(self):
        old_model = self.manager.proxy(UnversionedModel)
        new_model = self.manager.proxy(VersionedModel)
        foo_old = old_model("foo", a=1)
        yield foo_old.save()

        foo_new = yield new_model.load("foo")
        self.assertEqual(foo_new.c, 1)
        self.assertEqual(foo_new.was_migrated, True)

    @Manager.calls_manager
    def test_unversioned_reverse_migration(self):
        old_model = self.manager.proxy(UnversionedModel)
        new_model = self.manager.proxy(VersionedModel)
        foo_new = new_model("foo", c=1)
        model_name = "%s.%s" % (
            VersionedModel.__module__, VersionedModel.__name__)
        self.manager.store_versions[model_name] = None
        yield foo_new.save()

        foo_old = yield old_model.load("foo")
        self.assertEqual(foo_old.a, 1)
        self.assertEqual(foo_old.was_migrated, False)

    @Manager.calls_manager
    def test_version_migration(self):
        old_model = self.manager.proxy(OldVersionedModel)
        new_model = self.manager.proxy(VersionedModel)
        foo_old = old_model("foo", b=1)
        yield foo_old.save()

        foo_new = yield new_model.load("foo")
        self.assertEqual(foo_new.c, 1)
        self.assertEqual(foo_new.text, "hello")
        self.assertEqual(foo_new.was_migrated, True)

    @Manager.calls_manager
    def test_version_reverse_migration(self):
        old_model = self.manager.proxy(OldVersionedModel)
        new_model = self.manager.proxy(VersionedModel)
        foo_new = new_model("foo", c=1)
        model_name = "%s.%s" % (
            VersionedModel.__module__, VersionedModel.__name__)
        self.manager.store_versions[model_name] = OldVersionedModel.VERSION
        yield foo_new.save()

        foo_old = yield old_model.load("foo")
        self.assertEqual(foo_old.b, 1)
        self.assertEqual(foo_old.was_migrated, False)

    @Manager.calls_manager
    def test_version_migration_new_index(self):
        old_model = self.manager.proxy(VersionedModel)
        new_model = self.manager.proxy(IndexedVersionedModel)
        foo_old = old_model("foo", c=1, text=u"hi")
        yield foo_old.save()

        foo_new = yield new_model.load("foo")
        self.assertEqual(foo_new.c, 1)
        self.assertEqual(foo_new.text, "hi")
        self.assertEqual(self.get_model_indexes(foo_new), {"text_bin": ["hi"]})
        self.assertEqual(foo_new.was_migrated, True)

    @Manager.calls_manager
    def test_version_reverse_migration_new_index(self):
        old_model = self.manager.proxy(VersionedModel)
        new_model = self.manager.proxy(IndexedVersionedModel)
        foo_new = new_model("foo", c=1, text=u"hi")
        model_name = "%s.%s" % (
            VersionedModel.__module__, IndexedVersionedModel.__name__)
        self.manager.store_versions[model_name] = VersionedModel.VERSION
        yield foo_new.save()

        foo_old = yield old_model.load("foo")
        self.assertEqual(foo_old.c, 1)
        self.assertEqual(foo_old.text, "hi")
        # Old indexes are no longer kept across migrations.
        self.assertEqual(self.get_model_indexes(foo_old), {})
        self.assertEqual(foo_old.was_migrated, False)

    @Manager.calls_manager
    def test_version_migration_new_index_with_unicode(self):
        old_model = self.manager.proxy(VersionedModel)
        new_model = self.manager.proxy(IndexedVersionedModel)
        foo_old = old_model("foo", c=1, text=u"hi Zoë")
        yield foo_old.save()

        foo_new = yield new_model.load("foo")
        self.assertEqual(foo_new.c, 1)
        self.assertEqual(foo_new.text, u"hi Zoë")
        self.assertEqual(
            self.get_model_indexes(foo_new), {"text_bin": ["hi Zo\xc3\xab"]})
        self.assertEqual(foo_new.was_migrated, True)

    @Manager.calls_manager
    def test_version_migration_new_index_None(self):
        old_model = self.manager.proxy(VersionedModel)
        new_model = self.manager.proxy(IndexedVersionedModel)
        foo_old = old_model("foo", c=1, text=None)
        yield foo_old.save()

        foo_new = yield new_model.load("foo")
        self.assertEqual(foo_new.c, 1)
        self.assertEqual(foo_new.text, None)
        self.assertEqual(self.get_model_indexes(foo_new), {})

    @Manager.calls_manager
    def test_version_migration_remove_index(self):
        """
        An index can be removed in a migration.
        """
        old_model = self.manager.proxy(IndexedVersionedModel)
        new_model = self.manager.proxy(IndexRemovedVersionedModel)
        foo_old = old_model("foo", c=1, text=u"hi")
        yield foo_old.save()

        foo_new = yield new_model.load("foo")
        self.assertEqual(foo_new.c, 1)
        self.assertEqual(foo_new.text, "hi")
        self.assertEqual(self.get_model_indexes(foo_new), {})
        self.assertEqual(foo_new.was_migrated, True)

    @Manager.calls_manager
    def test_version_reverse_migration_remove_index(self):
        """
        A removed index can be restored in a reverse migration.
        """
        old_model = self.manager.proxy(IndexedVersionedModel)
        new_model = self.manager.proxy(IndexRemovedVersionedModel)
        foo_new = new_model("foo", c=1, text=u"hi")
        model_name = "%s.%s" % (
            VersionedModel.__module__, IndexRemovedVersionedModel.__name__)
        self.manager.store_versions[model_name] = IndexedVersionedModel.VERSION
        yield foo_new.save()

        foo_old = yield old_model.load("foo")
        self.assertEqual(foo_old.c, 1)
        self.assertEqual(foo_old.text, "hi")
        self.assertEqual(self.get_model_indexes(foo_new), {"text_bin": ["hi"]})

    @Manager.calls_manager
    def test_version_migration_failure(self):
        odd_model = self.manager.proxy(UnknownVersionedModel)
        new_model = self.manager.proxy(VersionedModel)
        foo_odd = odd_model("foo", d=1)
        yield foo_odd.save()

        try:
            yield new_model.load("foo")
            self.fail('Expected ModelMigrationError.')
        except ModelMigrationError, e:
            self.assertEqual(
                e.args[0], 'No migrators defined for VersionedModel version 5')

    @Manager.calls_manager
    def test_dynamic_field_migration(self):
        old_model = self.manager.proxy(UnversionedDynamicModel)
        new_model = self.manager.proxy(VersionedDynamicModel)
        old = old_model("foo")
        old.keep['bar'] = u"bar-val"
        old.keep['baz'] = u"baz-val"
        old.drop['bar'] = u"drop"
        yield old.save()

        new = yield new_model.load("foo")
        self.assertEqual(new.keep['bar'], u"bar-val")
        self.assertEqual(new.keep['baz'], u"baz-val")
        self.assertFalse("bar" in new.drop)


class TestModelOnTxRiak(VumiTestCase, ModelTestMixin):

    @inlineCallbacks
    def setUp(self):
        try:
            from vumi.persist.txriak_manager import TxRiakManager
        except ImportError, e:
            import_skip(e, 'riak')
        self.manager = TxRiakManager.from_config({'bucket_prefix': 'test.'})
        self.add_cleanup(self.cleanup_manager)
        yield self.manager.purge_all()

    @inlineCallbacks
    def cleanup_manager(self):
        yield self.manager.purge_all()
        yield self.manager.close_manager()


class TestModelOnRiak(VumiTestCase, ModelTestMixin):

    def setUp(self):
        try:
            from vumi.persist.riak_manager import RiakManager
        except ImportError, e:
            import_skip(e, 'riak')

        self.manager = RiakManager.from_config({'bucket_prefix': 'test.'})
        self.add_cleanup(self.cleanup_manager)
        self.manager.purge_all()

    def cleanup_manager(self):
        self.manager.purge_all()
        self.manager.close_manager()
