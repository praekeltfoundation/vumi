# -*- coding: utf-8 -*-

"""Tests for vumi.persist.model."""

from datetime import datetime

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.persist.model import (
    Model, Manager, ModelMigrator, ModelMigrationError, VumiRiakError)
from vumi.persist.fields import (
    ValidationError, Integer, Unicode, VumiMessage, Dynamic, ListOf, SetOf,
    ForeignKey, ManyToMany, Timestamp)
from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase, import_skip


# TODO: Split up the field-specific tests and move them to ``test_fields``.


class SimpleModel(Model):
    a = Integer()
    b = Unicode()


class IndexedModel(Model):
    a = Integer(index=True)
    b = Unicode(index=True, null=True)


class VumiMessageModel(Model):
    msg = VumiMessage(TransportUserMessage)


class DynamicModel(Model):
    a = Unicode()
    contact_info = Dynamic()


class ListOfModel(Model):
    items = ListOf(Integer())


class IndexedListOfModel(Model):
    items = ListOf(Integer(), index=True)


class SetOfModel(Model):
    items = SetOf(Integer())


class IndexedSetOfModel(Model):
    items = SetOf(Integer(), index=True)


class ForeignKeyModel(Model):
    simple = ForeignKey(SimpleModel, null=True)


class ManyToManyModel(Model):
    simples = ManyToMany(SimpleModel)


class InheritedModel(SimpleModel):
    c = Integer()


class OverriddenModel(InheritedModel):
    c = Integer(min=0, max=5)


class TimestampModel(Model):
    time = Timestamp(null=True)


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


class UnknownVersionedModel(Model):
    VERSION = 4
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

    # TODO: all copies of mkmsg must be unified!
    def mkmsg(self, **kw):
        kw.setdefault("transport_name", "sphex")
        kw.setdefault("transport_type", "sphex_type")
        kw.setdefault("to_addr", "1234")
        kw.setdefault("from_addr", "5678")
        return TransportUserMessage(**kw)

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

    def test_get_data_with_foreign_key_proxy(self):
        simple_model = self.manager.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        fk_model = self.manager.proxy(ForeignKeyModel)

        f1 = fk_model("bar1")
        f1.simple.set(s1)

        self.assertEqual(f1.get_data(), {
            'key': 'bar1',
            '$VERSION': None,
            'simple': 'foo'
            })

    def test_get_data_with_many_to_many_proxy(self):
        simple_model = self.manager.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        mm_model = self.manager.proxy(ManyToManyModel)

        m1 = mm_model("bar")
        m1.simples.add(s1)
        m1.save()

        self.assertEqual(m1.get_data(), {
            'key': 'bar',
            '$VERSION': None,
            'simples': ['foo'],
            })

    def test_get_data_with_dynamic_proxy(self):
        dynamic_model = self.manager.proxy(DynamicModel)

        d1 = dynamic_model("foo", a=u"ab")
        d1.contact_info['foo'] = u'bar'
        d1.contact_info['zip'] = u'zap'

        self.assertEqual(d1.get_data(), {
            'key': 'foo',
            '$VERSION': None,
            'a': 'ab',
            'contact_info.foo': 'bar',
            'contact_info.zip': 'zap',
            })

    def test_get_data_with_list_proxy(self):
        list_model = self.manager.proxy(ListOfModel)
        l1 = list_model("foo")
        l1.items.append(1)
        l1.items.append(2)
        self.assertEqual(l1.get_data(), {
            'key': 'foo',
            '$VERSION': None,
            'items': [1, 2],
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

        keys = yield indexed_model.index_keys('b', None)
        self.assertEqual(list(keys), ["foo3"])

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
    def test_vumimessage_field(self):
        msg_model = self.manager.proxy(VumiMessageModel)
        msg = self.mkmsg(extra="bar")
        m1 = msg_model("foo", msg=msg)
        yield m1.save()

        m2 = yield msg_model.load("foo")
        self.assertEqual(m1.msg, m2.msg)
        self.assertEqual(m2.msg, msg)

        self.assertRaises(ValidationError, setattr, m1, "msg", "foo")

        # test extra keys are removed
        msg2 = self.mkmsg()
        m1.msg = msg2
        self.assertTrue("extra" not in m1.msg)

    @Manager.calls_manager
    def test_vumimessage_field_excludes_cache(self):
        msg_model = self.manager.proxy(VumiMessageModel)
        cache_attr = TransportUserMessage._CACHE_ATTRIBUTE
        msg = self.mkmsg(extra="bar")
        msg.cache["cache"] = "me"
        self.assertEqual(msg[cache_attr], {"cache": "me"})

        m1 = msg_model("foo", msg=msg)
        self.assertTrue(cache_attr not in m1.msg)
        yield m1.save()

        m2 = yield msg_model.load("foo")
        self.assertTrue(cache_attr not in m2.msg)
        self.assertEqual(m2.msg, m1.msg)

    def _create_dynamic_instance(self, dynamic_model):
        d1 = dynamic_model("foo", a=u"ab")
        d1.contact_info['cellphone'] = u"+27123"
        d1.contact_info['telephone'] = u"+2755"
        d1.contact_info['honorific'] = u"BDFL"
        return d1

    @Manager.calls_manager
    def test_dynamic_fields(self):
        dynamic_model = self.manager.proxy(DynamicModel)
        d1 = self._create_dynamic_instance(dynamic_model)
        yield d1.save()

        d2 = yield dynamic_model.load("foo")
        self.assertEqual(d2.a, u"ab")
        self.assertEqual(d2.contact_info['cellphone'], u"+27123")
        self.assertEqual(d2.contact_info['telephone'], u"+2755")
        self.assertEqual(d2.contact_info['honorific'], u"BDFL")

    def test_dynamic_field_init(self):
        dynamic_model = self.manager.proxy(DynamicModel)
        contact_info = {'cellphone': u'+27123',
                        'telephone': u'+2755'}
        d1 = dynamic_model("foo", a=u"ab", contact_info=contact_info)
        self.assertEqual(d1.contact_info.copy(), contact_info)

    def test_dynamic_field_keys(self):
        d1 = self._create_dynamic_instance(self.manager.proxy(DynamicModel))
        keys = d1.contact_info.keys()
        iterkeys = d1.contact_info.iterkeys()
        self.assertTrue(keys, list)
        self.assertTrue(hasattr(iterkeys, 'next'))
        self.assertEqual(sorted(keys), ['cellphone', 'honorific', 'telephone'])
        self.assertEqual(sorted(iterkeys), sorted(keys))

    def test_dynamic_field_values(self):
        d1 = self._create_dynamic_instance(self.manager.proxy(DynamicModel))
        values = d1.contact_info.values()
        itervalues = d1.contact_info.itervalues()
        self.assertTrue(isinstance(values, list))
        self.assertTrue(hasattr(itervalues, 'next'))
        self.assertEqual(sorted(values), ["+27123", "+2755", "BDFL"])
        self.assertEqual(sorted(itervalues), sorted(values))

    def test_dynamic_field_items(self):
        d1 = self._create_dynamic_instance(self.manager.proxy(DynamicModel))
        items = d1.contact_info.items()
        iteritems = d1.contact_info.iteritems()
        self.assertTrue(isinstance(items, list))
        self.assertTrue(hasattr(iteritems, 'next'))
        self.assertEqual(sorted(items), [('cellphone', "+27123"),
                                         ('honorific', "BDFL"),
                                         ('telephone', "+2755")])
        self.assertEqual(sorted(iteritems), sorted(items))

    def test_dynamic_field_clear(self):
        d1 = self._create_dynamic_instance(self.manager.proxy(DynamicModel))
        d1.contact_info.clear()
        self.assertEqual(d1.contact_info.keys(), [])

    def test_dynamic_field_update(self):
        d1 = self._create_dynamic_instance(self.manager.proxy(DynamicModel))
        d1.contact_info.update({"cellphone": "123", "name": "foo"})
        self.assertEqual(sorted(d1.contact_info.items()), [
            ('cellphone', "123"), ('honorific', "BDFL"), ('name', "foo"),
            ('telephone', "+2755")])

    def test_dynamic_field_contains(self):
        d1 = self._create_dynamic_instance(self.manager.proxy(DynamicModel))
        self.assertTrue("cellphone" in d1.contact_info)
        self.assertFalse("landline" in d1.contact_info)

    def test_dynamic_field_del(self):
        d1 = self._create_dynamic_instance(self.manager.proxy(DynamicModel))
        del d1.contact_info["telephone"]
        self.assertEqual(sorted(d1.contact_info.keys()),
                         ['cellphone', 'honorific'])

    def test_dynamic_field_setting(self):
        d1 = self._create_dynamic_instance(self.manager.proxy(DynamicModel))
        d1.contact_info = {u'cellphone': u'789', u'name': u'foo'}
        self.assertEqual(sorted(d1.contact_info.items()), [
            (u'cellphone', u'789'),
            (u'name', u'foo'),
        ])

    @Manager.calls_manager
    def test_listof_fields(self):
        list_model = self.manager.proxy(ListOfModel)
        l1 = list_model("foo")
        l1.items.append(1)
        l1.items.append(2)
        yield l1.save()

        l2 = yield list_model.load("foo")
        self.assertEqual(l2.items[0], 1)
        self.assertEqual(l2.items[1], 2)
        self.assertEqual(list(l2.items), [1, 2])

        l2.items[0] = 5
        self.assertEqual(l2.items[0], 5)

        del l2.items[0]
        self.assertEqual(list(l2.items), [2])

        l2.items.append(5)
        self.assertEqual(list(l2.items), [2, 5])
        l2.items.remove(5)
        self.assertEqual(list(l2.items), [2])

        l2.items.extend([3, 4, 5])
        self.assertEqual(list(l2.items), [2, 3, 4, 5])

        l2.items = [1]
        self.assertEqual(list(l2.items), [1])

    @Manager.calls_manager
    def test_listof_fields_indexes(self):
        list_model = self.manager.proxy(IndexedListOfModel)
        l1 = list_model("foo")
        l1.items.append(1)
        l1.items.append(2)
        yield l1.save()

        assert_indexes = lambda mdl, values: self.assertEqual(
            mdl._riak_object.get_indexes(),
            set(('items_bin', str(v)) for v in values))

        l2 = yield list_model.load("foo")
        self.assertEqual(l2.items[0], 1)
        self.assertEqual(l2.items[1], 2)
        self.assertEqual(list(l2.items), [1, 2])
        assert_indexes(l2, [1, 2])

        l2.items[0] = 5
        self.assertEqual(l2.items[0], 5)
        assert_indexes(l2, [2, 5])

        del l2.items[0]
        self.assertEqual(list(l2.items), [2])
        assert_indexes(l2, [2])

        l2.items.append(5)
        self.assertEqual(list(l2.items), [2, 5])
        assert_indexes(l2, [2, 5])
        l2.items.remove(5)
        self.assertEqual(list(l2.items), [2])
        assert_indexes(l2, [2])

        l2.items.extend([3, 4, 5])
        self.assertEqual(list(l2.items), [2, 3, 4, 5])
        assert_indexes(l2, [2, 3, 4, 5])

        l2.items = [1]
        self.assertEqual(list(l2.items), [1])
        assert_indexes(l2, [1])

    def test_listof_setting(self):
        list_model = self.manager.proxy(ListOfModel)
        l1 = list_model("foo")
        l1.items = [7, 8, 9]
        self.assertEqual(list(l1.items), [7, 8, 9])

    @Manager.calls_manager
    def test_setof_fields(self):
        set_model = self.manager.proxy(SetOfModel)
        m1 = set_model("foo")
        m1.items.add(1)
        m1.items.add(2)
        yield m1.save()

        m2 = yield set_model.load("foo")
        self.assertTrue(1 in m2.items)
        self.assertTrue(2 in m2.items)
        self.assertEqual(set(m2.items), set([1, 2]))

        m2.items.add(5)
        self.assertTrue(5 in m2.items)

        m2.items.remove(1)
        self.assertTrue(1 not in m2.items)
        self.assertRaises(KeyError, m2.items.remove, 1)

        m2.items.add(1)
        m2.items.discard(1)
        self.assertTrue(1 not in m2.items)
        m2.items.discard(1)
        self.assertTrue(1 not in m2.items)

        m2.items.update([3, 4, 5])
        self.assertEqual(set(m2.items), set([2, 3, 4, 5]))

        m2.items = set([7, 8])
        self.assertEqual(set(m2.items), set([7, 8]))

    @Manager.calls_manager
    def test_setof_fields_indexes(self):
        set_model = self.manager.proxy(IndexedSetOfModel)
        m1 = set_model("foo")
        m1.items.add(1)
        m1.items.add(2)
        yield m1.save()

        assert_indexes = lambda mdl, values: self.assertEqual(
            mdl._riak_object.get_indexes(),
            set(('items_bin', str(v)) for v in values))

        m2 = yield set_model.load("foo")
        self.assertTrue(1 in m2.items)
        self.assertTrue(2 in m2.items)
        self.assertEqual(set(m2.items), set([1, 2]))
        assert_indexes(m2, [1, 2])

        m2.items.add(5)
        self.assertTrue(5 in m2.items)
        assert_indexes(m2, [1, 2, 5])

        m2.items.remove(1)
        self.assertTrue(1 not in m2.items)
        assert_indexes(m2, [2, 5])

        m2.items.add(1)
        m2.items.discard(1)
        self.assertTrue(1 not in m2.items)
        assert_indexes(m2, [2, 5])
        m2.items.discard(1)
        self.assertTrue(1 not in m2.items)
        assert_indexes(m2, [2, 5])

        m2.items.update([3, 4, 5])
        self.assertEqual(set(m2.items), set([2, 3, 4, 5]))
        assert_indexes(m2, [2, 3, 4, 5])

        m2.items = set([7, 8])
        self.assertEqual(set(m2.items), set([7, 8]))
        assert_indexes(m2, [7, 8])

    def test_setof_fields_validation(self):
        set_model = self.manager.proxy(SetOfModel)
        m1 = set_model("foo")

        self.assertRaises(ValidationError, m1.items.add, "foo")
        self.assertRaises(ValidationError, m1.items.remove, "foo")
        self.assertRaises(ValidationError, m1.items.discard, "foo")
        self.assertRaises(ValidationError, m1.items.update, set(["foo"]))

    @Manager.calls_manager
    def test_foreignkey_fields(self):
        fk_model = self.manager.proxy(ForeignKeyModel)
        simple_model = self.manager.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        f1 = fk_model("bar")
        f1.simple.set(s1)
        yield s1.save()
        yield f1.save()
        self.assertEqual(f1._riak_object.get_data()['simple'], s1.key)

        f2 = yield fk_model.load("bar")
        s2 = yield f2.simple.get()

        self.assertEqual(f2.simple.key, "foo")
        self.assertEqual(s2.a, 5)
        self.assertEqual(s2.b, u"3")

        f2.simple.set(None)
        s3 = yield f2.simple.get()
        self.assertEqual(s3, None)

        f2.simple.key = "foo"
        s4 = yield f2.simple.get()
        self.assertEqual(s4.key, "foo")

        f2.simple.key = None
        s5 = yield f2.simple.get()
        self.assertEqual(s5, None)

        self.assertRaises(ValidationError, f2.simple.set, object())

    @Manager.calls_manager
    def test_old_foreignkey_fields(self):
        fk_model = self.manager.proxy(ForeignKeyModel)
        simple_model = self.manager.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        f1 = fk_model("bar")
        # Create index directly and remove data field to simulate old-style
        # index-only implementation
        f1._riak_object.add_index('simple_bin', s1.key)
        data = f1._riak_object.get_data()
        data.pop('simple')
        f1._riak_object.set_data(data)
        yield s1.save()
        yield f1.save()

        f2 = yield fk_model.load("bar")
        s2 = yield f2.simple.get()

        self.assertEqual(f2.simple.key, "foo")
        self.assertEqual(s2.a, 5)
        self.assertEqual(s2.b, u"3")

        f2.simple.set(None)
        s3 = yield f2.simple.get()
        self.assertEqual(s3, None)

        f2.simple.key = "foo"
        s4 = yield f2.simple.get()
        self.assertEqual(s4.key, "foo")

        f2.simple.key = None
        s5 = yield f2.simple.get()
        self.assertEqual(s5, None)

        self.assertRaises(ValidationError, f2.simple.set, object())

    @Manager.calls_manager
    def test_reverse_foreignkey_fields(self):
        """
        When we declare a ForeignKey field, we add both a paginated index
        lookup method and a legacy non-paginated index lookup method to the
        foreign model's backlinks attribute.
        """
        fk_model = self.manager.proxy(ForeignKeyModel)
        simple_model = self.manager.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        f1 = fk_model("bar1")
        f1.simple.set(s1)
        f2 = fk_model("bar2")
        f2.simple.set(s1)
        yield s1.save()
        yield f1.save()
        yield f2.save()

        s2 = yield simple_model.load("foo")
        results = yield s2.backlinks.foreignkeymodels()
        self.assertEqual(sorted(results), ["bar1", "bar2"])

        results_p1 = yield s2.backlinks.foreignkeymodel_keys()
        self.assertEqual(sorted(results_p1), ["bar1", "bar2"])
        self.assertEqual(results_p1.has_next_page(), False)

    @Manager.calls_manager
    def load_all_bunches_flat(self, m2m_field):
        results = []
        for result_bunch in m2m_field.load_all_bunches():
            results.extend((yield result_bunch))
        returnValue(results)

    @Manager.calls_manager
    def test_manytomany_field(self):
        mm_model = self.manager.proxy(ManyToManyModel)
        simple_model = self.manager.proxy(SimpleModel)

        s1 = simple_model("foo", a=5, b=u'3')
        m1 = mm_model("bar")
        m1.simples.add(s1)
        yield s1.save()
        yield m1.save()
        self.assertEqual(m1._riak_object.get_data()['simples'], [s1.key])

        m2 = yield mm_model.load("bar")
        [s2] = yield self.load_all_bunches_flat(m2.simples)

        self.assertEqual(m2.simples.keys(), ["foo"])
        self.assertEqual(s2.a, 5)
        self.assertEqual(s2.b, u"3")

        m2.simples.remove(s2)
        simples = yield self.load_all_bunches_flat(m2.simples)
        self.assertEqual(simples, [])

        m2.simples.add_key("foo")
        [s4] = yield self.load_all_bunches_flat(m2.simples)
        self.assertEqual(s4.key, "foo")

        m2.simples.remove_key("foo")
        simples = yield self.load_all_bunches_flat(m2.simples)
        self.assertEqual(simples, [])

        self.assertRaises(ValidationError, m2.simples.add, object())
        self.assertRaises(ValidationError, m2.simples.remove, object())

        t1 = simple_model("bar1", a=3, b=u'4')
        t2 = simple_model("bar2", a=4, b=u'4')
        m2.simples.add(t1)
        m2.simples.add(t2)
        yield t1.save()
        yield t2.save()
        simples = yield self.load_all_bunches_flat(m2.simples)
        simples.sort(key=lambda s: s.key)
        self.assertEqual([s.key for s in simples], ["bar1", "bar2"])
        self.assertEqual(simples[0].a, 3)
        self.assertEqual(simples[1].a, 4)

        m2.simples.clear()
        m2.simples.add_key("unknown")
        self.assertEqual([], (yield self.load_all_bunches_flat(m2.simples)))

    @Manager.calls_manager
    def test_old_manytomany_field(self):
        mm_model = self.manager.proxy(ManyToManyModel)
        simple_model = self.manager.proxy(SimpleModel)

        s1 = simple_model("foo", a=5, b=u'3')
        m1 = mm_model("bar")
        # Create index directly to simulate old-style index-only implementation
        m1._riak_object.add_index('simples_bin', s1.key)
        # Manually remove the entry from the data dict to allow it to be
        # set from the index value in descriptor.clean()
        data = m1._riak_object.get_data()
        data.pop('simples')
        m1._riak_object.set_data(data)

        yield s1.save()
        yield m1.save()

        m2 = yield mm_model.load("bar")
        [s2] = yield self.load_all_bunches_flat(m2.simples)

        self.assertEqual(m2.simples.keys(), ["foo"])
        self.assertEqual(s2.a, 5)
        self.assertEqual(s2.b, u"3")

        m2.simples.remove(s2)
        simples = yield self.load_all_bunches_flat(m2.simples)
        self.assertEqual(simples, [])

        m2.simples.add_key("foo")
        [s4] = yield self.load_all_bunches_flat(m2.simples)
        self.assertEqual(s4.key, "foo")

        m2.simples.remove_key("foo")
        simples = yield self.load_all_bunches_flat(m2.simples)
        self.assertEqual(simples, [])

        self.assertRaises(ValidationError, m2.simples.add, object())
        self.assertRaises(ValidationError, m2.simples.remove, object())

        t1 = simple_model("bar1", a=3, b=u'4')
        t2 = simple_model("bar2", a=4, b=u'4')
        m2.simples.add(t1)
        m2.simples.add(t2)
        yield t1.save()
        yield t2.save()
        simples = yield self.load_all_bunches_flat(m2.simples)
        simples.sort(key=lambda s: s.key)
        self.assertEqual([s.key for s in simples], ["bar1", "bar2"])
        self.assertEqual(simples[0].a, 3)
        self.assertEqual(simples[1].a, 4)

        m2.simples.clear()
        m2.simples.add_key("unknown")
        self.assertEqual([], (yield self.load_all_bunches_flat(m2.simples)))

    @Manager.calls_manager
    def test_reverse_manytomany_fields(self):
        mm_model = self.manager.proxy(ManyToManyModel)
        simple_model = self.manager.proxy(SimpleModel)
        s1 = simple_model("foo1", a=5, b=u'3')
        s2 = simple_model("foo2", a=4, b=u'4')
        m1 = mm_model("bar1")
        m1.simples.add(s1)
        m1.simples.add(s2)
        m2 = mm_model("bar2")
        m2.simples.add(s1)
        yield s1.save()
        yield s2.save()
        yield m1.save()
        yield m2.save()

        s1 = yield simple_model.load("foo1")
        results = yield s1.backlinks.manytomanymodels()
        self.assertEqual(sorted(results), ["bar1", "bar2"])

        results_p1 = yield s1.backlinks.manytomanymodel_keys()
        self.assertEqual(sorted(results_p1), ["bar1", "bar2"])
        self.assertEqual(results_p1.has_next_page(), False)

        s2 = yield simple_model.load("foo2")
        results = yield s2.backlinks.manytomanymodels()
        self.assertEqual(sorted(results), ["bar1"])

        results_p1 = yield s2.backlinks.manytomanymodel_keys()
        self.assertEqual(sorted(results_p1), ["bar1"])
        self.assertEqual(results_p1.has_next_page(), False)

    def test_timestamp_field_setting(self):
        timestamp_model = self.manager.proxy(TimestampModel)
        t = timestamp_model("foo")

        now = datetime.now()
        t.time = now
        self.assertEqual(t.time, now)

        t.time = u"2007-01-25T12:00:00Z"
        self.assertEqual(t.time, datetime(2007, 01, 25, 12, 0))

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
        # Old indexes are kept across migrations.
        self.assertEqual(self.get_model_indexes(foo_old), {"text_bin": ["hi"]})
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
                e.args[0], 'No migrators defined for VersionedModel version 4')

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
        self.add_cleanup(self.manager.purge_all)
        yield self.manager.purge_all()


class TestModelOnRiak(VumiTestCase, ModelTestMixin):

    def setUp(self):
        try:
            from vumi.persist.riak_manager import RiakManager
        except ImportError, e:
            import_skip(e, 'riak')

        self.manager = RiakManager.from_config({'bucket_prefix': 'test.'})
        self.manager.purge_all()
