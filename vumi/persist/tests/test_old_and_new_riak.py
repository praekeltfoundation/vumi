# -*- coding: utf-8 -*-

"""Tests for vumi.persist.model."""

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.persist.model import (
    Model, Manager, ModelMigrator, ModelMigrationError)
from vumi.persist.fields import (
    ValidationError, Integer, Unicode, VumiMessage, Dynamic, ListOf,
    ForeignKey, ManyToMany, Timestamp)
from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase


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


class TestOldAndNew(VumiTestCase):

    # TODO: all copies of mkmsg must be unified!
    def mkmsg(self, **kw):
        kw.setdefault("transport_name", "sphex")
        kw.setdefault("transport_type", "sphex_type")
        kw.setdefault("to_addr", "1234")
        kw.setdefault("from_addr", "5678")
        return TransportUserMessage(**kw)

    def set_up_managers(self):
        from vumi.persist.riak_manager import RiakManager
        from vumi.persist.txriak_manager import TxRiakManager
        self.manager = TxRiakManager.from_config({'bucket_prefix': 'test.'})
        self.manager1 = self.manager
        self.manager2 = RiakManager.from_config({'bucket_prefix': 'test.'})

    @inlineCallbacks
    def setUp(self):
        self.set_up_managers()
        self.add_cleanup(self.manager.purge_all)
        yield self.manager.purge_all()

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

    @inlineCallbacks
    def assert_mapreduce_results(self, expected_keys, mr_func, *args, **kw):
        keys = yield mr_func(*args, **kw).get_keys()
        count = yield mr_func(*args, **kw).get_count()
        self.assertEqual(expected_keys, sorted(keys))
        self.assertEqual(len(expected_keys), count)

    @Manager.calls_manager
    def test_simple_search(self):
        simple_model = self.manager1.proxy(SimpleModel)
        yield simple_model.enable_search()
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        yield simple_model("three", a=2, b=u'ghi').save()

        alt_simple_model = self.manager2.proxy(SimpleModel)
        search = alt_simple_model.search
        yield self.assert_mapreduce_results(["one"], search, a=1)
        yield self.assert_mapreduce_results(["two"], search, a=2, b='def')
        yield self.assert_mapreduce_results(["three", "two"], search, a=2)

    @Manager.calls_manager
    def test_simple_search_escaping(self):
        simple_model = self.manager1.proxy(SimpleModel)
        search = simple_model.search
        yield simple_model.enable_search()
        yield simple_model("one", a=1, b=u'a\'bc').save()

        alt_simple_model = self.manager2.proxy(SimpleModel)
        search = alt_simple_model.search
        yield self.assert_mapreduce_results([], search, b=" OR a:1")
        yield self.assert_mapreduce_results([], search, b="b' OR a:1 '")
        yield self.assert_mapreduce_results(["one"], search, b="a\'bc")

    @Manager.calls_manager
    def test_simple_raw_search(self):
        simple_model = self.manager1.proxy(SimpleModel)
        yield simple_model.enable_search()
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        yield simple_model("three", a=2, b=u'ghi').save()

        alt_simple_model = self.manager2.proxy(SimpleModel)
        search = alt_simple_model.raw_search
        yield self.assert_mapreduce_results(["one"], search, 'a:1')
        yield self.assert_mapreduce_results(["two"], search, 'a:2 AND b:def')
        yield self.assert_mapreduce_results(
            ["one", "two"], search, 'b:abc OR b:def')
        yield self.assert_mapreduce_results(["three", "two"], search, 'a:2')

    @Manager.calls_manager
    def test_load_all_bunches(self):
        self.assertFalse(self.manager1.USE_MAPREDUCE_BUNCH_LOADING)
        simple_model = self.manager1.proxy(SimpleModel)
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        yield simple_model("three", a=2, b=u'ghi').save()

        alt_simple_model = self.manager2.proxy(SimpleModel)
        objs_iter = alt_simple_model.load_all_bunches(['one', 'two', 'bad'])
        objs = []
        for obj_bunch in objs_iter:
            objs.extend((yield obj_bunch))
        self.assertEqual(["one", "two"], sorted(obj.key for obj in objs))

    @Manager.calls_manager
    def test_load_all_bunches_skips_tombstones(self):
        self.assertFalse(self.manager1.USE_MAPREDUCE_BUNCH_LOADING)
        simple_model = self.manager1.proxy(SimpleModel)
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        tombstone = yield simple_model("tombstone", a=2, b=u'ghi').save()
        yield tombstone.delete()

        alt_simple_model = self.manager2.proxy(SimpleModel)
        objs_iter = alt_simple_model.load_all_bunches(
            ['one', 'two', 'tombstone'])
        objs = []
        for obj_bunch in objs_iter:
            objs.extend((yield obj_bunch))
        self.assertEqual(["one", "two"], sorted(obj.key for obj in objs))

    @Manager.calls_manager
    def test_load_all_bunches_mapreduce(self):
        self.manager1.USE_MAPREDUCE_BUNCH_LOADING = True
        simple_model = self.manager1.proxy(SimpleModel)
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        yield simple_model("three", a=2, b=u'ghi').save()

        alt_simple_model = self.manager2.proxy(SimpleModel)
        objs_iter = alt_simple_model.load_all_bunches(['one', 'two', 'bad'])
        objs = []
        for obj_bunch in objs_iter:
            objs.extend((yield obj_bunch))
        self.assertEqual(["one", "two"], sorted(obj.key for obj in objs))

    @Manager.calls_manager
    def test_load_all_bunches_mapreduce_skips_tombstones(self):
        self.manager1.USE_MAPREDUCE_BUNCH_LOADING = True
        simple_model = self.manager1.proxy(SimpleModel)
        yield simple_model("one", a=1, b=u'abc').save()
        yield simple_model("two", a=2, b=u'def').save()
        tombstone = yield simple_model("tombstone", a=2, b=u'ghi').save()
        yield tombstone.delete()

        alt_simple_model = self.manager2.proxy(SimpleModel)
        objs_iter = alt_simple_model.load_all_bunches(
            ['one', 'two', 'tombstone'])
        objs = []
        for obj_bunch in objs_iter:
            objs.extend((yield obj_bunch))
        self.assertEqual(["one", "two"], sorted(obj.key for obj in objs))

    @Manager.calls_manager
    def test_simple_instance(self):
        simple_model = self.manager1.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        yield s1.save()

        alt_simple_model = self.manager2.proxy(SimpleModel)
        s2 = yield alt_simple_model.load("foo")
        self.assertEqual(s2.a, 5)
        self.assertEqual(s2.b, u'3')
        self.assertEqual(s2.was_migrated, False)

    @Manager.calls_manager
    def test_simple_instance_delete(self):
        simple_model = self.manager1.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        yield s1.save()

        alt_simple_model = self.manager2.proxy(SimpleModel)
        s2 = yield alt_simple_model.load("foo")
        yield s2.delete()

        s3 = yield simple_model.load("foo")
        self.assertEqual(s3, None)
        s4 = yield alt_simple_model.load("foo")
        self.assertEqual(s4, None)

    @Manager.calls_manager
    def test_all_keys(self):
        simple_model = self.manager1.proxy(SimpleModel)

        keys = yield self.filter_tombstones(
            simple_model, (yield simple_model.all_keys()))
        self.assertEqual(keys, [])

        yield simple_model("foo-1", a=5, b=u'1').save()
        yield simple_model("foo-2", a=5, b=u'2').save()

        alt_simple_model = self.manager1.proxy(SimpleModel)
        keys = yield self.filter_tombstones(
            alt_simple_model, (yield alt_simple_model.all_keys()))
        self.assertEqual(sorted(keys), [u"foo-1", u"foo-2"])

    @Manager.calls_manager
    def test_index_keys(self):
        indexed_model = self.manager1.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        alt_indexed_model = self.manager2.proxy(IndexedModel)
        keys = yield alt_indexed_model.index_keys('a', 1)
        self.assertEqual(keys, ["foo1"])

        keys = yield alt_indexed_model.index_keys('b', u"one")
        self.assertEqual(sorted(keys), ["foo1", "foo2"])

        keys = yield alt_indexed_model.index_keys('b', None)
        self.assertEqual(keys, ["foo3"])

    @Manager.calls_manager
    def test_index_keys_quoting(self):
        indexed_model = self.manager1.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"+one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        alt_indexed_model = self.manager2.proxy(IndexedModel)
        keys = yield alt_indexed_model.index_keys('b', u"+one")
        self.assertEqual(sorted(keys), ["foo1"])

        keys = yield alt_indexed_model.index_keys('b', u"one")
        self.assertEqual(sorted(keys), ["foo2"])

        keys = yield alt_indexed_model.index_keys('b', None)
        self.assertEqual(keys, ["foo3"])

    @Manager.calls_manager
    def test_index_lookup(self):
        indexed_model = self.manager1.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        alt_indexed_model = self.manager2.proxy(IndexedModel)
        lookup = alt_indexed_model.index_lookup
        yield self.assert_mapreduce_results(["foo1"], lookup, 'a', 1)
        yield self.assert_mapreduce_results(
            ["foo1", "foo2"], lookup, 'b', u"one")
        yield self.assert_mapreduce_results(["foo3"], lookup, 'b', None)

    @Manager.calls_manager
    def test_index_match(self):
        indexed_model = self.manager1.proxy(IndexedModel)
        yield indexed_model("foo1", a=1, b=u"one").save()
        yield indexed_model("foo2", a=2, b=u"one").save()
        yield indexed_model("foo3", a=2, b=None).save()

        alt_indexed_model = self.manager2.proxy(IndexedModel)
        match = alt_indexed_model.index_match
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
        msg_model = self.manager1.proxy(VumiMessageModel)
        msg = self.mkmsg(extra="bar")
        m1 = msg_model("foo", msg=msg)
        yield m1.save()

        alt_msg_model = self.manager2.proxy(VumiMessageModel)
        m2 = yield alt_msg_model.load("foo")
        self.assertEqual(m1.msg, m2.msg)
        self.assertEqual(m2.msg, msg)

        self.assertRaises(ValidationError, setattr, m1, "msg", "foo")

        # test extra keys are removed
        msg2 = self.mkmsg()
        m1.msg = msg2
        self.assertTrue("extra" not in m1.msg)

    def _create_dynamic_instance(self, dynamic_model):
        d1 = dynamic_model("foo", a=u"ab")
        d1.contact_info['cellphone'] = u"+27123"
        d1.contact_info['telephone'] = u"+2755"
        d1.contact_info['honorific'] = u"BDFL"
        return d1

    @Manager.calls_manager
    def test_dynamic_fields(self):
        dynamic_model = self.manager1.proxy(DynamicModel)
        d1 = self._create_dynamic_instance(dynamic_model)
        yield d1.save()

        alt_dynamic_model = self.manager2.proxy(DynamicModel)
        d2 = yield alt_dynamic_model.load("foo")
        self.assertEqual(d2.a, u"ab")
        self.assertEqual(d2.contact_info['cellphone'], u"+27123")
        self.assertEqual(d2.contact_info['telephone'], u"+2755")
        self.assertEqual(d2.contact_info['honorific'], u"BDFL")

    @Manager.calls_manager
    def test_listof_fields(self):
        list_model = self.manager1.proxy(ListOfModel)
        l1 = list_model("foo")
        l1.items.append(1)
        l1.items.append(2)
        yield l1.save()

        alt_list_model = self.manager2.proxy(ListOfModel)
        l2 = yield alt_list_model.load("foo")
        self.assertEqual(l2.items[0], 1)
        self.assertEqual(l2.items[1], 2)
        self.assertEqual(list(l2.items), [1, 2])

        l2.items[0] = 5
        self.assertEqual(l2.items[0], 5)

        del l2.items[0]
        self.assertEqual(list(l2.items), [2])

        l2.items.extend([3, 4, 5])
        self.assertEqual(list(l2.items), [2, 3, 4, 5])

        l2.items = [1]
        self.assertEqual(list(l2.items), [1])

    @Manager.calls_manager
    def test_foreignkey_fields(self):
        fk_model = self.manager1.proxy(ForeignKeyModel)
        simple_model = self.manager1.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        f1 = fk_model("bar")
        f1.simple.set(s1)
        yield s1.save()
        yield f1.save()
        self.assertEqual(f1._riak_object.get_data()['simple'], s1.key)

        alt_fk_model = self.manager2.proxy(ForeignKeyModel)
        f2 = yield alt_fk_model.load("bar")
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
        fk_model = self.manager1.proxy(ForeignKeyModel)
        simple_model = self.manager1.proxy(SimpleModel)
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

        alt_fk_model = self.manager2.proxy(ForeignKeyModel)
        f2 = yield alt_fk_model.load("bar")
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
        fk_model = self.manager1.proxy(ForeignKeyModel)
        simple_model = self.manager1.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        f1 = fk_model("bar1")
        f1.simple.set(s1)
        f2 = fk_model("bar2")
        f2.simple.set(s1)
        yield s1.save()
        yield f1.save()
        yield f2.save()

        alt_simple_model = self.manager2.proxy(SimpleModel)
        s2 = yield alt_simple_model.load("foo")
        results = yield s2.backlinks.foreignkeymodels()
        self.assertEqual(sorted(results), ["bar1", "bar2"])

    @Manager.calls_manager
    def load_all_bunches_flat(self, m2m_field):
        results = []
        for result_bunch in m2m_field.load_all_bunches():
            results.extend((yield result_bunch))
        returnValue(results)

    @Manager.calls_manager
    def test_manytomany_field(self):
        mm_model = self.manager1.proxy(ManyToManyModel)
        simple_model = self.manager1.proxy(SimpleModel)

        s1 = simple_model("foo", a=5, b=u'3')
        m1 = mm_model("bar")
        m1.simples.add(s1)
        yield s1.save()
        yield m1.save()
        self.assertEqual(m1._riak_object.get_data()['simples'], [s1.key])

        alt_mm_model = self.manager2.proxy(ManyToManyModel)
        m2 = yield alt_mm_model.load("bar")
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

        alt_simple_model = self.manager2.proxy(SimpleModel)
        t1 = alt_simple_model("bar1", a=3, b=u'4')
        t2 = alt_simple_model("bar2", a=4, b=u'4')
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
        mm_model = self.manager1.proxy(ManyToManyModel)
        simple_model = self.manager1.proxy(SimpleModel)

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

        alt_mm_model = self.manager2.proxy(ManyToManyModel)
        m2 = yield alt_mm_model.load("bar")
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

        alt_simple_model = self.manager2.proxy(SimpleModel)
        t1 = alt_simple_model("bar1", a=3, b=u'4')
        t2 = alt_simple_model("bar2", a=4, b=u'4')
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
        mm_model = self.manager1.proxy(ManyToManyModel)
        simple_model = self.manager1.proxy(SimpleModel)
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

        alt_simple_model = self.manager2.proxy(SimpleModel)
        s1 = yield alt_simple_model.load("foo1")
        results = yield s1.backlinks.manytomanymodels()
        self.assertEqual(sorted(results), ["bar1", "bar2"])

        s2 = yield alt_simple_model.load("foo2")
        results = yield s2.backlinks.manytomanymodels()
        self.assertEqual(sorted(results), ["bar1"])

    @Manager.calls_manager
    def test_unversioned_migration(self):
        old_model = self.manager1.proxy(UnversionedModel)
        new_model = self.manager2.proxy(VersionedModel)
        foo_old = old_model("foo", a=1)
        yield foo_old.save()

        foo_new = yield new_model.load("foo")
        self.assertEqual(foo_new.c, 1)
        self.assertEqual(foo_new.was_migrated, True)

    @Manager.calls_manager
    def test_version_migration(self):
        old_model = self.manager1.proxy(OldVersionedModel)
        new_model = self.manager2.proxy(VersionedModel)
        foo_old = old_model("foo", b=1)
        yield foo_old.save()

        foo_new = yield new_model.load("foo")
        self.assertEqual(foo_new.c, 1)
        self.assertEqual(foo_new.text, "hello")
        self.assertEqual(foo_new.was_migrated, True)

    @Manager.calls_manager
    def test_version_migration_new_index(self):
        old_model = self.manager1.proxy(VersionedModel)
        new_model = self.manager2.proxy(IndexedVersionedModel)
        foo_old = old_model("foo", c=1, text=u"hi")
        yield foo_old.save()

        foo_new = yield new_model.load("foo")
        self.assertEqual(foo_new.c, 1)
        self.assertEqual(foo_new.text, "hi")
        self.assertEqual(self.get_model_indexes(foo_new), {"text_bin": ["hi"]})
        self.assertEqual(foo_new.was_migrated, True)

    @Manager.calls_manager
    def test_version_migration_new_index_with_unicode(self):
        old_model = self.manager1.proxy(VersionedModel)
        new_model = self.manager2.proxy(IndexedVersionedModel)
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
        old_model = self.manager1.proxy(VersionedModel)
        new_model = self.manager2.proxy(IndexedVersionedModel)
        foo_old = old_model("foo", c=1, text=None)
        yield foo_old.save()

        foo_new = yield new_model.load("foo")
        self.assertEqual(foo_new.c, 1)
        self.assertEqual(foo_new.text, None)
        self.assertEqual(self.get_model_indexes(foo_new), {})

    @Manager.calls_manager
    def test_version_migration_failure(self):
        odd_model = self.manager1.proxy(UnknownVersionedModel)
        new_model = self.manager2.proxy(VersionedModel)
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
        old_model = self.manager1.proxy(UnversionedDynamicModel)
        new_model = self.manager2.proxy(VersionedDynamicModel)
        old = old_model("foo")
        old.keep['bar'] = u"bar-val"
        old.keep['baz'] = u"baz-val"
        old.drop['bar'] = u"drop"
        yield old.save()

        new = yield new_model.load("foo")
        self.assertEqual(new.keep['bar'], u"bar-val")
        self.assertEqual(new.keep['baz'], u"baz-val")
        self.assertFalse("bar" in new.drop)


class TestNewAndOld(TestOldAndNew):

    def set_up_managers(self):
        from vumi.persist.riak_manager import RiakManager
        from vumi.persist.txriak_manager import TxRiakManager
        self.manager = TxRiakManager.from_config({'bucket_prefix': 'test.'})
        self.manager1 = RiakManager.from_config({'bucket_prefix': 'test.'})
        self.manager2 = self.manager
