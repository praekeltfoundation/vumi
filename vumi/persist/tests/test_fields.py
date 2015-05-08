# -*- coding: utf-8 -*-

"""Tests for vumi.persist.fields."""

from datetime import datetime
from functools import wraps

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import Message, TransportUserMessage
from vumi.persist.fields import (
    ValidationError, Field, Integer, Unicode, Tag, Timestamp, Json,
    ListOf, SetOf, Dynamic, FieldWithSubtype, Boolean, VumiMessage,
    ForeignKey, ManyToMany)
from vumi.persist.model import Manager, Model
from vumi.tests.helpers import VumiTestCase, MessageHelper, import_skip


def needs_riak(method):
    """
    Mark a test method as needing Riak setup.
    """
    method.needs_riak = True
    return method


class ModelFieldTestsDecorator(object):
    """
    Class decorator for replacing `@needs_riak`-marked test methods with two
    wrapped versions, one for each Riak manager.

    This is used here instead of the more usual mechanism of a mixin and two
    subclasses because we have a lot of small test classes which have several
    test methods that don't need Riak.
    """

    def __call__(deco, cls):
        """
        Find all methods on the the given class that are marked with
        `@needs_riak` and replace them with wrapped versions for both
        RiakManager and TxRiakManager.
        """
        # We can't use `inspect.getmembers()` because of a bug in Python 2.6
        # around empty slots: http://bugs.python.org/issue1162154
        needs_riak_methods = []
        for member_name in dir(cls):
            # If the class has an empty slot (`__provides__` from
            # zope.interface, in this case) we get a name for a member that
            # does not exist.
            member = getattr(cls, member_name, None)
            if getattr(member, "needs_riak", False):
                needs_riak_methods.append((member_name, member))
        for name, meth in needs_riak_methods:
            delattr(cls, name)
            setattr(cls, name + "__on_riak", deco.wrap_riak_setup(meth))
            setattr(cls, name + "__on_txriak", deco.wrap_txriak_setup(meth))
        return cls

    def wrap_riak_setup(deco, meth):
        """
        Return a wrapper around `meth` that sets up a RiakManager.
        """
        @wraps(meth)
        def wrapper(self):
            deco.setup_riak(self)
            return meth(self)
        return wrapper

    def wrap_txriak_setup(deco, meth):
        """
        Return a wrapper around `meth` that sets up a TxRiakManager.
        """
        @wraps(meth)
        def wrapper(self):
            d = deco.setup_txriak(self)
            return d.addCallback(lambda _: meth(self))
        return wrapper

    def setup_riak(deco, self):
        """
        Set up a RiakManager on the given test class.
        """
        try:
            from vumi.persist.riak_manager import RiakManager
        except ImportError, e:
            import_skip(e, 'riak')
        self.manager = RiakManager.from_config({'bucket_prefix': 'test.'})
        self.add_cleanup(deco.cleanup_manager, self)
        self.manager.purge_all()

    @inlineCallbacks
    def setup_txriak(deco, self):
        """
        Set up a TxRiakManager on the given test class.
        """
        try:
            from vumi.persist.txriak_manager import TxRiakManager
        except ImportError, e:
            import_skip(e, 'riak')
        self.manager = TxRiakManager.from_config({'bucket_prefix': 'test.'})
        self.add_cleanup(deco.cleanup_manager, self)
        yield self.manager.purge_all()

    @inlineCallbacks
    def cleanup_manager(deco, self):
        """
        Clean up the Riak manager on the given test class.
        """
        yield self.manager.purge_all()
        yield self.manager.close_manager()


model_field_tests = ModelFieldTestsDecorator()


@model_field_tests
class TestBaseField(VumiTestCase):
    def test_validate(self):
        f = Field()
        f.validate("foo")
        f.validate(object())
        self.assertRaises(ValidationError, f.validate, None)

    def test_validate_null(self):
        f = Field(null=True)
        f.validate("foo")
        f.validate(None)

    def test_to_riak(self):
        f = Field()
        obj = object()
        self.assertEqual(f.to_riak(obj), obj)

    def test_from_riak(self):
        f = Field()
        obj = object()
        self.assertEqual(f.from_riak(obj), obj)

    def test_get_descriptor(self):
        f = Field()
        descriptor = f.get_descriptor("foo")
        self.assertEqual(descriptor.key, "foo")
        self.assertEqual(descriptor.field, f)
        self.assertTrue("Field object" in repr(descriptor))

    class BaseFieldModel(Model):
        """
        Toy model for Field tests.
        """
        f = Field()

    @needs_riak
    @Manager.calls_manager
    def test_assorted_values(self):
        """
        Values are preserved when the field is stored and later loaded.
        """
        base_model = self.manager.proxy(self.BaseFieldModel)
        yield base_model("m_str", f="string").save()
        yield base_model("m_int", f=1).save()
        yield base_model("m_list", f=["string", 1]).save()
        yield base_model("m_dict", f={"key": "val"}).save()

        m_str = yield base_model.load("m_str")
        self.assertEqual(m_str.f, "string")
        m_int = yield base_model.load("m_int")
        self.assertEqual(m_int.f, 1)
        m_list = yield base_model.load("m_list")
        self.assertEqual(m_list.f, ["string", 1])
        m_dict = yield base_model.load("m_dict")
        self.assertEqual(m_dict.f, {"key": "val"})


@model_field_tests
class TestInteger(VumiTestCase):
    def test_validate_unbounded(self):
        i = Integer()
        i.validate(5)
        i.validate(-3)
        self.assertRaises(ValidationError, i.validate, 5.0)
        self.assertRaises(ValidationError, i.validate, "5")

    def test_validate_minimum(self):
        i = Integer(min=3)
        i.validate(3)
        i.validate(4)
        self.assertRaises(ValidationError, i.validate, 2)

    def test_validate_maximum(self):
        i = Integer(max=5)
        i.validate(5)
        i.validate(4)
        self.assertRaises(ValidationError, i.validate, 6)

    class IntegerModel(Model):
        """
        Toy model for Integer field tests.
        """
        i = Integer()

    @needs_riak
    @Manager.calls_manager
    def test_assorted_values(self):
        """
        Values are preserved when the field is stored and later loaded.
        """
        int_model = self.manager.proxy(self.IntegerModel)
        yield int_model("m_1", i=1).save()
        yield int_model("m_leet", i=1337).save()

        m_1 = yield int_model.load("m_1")
        self.assertEqual(m_1.i, 1)
        m_leet = yield int_model.load("m_leet")
        self.assertEqual(m_leet.i, 1337)


@model_field_tests
class TestBoolean(VumiTestCase):

    def test_validate(self):
        b = Boolean()
        b.validate(True)
        b.validate(False)
        self.assertRaises(ValidationError, b.validate, 'True')
        self.assertRaises(ValidationError, b.validate, 'False')
        self.assertRaises(ValidationError, b.validate, 1)
        self.assertRaises(ValidationError, b.validate, 0)

    class BooleanModel(Model):
        """
        Toy model for Boolean field tests.
        """
        b = Boolean()

    @needs_riak
    @Manager.calls_manager
    def test_assorted_values(self):
        """
        Values are preserved when the field is stored and later loaded.
        """
        bool_model = self.manager.proxy(self.BooleanModel)
        yield bool_model("m_t", b=True).save()
        yield bool_model("m_f", b=False).save()

        m_t = yield bool_model.load("m_t")
        self.assertEqual(m_t.b, True)
        m_f = yield bool_model.load("m_f")
        self.assertEqual(m_f.b, False)


@model_field_tests
class TestUnicode(VumiTestCase):
    def test_validate(self):
        u = Unicode()
        u.validate(u"")
        u.validate(u"a")
        u.validate(u"æ")
        u.validate(u"foé")
        self.assertRaises(ValidationError, u.validate, "")
        self.assertRaises(ValidationError, u.validate, "foo")
        self.assertRaises(ValidationError, u.validate, 3)

    def test_validate_max_length(self):
        u = Unicode(max_length=5)
        u.validate(u"12345")
        u.validate(u"1234")
        self.assertRaises(ValidationError, u.validate, u"123456")

    class UnicodeModel(Model):
        """
        Toy model for Unicode field tests.
        """
        u = Unicode()

    @needs_riak
    @Manager.calls_manager
    def test_assorted_values(self):
        """
        Values are preserved when the field is stored and later loaded.
        """
        unicode_model = self.manager.proxy(self.UnicodeModel)
        yield unicode_model("m_empty", u=u"").save()
        yield unicode_model("m_full", u=u"You must be an optimist").save()
        yield unicode_model("m_unicode", u=u"foé").save()

        m_empty = yield unicode_model.load("m_empty")
        self.assertEqual(m_empty.u, u"")
        m_full = yield unicode_model.load("m_full")
        self.assertEqual(m_full.u, u"You must be an optimist")
        m_unicode = yield unicode_model.load("m_unicode")
        self.assertEqual(m_unicode.u, u"foé")


@model_field_tests
class TestTag(VumiTestCase):
    def test_validate(self):
        t = Tag()
        t.validate(("pool", "tagname"))
        self.assertRaises(ValidationError, t.validate, ["pool", "tagname"])
        self.assertRaises(ValidationError, t.validate, ("pool",))

    def test_to_riak(self):
        t = Tag()
        self.assertEqual(t.to_riak(("pool", "tagname")), ["pool", "tagname"])

    def test_from_riak(self):
        t = Tag()
        self.assertEqual(t.from_riak(["pool", "tagname"]), ("pool", "tagname"))


@model_field_tests
class TestTimestamp(VumiTestCase):
    def test_validate(self):
        t = Timestamp()
        t.validate(datetime.now())
        t.validate("2007-01-25T12:00:00Z")
        t.validate(u"2007-01-25T12:00:00Z")
        self.assertRaises(ValidationError, t.validate, "foo")

    def test_to_riak(self):
        t = Timestamp()
        dt = datetime(2100, 10, 5, 11, 10, 9)
        self.assertEqual(t.to_riak(dt), "2100-10-05 11:10:09.000000")

    def test_from_riak(self):
        t = Timestamp()
        dt = datetime(2100, 10, 5, 11, 10, 9)
        self.assertEqual(t.from_riak("2100-10-05 11:10:09.000000"), dt)

    class TimestampModel(Model):
        """
        Toy model for Timestamp tests.
        """
        time = Timestamp(null=True)

    @needs_riak
    def test_set_field(self):
        """
        A timestamp field can be set to a datetime or string value through its
        descriptor.
        """
        timestamp_model = self.manager.proxy(self.TimestampModel)
        t = timestamp_model("foo")

        now = datetime.now()
        t.time = now
        self.assertEqual(t.time, now)

        t.time = u"2007-01-25T12:00:00Z"
        self.assertEqual(t.time, datetime(2007, 01, 25, 12, 0))

    @needs_riak
    @Manager.calls_manager
    def test_assorted_values(self):
        """
        Values are preserved when the field is stored and later loaded.
        """
        timestamp_model = self.manager.proxy(self.TimestampModel)
        now = datetime.now()
        yield timestamp_model("m_now", time=now).save()
        yield timestamp_model("m_string", time=u"2007-01-25T12:00:00Z").save()

        m_now = yield timestamp_model.load("m_now")
        self.assertEqual(m_now.time, now)
        m_string = yield timestamp_model.load("m_string")
        self.assertEqual(m_string.time, datetime(2007, 01, 25, 12, 0))


@model_field_tests
class TestJson(VumiTestCase):
    def test_validate(self):
        j = Json()
        j.validate({"foo": None})
        self.assertRaises(ValidationError, j.validate, None)

    def test_to_riak(self):
        j = Json()
        d = {"foo": 5}
        self.assertEqual(j.to_riak(d), d)

    def test_from_riak(self):
        j = Json()
        d = {"foo": [1, 2, 3]}
        self.assertEqual(j.from_riak(d), d)

    class JsonModel(Model):
        """
        Toy model for Json tests.
        """
        j = Json()

    @needs_riak
    @Manager.calls_manager
    def test_assorted_values(self):
        """
        Values are preserved when the field is stored and later loaded.
        """
        json_model = self.manager.proxy(self.JsonModel)
        yield json_model("m_str", j="string").save()
        yield json_model("m_int", j=1).save()
        yield json_model("m_list", j=["string", 1]).save()
        yield json_model("m_dict", j={"key": "val"}).save()

        m_str = yield json_model.load("m_str")
        self.assertEqual(m_str.j, "string")
        m_int = yield json_model.load("m_int")
        self.assertEqual(m_int.j, 1)
        m_list = yield json_model.load("m_list")
        self.assertEqual(m_list.j, ["string", 1])
        m_dict = yield json_model.load("m_dict")
        self.assertEqual(m_dict.j, {"key": "val"})


class TestFieldWithSubtype(VumiTestCase):
    def test_fails_on_fancy_subtype(self):
        self.assertRaises(RuntimeError, FieldWithSubtype, Dynamic())


@model_field_tests
class TestDynamic(VumiTestCase):
    def test_validate(self):
        dynamic = Dynamic()
        dynamic.validate({u'a': u'foo', u'b': u'bar'})
        self.assertRaises(ValidationError, dynamic.validate,
                          {u'a': 'foo', u'b': u'bar'})
        self.assertRaises(ValidationError, dynamic.validate,
                          u'this is not a dict')
        self.assertRaises(ValidationError, dynamic.validate,
                          {u'a': 'foo', u'b': 2})

    class DynamicModel(Model):
        """
        Toy model for Dynamic tests.
        """
        a = Unicode()
        contact_info = Dynamic()

    @needs_riak
    def test_get_data_with_dynamic_proxy(self):
        """
        A Dynamic field creates more than one field in the Riak object.
        """
        dynamic_model = self.manager.proxy(self.DynamicModel)

        m = dynamic_model("foo", a=u"ab")
        m.contact_info['foo'] = u'bar'
        m.contact_info['zip'] = u'zap'

        self.assertEqual(m.get_data(), {
            'key': 'foo',
            '$VERSION': None,
            'a': 'ab',
            'contact_info.foo': 'bar',
            'contact_info.zip': 'zap',
        })

    def _create_dynamic_instance(self, dynamic_model, key):
        m = dynamic_model(key, a=u"ab")
        m.contact_info['cellphone'] = u"+27123"
        m.contact_info['telephone'] = u"+2755"
        m.contact_info['honorific'] = u"BDFL"
        return m

    @needs_riak
    @Manager.calls_manager
    def test_dynamic_value(self):
        """
        Values are preserved when the field is stored and later loaded.
        """
        dynamic_model = self.manager.proxy(self.DynamicModel)
        yield self._create_dynamic_instance(dynamic_model, "foo").save()

        m = yield dynamic_model.load("foo")
        self.assertEqual(m.a, u"ab")
        self.assertEqual(m.contact_info['cellphone'], u"+27123")
        self.assertEqual(m.contact_info['telephone'], u"+2755")
        self.assertEqual(m.contact_info['honorific'], u"BDFL")

    @needs_riak
    def test_dynamic_field_init(self):
        """
        Dynamic fields can be initialised with dicts.
        """
        dynamic_model = self.manager.proxy(self.DynamicModel)
        contact_info = {'cellphone': u'+27123', 'telephone': u'+2755'}
        m = dynamic_model("foo", a=u"ab", contact_info=contact_info)
        self.assertEqual(m.contact_info.copy(), contact_info)

    @needs_riak
    def test_dynamic_field_keys_and_values(self):
        """
        Dynamic field keys and values are available as lists or and iterators,
        similar to a dict.
        """
        dynamic_model = self.manager.proxy(self.DynamicModel)
        m = self._create_dynamic_instance(dynamic_model, "foo")

        keys = m.contact_info.keys()
        iterkeys = m.contact_info.iterkeys()
        self.assertTrue(isinstance(keys, list))
        self.assertTrue(hasattr(iterkeys, 'next'))
        self.assertEqual(sorted(keys), ['cellphone', 'honorific', 'telephone'])
        self.assertEqual(sorted(iterkeys), sorted(keys))

        values = m.contact_info.values()
        itervalues = m.contact_info.itervalues()
        self.assertTrue(isinstance(values, list))
        self.assertTrue(hasattr(itervalues, 'next'))
        self.assertEqual(sorted(values), ["+27123", "+2755", "BDFL"])
        self.assertEqual(sorted(itervalues), sorted(values))

        items = m.contact_info.items()
        iteritems = m.contact_info.iteritems()
        self.assertTrue(isinstance(items, list))
        self.assertTrue(hasattr(iteritems, 'next'))
        self.assertEqual(sorted(items), [('cellphone', "+27123"),
                                         ('honorific', "BDFL"),
                                         ('telephone', "+2755")])
        self.assertEqual(sorted(iteritems), sorted(items))

    @needs_riak
    def test_dynamic_field_clear(self):
        """
        Dynamic fields can be cleared.
        """
        dynamic_model = self.manager.proxy(self.DynamicModel)
        m = self._create_dynamic_instance(dynamic_model, "foo")
        m.contact_info.clear()
        self.assertEqual(m.contact_info.items(), [])

    @needs_riak
    def test_dynamic_field_update(self):
        """
        Dynamic fields can be bulk-updated.
        """
        dynamic_model = self.manager.proxy(self.DynamicModel)
        m = self._create_dynamic_instance(dynamic_model, "foo")
        m.contact_info.update({"cellphone": "123", "name": "foo"})
        self.assertEqual(sorted(m.contact_info.items()), [
            ('cellphone', "123"), ('honorific', "BDFL"), ('name', "foo"),
            ('telephone', "+2755")])

    @needs_riak
    def test_dynamic_field_contains(self):
        """
        Dynamic fields support `in`.
        """
        dynamic_model = self.manager.proxy(self.DynamicModel)
        m = self._create_dynamic_instance(dynamic_model, "foo")
        self.assertTrue("cellphone" in m.contact_info)
        self.assertFalse("landline" in m.contact_info)

    @needs_riak
    def test_dynamic_field_del(self):
        """
        Values can be removed from dynamic fields.
        """
        dynamic_model = self.manager.proxy(self.DynamicModel)
        m = self._create_dynamic_instance(dynamic_model, "foo")
        del m.contact_info["telephone"]
        self.assertEqual(sorted(m.contact_info.keys()),
                         ['cellphone', 'honorific'])

    @needs_riak
    def test_dynamic_field_setting(self):
        """
        Setting a dynamic field to a dict replaces its contents.
        """
        dynamic_model = self.manager.proxy(self.DynamicModel)
        m = self._create_dynamic_instance(dynamic_model, "foo")
        m.contact_info = {u'cellphone': u'789', u'name': u'foo'}
        self.assertEqual(sorted(m.contact_info.items()), [
            (u'cellphone', u'789'),
            (u'name', u'foo'),
        ])


@model_field_tests
class TestListOf(VumiTestCase):
    def test_validate(self):
        """
        By default, a ListOf field is a list of Unicode fields.
        """
        listof = ListOf()
        listof.validate([u'foo', u'bar'])
        self.assertRaises(ValidationError, listof.validate,
                          u'this is not a list')
        self.assertRaises(ValidationError, listof.validate, ['a', 2])
        self.assertRaises(ValidationError, listof.validate, [1, 2])

    def test_validate_with_subtype(self):
        """
        If an explicit subtype is provided, its validation is used.
        """
        listof_unicode = ListOf(Unicode())
        listof_unicode.validate([u"a", u"b"])
        self.assertRaises(ValidationError, listof_unicode.validate, [1, 2])

        listof_int = ListOf(Integer())
        listof_int.validate([1, 2])
        self.assertRaises(ValidationError, listof_int.validate, [u"a", u"b"])

        listof_smallint = ListOf(Integer(max=10))
        listof_smallint.validate([1, 2])
        self.assertRaises(
            ValidationError, listof_smallint.validate, [1, 100])

    class ListOfModel(Model):
        """
        Toy model for ListOf tests.
        """
        items = ListOf(Integer())
        texts = ListOf(Unicode())

    class IndexedListOfModel(Model):
        """
        Toy model for ListOf index tests.
        """
        items = ListOf(Integer(), index=True)

    @needs_riak
    def test_get_data_with_list_proxy(self):
        """
        A ListOf field creates a list field in the Riak object.
        """
        list_model = self.manager.proxy(self.ListOfModel)
        m = list_model("foo")
        m.items.append(1)
        m.items.append(42)
        m.texts.append(u"Thing 1.")
        m.texts.append(u"Thing 42.")
        self.assertEqual(m.get_data(), {
            'key': 'foo',
            '$VERSION': None,
            'items': [1, 42],
            'texts': [u"Thing 1.", u"Thing 42."],
        })

    @needs_riak
    @Manager.calls_manager
    def test_listof_fields(self):
        """
        A ListOf field can be manipulated as if it were a list.
        """
        list_model = self.manager.proxy(self.ListOfModel)
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

    @needs_riak
    @Manager.calls_manager
    def test_listof_fields_indexes(self):
        """
        An indexed ListOf field has an index value for each item in the list.
        """
        list_model = self.manager.proxy(self.IndexedListOfModel)
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


@model_field_tests
class TestSetOf(VumiTestCase):
    def test_validate(self):
        """
        By default, a SetOf field is a set of Unicode fields.
        """
        f = SetOf()
        f.validate(set([u'foo', u'bar']))
        self.assertRaises(ValidationError, f.validate, u'this is not a set')
        self.assertRaises(ValidationError, f.validate, set(['a', 2]))
        self.assertRaises(ValidationError, f.validate, [u'a', u'b'])

    def test_validate_with_subtype(self):
        """
        If an explicit subtype is provided, its validation is used.
        """
        setof_unicode = SetOf(Unicode())
        setof_unicode.validate(set([u"a", u"b"]))
        self.assertRaises(ValidationError, setof_unicode.validate, set([1, 2]))

        setof_int = SetOf(Integer())
        setof_int.validate(set([1, 2]))
        self.assertRaises(
            ValidationError, setof_int.validate, set([u"a", u"b"]))

        setof_smallint = SetOf(Integer(max=10))
        setof_smallint.validate(set([1, 2]))
        self.assertRaises(
            ValidationError, setof_smallint.validate, set([1, 100]))

    def test_to_riak(self):
        """
        The JSON representation of a SetOf field is a sorted list.
        """
        f = SetOf()
        self.assertEqual(f.to_riak(set([1, 2, 3])), [1, 2, 3])

    def test_from_riak(self):
        """
        The JSON list is turned into a set when read.
        """
        f = SetOf()
        self.assertEqual(f.from_riak([1, 2, 3]), set([1, 2, 3]))

    class SetOfModel(Model):
        """
        Toy model for SetOf tests.
        """
        items = SetOf(Integer())
        texts = SetOf(Unicode())

    class IndexedSetOfModel(Model):
        """
        Toy model for SetOf index tests.
        """
        items = SetOf(Integer(), index=True)

    @needs_riak
    def test_setof_fields_validation(self):
        set_model = self.manager.proxy(self.SetOfModel)
        m1 = set_model("foo")

        self.assertRaises(ValidationError, m1.items.add, "foo")
        self.assertRaises(ValidationError, m1.items.remove, "foo")
        self.assertRaises(ValidationError, m1.items.discard, "foo")
        self.assertRaises(ValidationError, m1.items.update, set(["foo"]))

    @needs_riak
    @Manager.calls_manager
    def test_setof_fields(self):
        """
        A SetOf field can be manipulated as if it were a set.
        """
        set_model = self.manager.proxy(self.SetOfModel)
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

    @needs_riak
    @Manager.calls_manager
    def test_setof_fields_indexes(self):
        """
        An indexed SetOf field has an index value for each item in the set.
        """
        set_model = self.manager.proxy(self.IndexedSetOfModel)
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


@model_field_tests
class TestVumiMessage(VumiTestCase):
    def test_validate(self):
        f = VumiMessage(Message)
        msg = Message()
        f.validate(msg)
        self.assertRaises(
            ValidationError, f.validate, u'this is not a vumi message')
        self.assertRaises(
            ValidationError, f.validate, None)

    class VumiMessageModel(Model):
        """
        Toy model for VumiMessage tests.
        """
        msg = VumiMessage(TransportUserMessage)

    @needs_riak
    @Manager.calls_manager
    def test_vumimessage_field(self):
        msg_helper = self.add_helper(MessageHelper())
        msg_model = self.manager.proxy(self.VumiMessageModel)
        msg = msg_helper.make_inbound("foo", extra="bar")
        m1 = msg_model("foo", msg=msg)
        yield m1.save()

        m2 = yield msg_model.load("foo")
        self.assertEqual(m1.msg, m2.msg)
        self.assertEqual(m2.msg, msg)

        self.assertRaises(ValidationError, setattr, m1, "msg", "foo")

        # test extra keys are removed
        msg2 = msg_helper.make_inbound("foo")
        m1.msg = msg2
        self.assertTrue("extra" not in m1.msg)

    @needs_riak
    @Manager.calls_manager
    def test_vumimessage_field_excludes_cache(self):
        msg_helper = self.add_helper(MessageHelper())
        msg_model = self.manager.proxy(self.VumiMessageModel)
        cache_attr = TransportUserMessage._CACHE_ATTRIBUTE
        msg = msg_helper.make_inbound("foo", extra="bar")
        msg.cache["cache"] = "me"
        self.assertEqual(msg[cache_attr], {"cache": "me"})

        m1 = msg_model("foo", msg=msg)
        self.assertTrue(cache_attr not in m1.msg)
        yield m1.save()

        m2 = yield msg_model.load("foo")
        self.assertTrue(cache_attr not in m2.msg)
        self.assertEqual(m2.msg, m1.msg)


class ReferencedModel(Model):
    """
    Toy model for testing fields that reference other models.
    """
    a = Integer()
    b = Unicode()


@model_field_tests
class TestForeignKey(VumiTestCase):

    class ForeignKeyModel(Model):
        """
        Toy model for ForeignKey tests.
        """
        referenced = ForeignKey(ReferencedModel, null=True)

    @needs_riak
    def test_get_data_with_foreign_key_proxy(self):
        """
        A ForeignKey field stores the referenced key in the Riak object.
        """
        referenced_model = self.manager.proxy(ReferencedModel)
        s1 = referenced_model("foo", a=5, b=u'3')
        fk_model = self.manager.proxy(self.ForeignKeyModel)

        f1 = fk_model("bar1")
        f1.referenced.set(s1)

        self.assertEqual(f1.get_data(), {
            'key': 'bar1',
            '$VERSION': None,
            'referenced': 'foo'
        })

    @needs_riak
    @Manager.calls_manager
    def test_foreignkey_fields(self):
        """
        ForeignKey fields can operate on both keys and model instances.
        """
        fk_model = self.manager.proxy(self.ForeignKeyModel)
        referenced_model = self.manager.proxy(ReferencedModel)
        s1 = referenced_model("foo", a=5, b=u'3')
        f1 = fk_model("bar")
        f1.referenced.set(s1)
        yield s1.save()
        yield f1.save()
        self.assertEqual(f1._riak_object.get_data()['referenced'], s1.key)

        f2 = yield fk_model.load("bar")
        s2 = yield f2.referenced.get()

        self.assertEqual(f2.referenced.key, "foo")
        self.assertEqual(s2.a, 5)
        self.assertEqual(s2.b, u"3")

        f2.referenced.set(None)
        s3 = yield f2.referenced.get()
        self.assertEqual(s3, None)

        f2.referenced.key = "foo"
        s4 = yield f2.referenced.get()
        self.assertEqual(s4.key, "foo")

        f2.referenced.key = None
        s5 = yield f2.referenced.get()
        self.assertEqual(s5, None)

        self.assertRaises(ValidationError, f2.referenced.set, object())

    @needs_riak
    @Manager.calls_manager
    def test_old_foreignkey_fields(self):
        """
        Old versions of the ForeignKey field relied entirely on indexes and
        didn't store the referenced key in the model data.
        """
        fk_model = self.manager.proxy(self.ForeignKeyModel)
        referenced_model = self.manager.proxy(ReferencedModel)
        s1 = referenced_model("foo", a=5, b=u'3')
        f1 = fk_model("bar")
        # Create index directly and remove data field to simulate old-style
        # index-only implementation
        f1._riak_object.add_index('referenced_bin', s1.key)
        data = f1._riak_object.get_data()
        data.pop('referenced')
        f1._riak_object.set_data(data)
        yield s1.save()
        yield f1.save()

        f2 = yield fk_model.load("bar")
        s2 = yield f2.referenced.get()

        self.assertEqual(f2.referenced.key, "foo")
        self.assertEqual(s2.a, 5)
        self.assertEqual(s2.b, u"3")

        f2.referenced.set(None)
        s3 = yield f2.referenced.get()
        self.assertEqual(s3, None)

        f2.referenced.key = "foo"
        s4 = yield f2.referenced.get()
        self.assertEqual(s4.key, "foo")

        f2.referenced.key = None
        s5 = yield f2.referenced.get()
        self.assertEqual(s5, None)

        self.assertRaises(ValidationError, f2.referenced.set, object())

    @needs_riak
    @Manager.calls_manager
    def test_reverse_foreignkey_fields(self):
        """
        When we declare a ForeignKey field, we add both a paginated index
        lookup method and a legacy non-paginated index lookup method to the
        foreign model's backlinks attribute.
        """
        fk_model = self.manager.proxy(self.ForeignKeyModel)
        referenced_model = self.manager.proxy(ReferencedModel)
        s1 = referenced_model("foo", a=5, b=u'3')
        f1 = fk_model("bar1")
        f1.referenced.set(s1)
        f2 = fk_model("bar2")
        f2.referenced.set(s1)
        yield s1.save()
        yield f1.save()
        yield f2.save()

        s2 = yield referenced_model.load("foo")
        results = yield s2.backlinks.foreignkeymodels()
        self.assertEqual(sorted(results), ["bar1", "bar2"])

        results_p1 = yield s2.backlinks.foreignkeymodel_keys()
        self.assertEqual(sorted(results_p1), ["bar1", "bar2"])
        self.assertEqual(results_p1.has_next_page(), False)


@model_field_tests
class TestManyToMany(VumiTestCase):

    @Manager.calls_manager
    def load_all_bunches_flat(self, m2m_field):
        results = []
        for result_bunch in m2m_field.load_all_bunches():
            results.extend((yield result_bunch))
        returnValue(results)

    class ManyToManyModel(Model):
        references = ManyToMany(ReferencedModel)

    @needs_riak
    def test_get_data_with_many_to_many_proxy(self):
        """
        A ManyToMany field stores the referenced keys in the Riak object.
        """
        referenced_model = self.manager.proxy(ReferencedModel)
        s1 = referenced_model("foo", a=5, b=u'3')
        mm_model = self.manager.proxy(self.ManyToManyModel)

        m1 = mm_model("bar")
        m1.references.add(s1)
        m1.save()

        self.assertEqual(m1.get_data(), {
            'key': 'bar',
            '$VERSION': None,
            'references': ['foo'],
        })

    @needs_riak
    @Manager.calls_manager
    def test_manytomany_field(self):
        """
        ManyToMany fields can operate on both keys and model instances.
        """
        mm_model = self.manager.proxy(self.ManyToManyModel)
        referenced_model = self.manager.proxy(ReferencedModel)

        s1 = referenced_model("foo", a=5, b=u'3')
        m1 = mm_model("bar")
        m1.references.add(s1)
        yield s1.save()
        yield m1.save()
        self.assertEqual(m1._riak_object.get_data()['references'], [s1.key])

        m2 = yield mm_model.load("bar")
        [s2] = yield self.load_all_bunches_flat(m2.references)

        self.assertEqual(m2.references.keys(), ["foo"])
        self.assertEqual(s2.a, 5)
        self.assertEqual(s2.b, u"3")

        m2.references.remove(s2)
        references = yield self.load_all_bunches_flat(m2.references)
        self.assertEqual(references, [])

        m2.references.add_key("foo")
        [s4] = yield self.load_all_bunches_flat(m2.references)
        self.assertEqual(s4.key, "foo")

        m2.references.remove_key("foo")
        references = yield self.load_all_bunches_flat(m2.references)
        self.assertEqual(references, [])

        self.assertRaises(ValidationError, m2.references.add, object())
        self.assertRaises(ValidationError, m2.references.remove, object())

        t1 = referenced_model("bar1", a=3, b=u'4')
        t2 = referenced_model("bar2", a=4, b=u'4')
        m2.references.add(t1)
        m2.references.add(t2)
        yield t1.save()
        yield t2.save()
        references = yield self.load_all_bunches_flat(m2.references)
        references.sort(key=lambda s: s.key)
        self.assertEqual([s.key for s in references], ["bar1", "bar2"])
        self.assertEqual(references[0].a, 3)
        self.assertEqual(references[1].a, 4)

        m2.references.clear()
        m2.references.add_key("unknown")
        self.assertEqual([], (yield self.load_all_bunches_flat(m2.references)))

    @needs_riak
    @Manager.calls_manager
    def test_old_manytomany_field(self):
        """
        Old versions of the ManyToMany field relied entirely on indexes and
        didn't store the referenced keys in the model data.
        """
        mm_model = self.manager.proxy(self.ManyToManyModel)
        referenced_model = self.manager.proxy(ReferencedModel)

        s1 = referenced_model("foo", a=5, b=u'3')
        m1 = mm_model("bar")
        # Create index directly to simulate old-style index-only implementation
        m1._riak_object.add_index('references_bin', s1.key)
        # Manually remove the entry from the data dict to allow it to be
        # set from the index value in descriptor.clean()
        data = m1._riak_object.get_data()
        data.pop('references')
        m1._riak_object.set_data(data)

        yield s1.save()
        yield m1.save()

        m2 = yield mm_model.load("bar")
        [s2] = yield self.load_all_bunches_flat(m2.references)

        self.assertEqual(m2.references.keys(), ["foo"])
        self.assertEqual(s2.a, 5)
        self.assertEqual(s2.b, u"3")

        m2.references.remove(s2)
        references = yield self.load_all_bunches_flat(m2.references)
        self.assertEqual(references, [])

        m2.references.add_key("foo")
        [s4] = yield self.load_all_bunches_flat(m2.references)
        self.assertEqual(s4.key, "foo")

        m2.references.remove_key("foo")
        references = yield self.load_all_bunches_flat(m2.references)
        self.assertEqual(references, [])

        self.assertRaises(ValidationError, m2.references.add, object())
        self.assertRaises(ValidationError, m2.references.remove, object())

        t1 = referenced_model("bar1", a=3, b=u'4')
        t2 = referenced_model("bar2", a=4, b=u'4')
        m2.references.add(t1)
        m2.references.add(t2)
        yield t1.save()
        yield t2.save()
        references = yield self.load_all_bunches_flat(m2.references)
        references.sort(key=lambda s: s.key)
        self.assertEqual([s.key for s in references], ["bar1", "bar2"])
        self.assertEqual(references[0].a, 3)
        self.assertEqual(references[1].a, 4)

        m2.references.clear()
        m2.references.add_key("unknown")
        self.assertEqual([], (yield self.load_all_bunches_flat(m2.references)))

    @needs_riak
    @Manager.calls_manager
    def test_reverse_manytomany_fields(self):
        """
        When we declare a ManyToMany field, we add both a paginated index
        lookup method and a legacy non-paginated index lookup method to the
        foreign model's backlinks attribute.
        """
        mm_model = self.manager.proxy(self.ManyToManyModel)
        referenced_model = self.manager.proxy(ReferencedModel)
        s1 = referenced_model("foo1", a=5, b=u'3')
        s2 = referenced_model("foo2", a=4, b=u'4')
        m1 = mm_model("bar1")
        m1.references.add(s1)
        m1.references.add(s2)
        m2 = mm_model("bar2")
        m2.references.add(s1)
        yield s1.save()
        yield s2.save()
        yield m1.save()
        yield m2.save()

        s1 = yield referenced_model.load("foo1")
        results = yield s1.backlinks.manytomanymodels()
        self.assertEqual(sorted(results), ["bar1", "bar2"])

        results_p1 = yield s1.backlinks.manytomanymodel_keys()
        self.assertEqual(sorted(results_p1), ["bar1", "bar2"])
        self.assertEqual(results_p1.has_next_page(), False)

        s2 = yield referenced_model.load("foo2")
        results = yield s2.backlinks.manytomanymodels()
        self.assertEqual(sorted(results), ["bar1"])

        results_p1 = yield s2.backlinks.manytomanymodel_keys()
        self.assertEqual(sorted(results_p1), ["bar1"])
        self.assertEqual(results_p1.has_next_page(), False)
