# -*- coding: utf-8 -*-

"""Tests for vumi.persist.fields."""

from datetime import datetime
from functools import wraps
import inspect

from twisted.internet.defer import inlineCallbacks

from vumi.message import Message
from vumi.persist.fields import (
    ValidationError, Field, Integer, Unicode, Tag, Timestamp, Json,
    ListOf, SetOf, Dynamic, FieldWithSubtype, Boolean, VumiMessage)
from vumi.persist.model import Manager, Model
from vumi.tests.helpers import VumiTestCase, import_skip


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
        needs_riak_methods = inspect.getmembers(
            cls, predicate=lambda m: getattr(m, "needs_riak", False))
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


class TestFieldWithSubtype(VumiTestCase):
    def test_fails_on_fancy_subtype(self):
        self.assertRaises(RuntimeError, FieldWithSubtype, Dynamic())


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


class TestListOf(VumiTestCase):
    def test_validate(self):
        listof = ListOf()
        listof.validate([u'foo', u'bar'])
        self.assertRaises(ValidationError, listof.validate,
                          u'this is not a list')
        self.assertRaises(ValidationError, listof.validate, ['a', 2])


class TestSetOf(VumiTestCase):
    def test_validate(self):
        f = SetOf()
        f.validate(set([u'foo', u'bar']))
        self.assertRaises(ValidationError, f.validate, u'this is not a set')
        self.assertRaises(ValidationError, f.validate, set(['a', 2]))

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


class TestVumiMessage(VumiTestCase):
    def test_validate(self):
        f = VumiMessage(Message)
        msg = Message()
        f.validate(msg)
        self.assertRaises(
            ValidationError, f.validate, u'this is not a vumi message')
        self.assertRaises(
            ValidationError, f.validate, None)
