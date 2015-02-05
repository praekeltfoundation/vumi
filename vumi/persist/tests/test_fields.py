# -*- coding: utf-8 -*-

"""Tests for vumi.persist.fields."""

from datetime import datetime

from vumi.message import Message

from vumi.persist.fields import (
    ValidationError, Field, Integer, Unicode, Tag, Timestamp, Json,
    ListOf, SetOf, Dynamic, FieldWithSubtype, Boolean, VumiMessage)
from vumi.tests.helpers import VumiTestCase


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


class TestBoolean(VumiTestCase):

    def test_validate(self):
        b = Boolean()
        b.validate(True)
        b.validate(False)
        self.assertRaises(ValidationError, b.validate, 'True')
        self.assertRaises(ValidationError, b.validate, 'False')
        self.assertRaises(ValidationError, b.validate, 1)
        self.assertRaises(ValidationError, b.validate, 0)


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
