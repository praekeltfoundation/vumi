# -*- coding: utf-8 -*-

"""Tests for vumi.persist.fields."""

from datetime import datetime

from twisted.trial.unittest import TestCase

from vumi.persist.fields import (
    ValidationError, Field, Integer, Unicode, Tag, Timestamp,
    Dynamic, FieldWithSubtype)


class TestBaseField(TestCase):
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


class TestInteger(TestCase):
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


class TestUnicode(TestCase):
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


class TestTag(TestCase):
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


class TestTimestamp(TestCase):
    def test_validate(self):
        t = Timestamp()
        t.validate(datetime.now())
        self.assertRaises(ValidationError, t.validate, "foo")

    def test_to_riak(self):
        t = Timestamp()
        dt = datetime(2100, 10, 5, 11, 10, 9)
        self.assertEqual(t.to_riak(dt), "2100-10-05 11:10:09.000000")

    def test_from_riak(self):
        t = Timestamp()
        dt = datetime(2100, 10, 5, 11, 10, 9)
        self.assertEqual(t.from_riak("2100-10-05 11:10:09.000000"), dt)


class TestFieldWithSubtype(TestCase):
    def test_fails_on_fancy_subtype(self):
        self.assertRaises(RuntimeError, FieldWithSubtype, Dynamic())
