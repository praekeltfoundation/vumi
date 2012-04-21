# -*- coding: utf-8 -*-

"""Tests for vumi.persist.fields."""

from twisted.trial.unittest import TestCase

from vumi.persist.fields import (
    ValidationError, Field, Integer, Unicode, Tag, ForeignKey, Dynamic)


class TestBaseField(TestCase):
    def test_validate(self):
        f = Field()
        f.validate("foo")
        f.validate(object())

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
