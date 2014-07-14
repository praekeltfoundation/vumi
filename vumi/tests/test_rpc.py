# -*- coding: utf-8 -*-

"""Tests for vumi.rpc."""

from vumi.rpc import (
    RpcCheckError, Signature, signature, RpcType, Null, Unicode, Int, List,
    Dict, Tag)
from vumi.tests.helpers import VumiTestCase


class TestSignature(VumiTestCase):
    def test_check_params(self):
        s = Signature(lambda slf, x, y: x, x=Int(), y=Unicode())
        s.check_params([None, 1, u"a"], {})
        self.assertRaises(RpcCheckError, s.check_params,
                          [None, u"a", u"a"], {})
        self.assertRaises(RpcCheckError, s.check_params, [None, 1, 2], {})
        self.assertRaises(RpcCheckError, s.check_params,
                          [None, 1, u"a", 3], {})

    def test_check_params_with_defaults(self):
        s = Signature(lambda slf, x, y=u"default": x,
                      x=Int(), y=Unicode())
        s.check_params([None, 1, u"a"], {})
        s.check_params([None, 1], {})

    def test_check_result(self):
        s = Signature(lambda slf, x: x, x=Int("foo"),
                      returns=Int("bar"))
        self.assertEqual(s.check_result(5), 5)
        self.assertRaises(RpcCheckError, s.check_result, 'a')

    def test_param_doc(self):
        s = Signature(lambda slf, x: x, x=Int("foo"),
                      returns=Int("bar"))
        self.assertEqual(s.param_doc(), [
            ':param Int x:',
            '    foo',
            ':rtype Int:',
            '    bar'])

    def test_jsonrpc_signature(self):
        s = Signature(lambda slf, x: unicode(x), x=Int("foo"),
                      returns=Unicode("bar"))
        self.assertEqual(s.jsonrpc_signature(),
                         [['string', 'int']])


class DummyApi(object):
    def __init__(self, result=None):
        self.result = result

    @signature(a=Unicode(), b=Int())
    def test(self, a, b):
        """Basic doc."""
        return self.result


@signature(a=Unicode(), b=Int(), returns=Unicode(), requires_self=False)
def dummy_function(a, b):
    return unicode(a * b)


class TestSignatureDecorate(VumiTestCase):
    def test_decorate_unbound_method(self):
        api = DummyApi()
        self.assertEqual(api.test.signature, [['null', 'string', 'int']])
        self.assertTrue(isinstance(api.test.signature_object, Signature))
        self.assertEqual(api.test.__name__, 'test')
        self.assertEqual(api.test.__module__, 'vumi.tests.test_rpc')
        self.assertEqual(
            api.test.__doc__,
            'Basic doc.\n\n:param Unicode a:\n:param Int b:\n:rtype Null:')

        self.assertEqual(api.test(u'a', 1), None)
        self.assertRaises(RpcCheckError, api.test, 'a', 1)
        self.assertRaises(RpcCheckError, api.test, u'a', 1, 2)

        api.result = 1
        self.assertRaises(RpcCheckError, api.test, u'a', 1)

    def test_decorate_function(self):
        self.assertEqual(dummy_function(u"a", 2), u"aa")
        self.assertRaises(RpcCheckError, dummy_function, "a", 1)
        self.assertRaises(RpcCheckError, dummy_function, u"a", None)
        self.assertRaises(RpcCheckError, dummy_function, u"a", 1, 2)


class TestRpcType(VumiTestCase):
    def test_jsonrpc_type(self):
        self.assertEqual(RpcType.jsonrpc_type, None)

    def test_name(self):
        self.assertEqual(RpcType().name, "RpcType")

    def test_help(self):
        self.assertEqual(RpcType().help(), "")
        self.assertEqual(RpcType(help="foo").help(), "foo")

    def test_nullable(self):
        self.assertEqual(RpcType().nullable(), False)
        self.assertEqual(RpcType(null=False).nullable(), False)
        self.assertEqual(RpcType(null=True).nullable(), True)

    def test_check(self):
        r = RpcType()
        self.assertRaises(RpcCheckError, r.check, "name", None)
        self.assertRaises(RpcCheckError, r.check, "name", None)
        r = RpcType(null=True)
        r.check("name", None)

    def test_nonnull_check(self):
        self.assertRaises(RpcCheckError, RpcType().check, "name", "foo")


class TestNull(VumiTestCase):
    def test_jsonrpc_type(self):
        self.assertEqual(Null.jsonrpc_type, 'null')

    def test_check(self):
        n = Null()
        n.check("name", None)
        self.assertRaises(RpcCheckError, n.check, "name", 1)


class TestUnicode(VumiTestCase):
    def test_jsonrpc_type(self):
        self.assertEqual(Unicode.jsonrpc_type, 'string')

    def test_check(self):
        u = Unicode()
        u.check("name", u"foo")
        self.assertRaises(RpcCheckError, u.check, "name", "foo")
        self.assertRaises(RpcCheckError, u.check, "name", 1)


class TestInt(VumiTestCase):
    def test_jsonrpc_type(self):
        self.assertEqual(Int.jsonrpc_type, 'int')

    def test_check(self):
        i = Int()
        i.check("name", 1)
        self.assertRaises(RpcCheckError, i.check, "name", "foo")


class TestList(VumiTestCase):
    def test_jsonrpc_type(self):
        self.assertEqual(List.jsonrpc_type, 'array')

    def test_check(self):
        l = List()
        l.check("name", [])
        l.check("name", ["a", 1, 0.1])
        self.assertRaises(RpcCheckError, l.check, "name", "foo")

    def test_item_type(self):
        l = List(item_type=Int())
        l.check("name", [])
        l.check("name", [1, 2, 3])
        self.assertRaises(RpcCheckError, l.check, "name", [1, 0.1])

    def test_length(self):
        l = List(length=2)
        l.check("name", [1, 2])
        self.assertRaises(RpcCheckError, l.check, "name", [])
        self.assertRaises(RpcCheckError, l.check, "name", [1, 2, 3])


class TestDict(VumiTestCase):
    def test_jsonrpc_type(self):
        self.assertEqual(Dict.jsonrpc_type, 'struct')

    def test_check(self):
        d = Dict()
        d.check("name", {})
        d.check("name", {"a": 1, "b": "c"})
        self.assertRaises(RpcCheckError, d.check, "name", "foo")

    def test_item_type(self):
        d = Dict(item_type=Int())
        d.check("name", {})
        d.check("name", {"a": 1, "b": 2})
        self.assertRaises(RpcCheckError, d.check, "name", {"a": 1, "b": "c"})

    def test_required_fields(self):
        d = Dict(required_fields={'foo': Int(null=True), 'bar': Unicode()})
        d.check("name", {'foo': None, 'bar': u'b'})
        d.check("name", {'foo': 1, 'bar': u'b'})
        d.check("name", {'foo': 1, 'bar': u'b', 'extra': 2})
        self.assertRaises(RpcCheckError, d.check, "name", {"foo": 1})
        self.assertRaises(RpcCheckError, d.check, "name", {"bar": u"b"})
        self.assertRaises(RpcCheckError, d.check, "name",
                          {'foo': u'b', 'bar': u'b'})

    def test_optional_fields(self):
        d = Dict(optional_fields={'foo': Int(null=True), 'bar': Unicode()})
        d.check("name", {'foo': None, 'bar': u'b'})
        d.check("name", {'foo': 1, 'bar': u'b'})
        d.check("name", {'foo': 1, 'bar': u'b', 'extra': 2})
        d.check("name", {"foo": 1})
        d.check("name", {"bar": u"b"})
        self.assertRaises(RpcCheckError, d.check, "name", {"foo": u'b'})

    def test_fallback_to_item_type(self):
        d = Dict(required_fields={'foo': Int()},
                 optional_fields={'bar': Int()},
                 item_type=Unicode())
        d.check("name", {'foo': 1})
        d.check("name", {'foo': 1, 'bar': 2})
        d.check("name", {'foo': 1, 'bar': 2, 'extra': u'foo'})
        self.assertRaises(RpcCheckError, d.check, "name",
                          {'foo': 1, 'bar': 2, 'extra': 3})

    def test_closed(self):
        d = Dict(required_fields={'foo': Int()},
                 optional_fields={'bar': Int()},
                 closed=True)
        d.check("name", {'foo': 1})
        d.check("name", {'foo': 1, 'bar': 2})
        self.assertRaises(RpcCheckError, d.check, "name",
                          {'foo': 1, 'bar': 2, 'extra': 3})


class TestTag(VumiTestCase):
    def test_jsonrpc_type(self):
        self.assertEqual(Tag.jsonrpc_type, 'array')

    def test_check(self):
        t = Tag()
        t.check("name", (u"pool", u"tag"))
        t.check("name", [u"pool", u"tag"])
        self.assertRaises(RpcCheckError, t.check, "name", 1)
        self.assertRaises(RpcCheckError, t.check, "name", u"ab")
        self.assertRaises(RpcCheckError, t.check, "name", [u"a"])
        self.assertRaises(RpcCheckError, t.check, "name", [u"a", u"b", u"c"])
        self.assertRaises(RpcCheckError, t.check, "name", [u"pool", 1])
        self.assertRaises(RpcCheckError, t.check, "name", [2, u"tag"])
