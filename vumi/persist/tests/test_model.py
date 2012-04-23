"""Tests for vumi.persist.model."""

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.persist.model import Model
from vumi.persist.fields import (
    ValidationError, Integer, Unicode, VumiMessage, Dynamic, ListOf,
    ForeignKey)
from vumi.persist.riak_manager import RiakManager
from vumi.persist.txriak_manager import TxRiakManager
from vumi.message import TransportUserMessage


class SimpleModel(Model):
    a = Integer()
    b = Unicode()


class VumiMessageModel(Model):
    msg = VumiMessage(TransportUserMessage)


class DynamicModel(Model):
    a = Unicode()
    contact_info = Dynamic()


class ListOfModel(Model):
    items = ListOf(Integer())


class ForeignKeyModel(Model):
    simple = ForeignKey(SimpleModel)


class InheritedModel(SimpleModel):
    c = Integer()


class TestModelOnTxRiak(TestCase):

    # TODO: all copies of mkmsg must be unified! 
    def mkmsg(self, **kw):
        kw.setdefault("transport_name", "sphex")
        kw.setdefault("transport_type", "sphex_type")
        kw.setdefault("to_addr", "1234")
        kw.setdefault("from_addr", "5678")
        return TransportUserMessage(**kw)

    @inlineCallbacks
    def setUp(self):
        self.manager = TxRiakManager.from_config({'bucket_prefix': 'test.'})
        yield self.manager.purge_all()

    @inlineCallbacks
    def tearDown(self):
        yield self.manager.purge_all()

    def test_simple_class(self):
        self.assertEqual(sorted(SimpleModel.fields.keys()), ['a', 'b'])
        self.assertTrue(isinstance(SimpleModel.a, Integer))
        self.assertTrue(isinstance(SimpleModel.b, Unicode))

    @inlineCallbacks
    def test_simple_instance(self):
        simple_model = self.manager.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        yield s1.save()

        s2 = yield simple_model.load("foo")
        self.assertEqual(s2.a, 5)
        self.assertEqual(s2.b, u'3')

    @inlineCallbacks
    def test_nonexist_keys_return_none(self):
        simple_model = self.manager.proxy(SimpleModel)
        s = yield simple_model.load("foo")
        self.assertEqual(s, None)

    @inlineCallbacks
    def test_vumimessage_field(self):
        msg_model = self.manager.proxy(VumiMessageModel)
        msg = self.mkmsg()
        m1 = msg_model("foo", msg=msg)
        yield m1.save()

        m2 = yield msg_model.load("foo")
        self.assertEqual(m1.msg, m2.msg)
        self.assertEqual(m2.msg, msg)

    @inlineCallbacks
    def test_dynamic_fields(self):
        dynamic_model = self.manager.proxy(DynamicModel)
        d1 = dynamic_model("foo", a=u"ab")
        d1.contact_info.cellphone = u"+27123"
        d1.contact_info.telephone = u"+2755"
        d1.contact_info.honorific = u"BDFL"
        yield d1.save()

        d2 = yield dynamic_model.load("foo")
        self.assertEqual(d2.a, u"ab")
        self.assertEqual(d2.contact_info.cellphone, u"+27123")
        self.assertEqual(d2.contact_info.telephone, u"+2755")
        self.assertEqual(d2.contact_info.honorific, u"BDFL")

    @inlineCallbacks
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

        del l2.items[0]
        self.assertEqual(list(l2.items), [2])

        l2.items.extend([3, 4, 5])
        self.assertEqual(list(l2.items), [2, 3, 4, 5])

    @inlineCallbacks
    def test_foreignkey_fields(self):
        fk_model = self.manager.proxy(ForeignKeyModel)
        simple_model = self.manager.proxy(SimpleModel)
        s1 = simple_model("foo", a=5, b=u'3')
        f1 = fk_model("bar")
        f1.simple.set(s1)
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

        self.assertRaises(ValidationError, f2.simple.set, object)

    @inlineCallbacks
    def test_reverse_foreingkey_fields(self):
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
        self.assertEqual(sorted(s.key for s in results), ["bar1", "bar2"])
        self.assertEqual([s.__class__ for s in results],
                         [ForeignKeyModel] * 2)

    @inlineCallbacks
    def test_inherited_model(self):
        self.assertEqual(sorted(InheritedModel.fields.keys()),
                         ["a", "b", "c"])

        inherited_model = self.manager.proxy(InheritedModel)

        im1 = inherited_model("foo", a=1, b=u"2", c=3)
        yield im1.save()

        im2 = yield inherited_model.load("foo")
        self.assertEqual(im2.a, 1)
        self.assertEqual(im2.b, u'2')
        self.assertEqual(im2.c, 3)


class TestModelOnRiak(TestModelOnTxRiak):
    @inlineCallbacks
    def setUp(self):
        self.manager = RiakManager.from_config({'bucket_prefix': 'test.'})
        yield self.manager.purge_all()
