import uuid

from twisted.trial import unittest
from twisted.internet.task import Clock
from smpp.pdu_builder import DeliverSM, BindTransceiverResp
from smpp.pdu import unpack_pdu

from vumi.transports.smpp.clientserver.client import (
        EsmeTransceiver,
        KeyValueBase,
        KeyValueStore,
        ESME)


class KeyValueStoreTestCase(unittest.TestCase):

    def setUp(self):
        self.kvs = KeyValueStore()
        self.prefix = "smpp_test_%s" % uuid.uuid4()

    def tearDown(self):
        pass

    def run_all_tests_on_instance(self, instance):
        self.test_implements_abstract(instance)
        self.test_set_get_delete(instance)
        self.test_incr(instance)

    def test_instance_test(self):
        newKeyValueStoreTestCase = KeyValueStoreTestCase()
        newKeyValueStoreTestCase.prefix = "smpp_test_%s" % uuid.uuid4()
        instance = KeyValueStore()
        newKeyValueStoreTestCase.run_all_tests_on_instance(instance)

    def test_implements_abstract(self, third_party_impl=None):
        if third_party_impl:
            self.assertTrue(issubclass(third_party_impl.__class__,
                KeyValueBase))
            self.kvs = third_party_impl
        else:
            self.assertTrue(issubclass(KeyValueStore, KeyValueBase))
        self.assertTrue(isinstance(self.kvs, KeyValueBase))

    def test_set_get_delete(self, third_party_impl=None):
        if third_party_impl:
            self.kvs = third_party_impl
        self.kvs.set(self.prefix + "cookie", "monster")
        self.assertEqual(self.kvs.get(self.prefix + "cookie"), "monster")
        self.assertEqual(self.kvs.get(self.prefix + "kookie"), None)
        self.kvs.set(self.prefix + "cookie", "crumbles")
        self.assertNotEqual(self.kvs.get(self.prefix + "cookie"), "monster")
        self.assertEqual(self.kvs.get(self.prefix + "cookie"), "crumbles")
        self.assertEqual(self.kvs.delete(self.prefix + "cookie"), "crumbles")
        self.assertEqual(self.kvs.get(self.prefix + "cookie"), None)

    def test_incr(self, third_party_impl=None):
        if third_party_impl:
            self.kvs = third_party_impl
        self.assertEqual(self.kvs.incr(self.prefix + "counter"), None)
        self.kvs.set(self.prefix + "counter", 1)
        self.assertEqual(self.kvs.incr(self.prefix + "counter"), "2")
        self.kvs.set(self.prefix + "counter", "1")
        self.assertEqual(self.kvs.incr(self.prefix + "counter"), "2")
        self.kvs.set(self.prefix + "counter", "oops")
        self.assertEqual(self.kvs.incr(self.prefix + "counter"), None)
        self.assertEqual(self.kvs.delete(self.prefix + "counter"), "oops")


class FakeTransport(object):
    def __init__(self):
        self.connected = True

    def loseConnection(self):
        self.connected = False


class FakeEsmeTransceiver(EsmeTransceiver):
    def __init__(self):
        self.defaults = {}
        self.name = 'test_esme'
        self.inc = 1
        self.seq = [0]
        self.smpp_bind_timeout = 10
        self.clock = Clock()
        self.callLater = self.clock.callLater
        self.transport = FakeTransport()

    def sendPDU(self, *args):
        pass


class EsmeSequenceNumberTestCase(unittest.TestCase):

    def test_sequence_rollover(self):
        esme = FakeEsmeTransceiver()
        self.assertEqual(0, esme.getSeq())
        esme.incSeq()
        self.assertEqual(1, esme.getSeq())
        esme.seq = [4004004004]
        self.assertEqual(4004004004, esme.getSeq())
        esme.incSeq()
        self.assertEqual(1, esme.getSeq())

    def test_sequence_rollover_10_4(self):
        esme = FakeEsmeTransceiver()
        esme.inc = 10
        esme.seq = [4]
        self.assertEqual(4, esme.getSeq())
        esme.incSeq()
        self.assertEqual(14, esme.getSeq())
        esme.seq = [4004004004]
        self.assertEqual(4004004004, esme.getSeq())
        esme.incSeq()
        self.assertEqual(14, esme.getSeq())

    def test_sequence_rollover_5_3(self):
        esme = FakeEsmeTransceiver()
        esme.inc = 5
        esme.seq = [3]
        self.assertEqual(3, esme.getSeq())
        esme.incSeq()
        self.assertEqual(8, esme.getSeq())
        esme.seq = [4004004003]
        self.assertEqual(4004004003, esme.getSeq())
        esme.incSeq()
        self.assertEqual(8, esme.getSeq())


class EsmeTransceiverTestCase(unittest.TestCase):
    def get_esme(self):
        esme = FakeEsmeTransceiver()
        esme.setDeliverSMCallback(lambda *a, **k: None)
        return esme

    def get_sm(self, msg, data_coding=3):
        sm = DeliverSM(1, short_message=msg, data_coding=data_coding)
        return unpack_pdu(sm.get_bin())

    def test_deliver_sm_simple(self):
        """A simple message should be delivered."""
        esme = self.get_esme()

        def _cb(**kw):
            self.assertEqual(u'hello', kw['short_message'])
        esme.setDeliverSMCallback(_cb)
        esme.handle_deliver_sm(self.get_sm('hello'))

    def test_deliver_sm_ucs2(self):
        """A UCS-2 message should be delivered."""
        esme = self.get_esme()

        def _cb(**kw):
            self.assertEqual(u'hello', kw['short_message'])
        esme.setDeliverSMCallback(_cb)
        esme.handle_deliver_sm(self.get_sm('\x00h\x00e\x00l\x00l\x00o', 8))

    def test_bad_sm_ucs2(self):
        """An invalid UCS-2 message should be discarded."""
        esme = self.get_esme()
        bad_msg = '\n\x00h\x00e\x00l\x00l\x00o'

        def _cb(**kw):
            self.assertEqual(bad_msg, kw['short_message'])
            self.flushLoggedErrors()
        esme.setDeliverSMCallback(_cb)
        esme.handle_deliver_sm(self.get_sm(bad_msg, 8))

    def test_bind_timeout(self):
        esme = self.get_esme()
        esme.connectionMade()

        self.assertEqual(True, esme.transport.connected)
        self.assertNotEqual(None, esme._lose_conn)

        esme.clock.advance(esme.smpp_bind_timeout)

        self.assertEqual(False, esme.transport.connected)
        self.assertEqual(None, esme._lose_conn)

    def test_bind_no_timeout(self):
        esme = self.get_esme()
        esme.setConnectCallback(lambda *a, **kw: None)
        esme.connectionMade()

        self.assertEqual(True, esme.transport.connected)
        self.assertNotEqual(None, esme._lose_conn)

        esme.handle_bind_transceiver_resp(unpack_pdu(
            BindTransceiverResp(1).get_bin()))

        self.assertEqual(True, esme.transport.connected)
        self.assertEqual(None, esme._lose_conn)
        esme.lc_enquire.stop()
