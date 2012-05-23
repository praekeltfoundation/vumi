import uuid

from twisted.trial import unittest
from twisted.internet.task import Clock
from smpp.pdu_builder import DeliverSM, BindTransceiverResp
from smpp.pdu import unpack_pdu

from vumi.tests.utils import FakeRedis, LogCatcher
from vumi.transports.smpp.clientserver.client import (
        EsmeTransceiver,
        EsmeReceiver,
        EsmeTransmitter,
        EsmeCallbacks,
        KeyValueBase,
        KeyValueStore,
        ESME)
from vumi.transports.smpp.clientserver.config import ClientConfig


class KeyValueStoreTestCase(unittest.TestCase):

    def setUp(self):
        self.kvs = KeyValueStore()
        KeyValueBase.register(self.kvs.__class__)
        self.prefix = "smpp_test_%s" % uuid.uuid4()

    def tearDown(self):
        pass

    def run_all_tests_on_instance(self, instance):
        self.kvs = instance
        KeyValueBase.register(instance.__class__)
        self.test_implements_abstract()
        self.test_set_get_delete()
        self.test_incr()

    def test_instance_test(self):
        newKeyValueStoreTestCase = KeyValueStoreTestCase()
        newKeyValueStoreTestCase.prefix = "smpp_test_%s" % uuid.uuid4()
        instance = KeyValueStore()
        newKeyValueStoreTestCase.run_all_tests_on_instance(instance)

    def test_implements_abstract(self):
        self.assertTrue(issubclass(KeyValueStore, KeyValueBase))
        self.assertTrue(isinstance(self.kvs, KeyValueBase))

    def test_set_get_delete(self):
        key1 = "%s#cookie" % self.prefix

        try:
            self.assertEqual(self.kvs.get(key1), None)
            self.kvs.set(key1, "monster")
            self.assertEqual(self.kvs.get(key1), "monster")
            self.kvs.set(key1, "crumbles")
            self.assertNotEqual(self.kvs.get(key1), "monster")
            self.assertEqual(self.kvs.get(key1), "crumbles")
            self.assertEqual(self.kvs.delete(key1), True)
            self.assertEqual(self.kvs.get(key1), None)

        except:
            self.kvs.delete(key1)
            raise

    def test_incr(self):
        key1 = "%s#counter" % self.prefix

        try:
            self.assertEqual(self.kvs.get(key1), None)
            self.assertEqual(self.kvs.incr(key1), 1)
            self.kvs.set(key1, 1)
            self.assertEqual(self.kvs.incr(key1), 2)
            self.kvs.set(key1, "1")
            self.assertEqual(self.kvs.incr(key1), 2)
            self.kvs.delete(key1)
            self.assertEqual(self.kvs.incr(key1), 1)
            self.assertEqual(self.kvs.incr(key1), 2)
            self.assertEqual(self.kvs.incr(key1), 3)
            self.assertEqual(self.kvs.delete(key1), True)

        except:
            self.kvs.delete(key1)
            raise


class FakeTransport(object):
    def __init__(self):
        self.connected = True

    def loseConnection(self):
        self.connected = False


class FakeEsmeTransceiver(EsmeTransceiver):

    def __init__(self, *args, **kwargs):
        EsmeTransceiver.__init__(self, *args, **kwargs)
        self.transport = FakeTransport()
        self.clock = Clock()
        self.callLater = self.clock.callLater

    def send_pdu(self, *args):
        pass


class FakeEsmeReceiver(EsmeReceiver):

    def __init__(self, *args, **kwargs):
        EsmeReceiver.__init__(self, *args, **kwargs)
        self.transport = FakeTransport()
        self.clock = Clock()
        self.callLater = self.clock.callLater

    def send_pdu(self, *args):
        pass


class FakeEsmeTransmitter(EsmeTransmitter):

    def __init__(self, *args, **kwargs):
        EsmeTransmitter.__init__(self, *args, **kwargs)
        self.transport = FakeTransport()
        self.clock = Clock()
        self.callLater = self.clock.callLater

    def send_pdu(self, *args):
        pass


class EsmeSequenceNumberTestCase(unittest.TestCase):

    def test_sequence_rollover(self):
        config = ClientConfig(host="127.0.0.1", port="0",
                              system_id="1234", password="password")
        esme = FakeEsmeTransceiver(config, FakeRedis(), None)
        self.assertEqual(1, esme.get_seq())
        esme.get_next_seq()
        self.assertEqual(2, esme.get_seq())
        esme.set_seq(4004004004)
        self.assertEqual(4004004004, esme.get_seq())
        esme.get_next_seq()
        self.assertEqual(1, esme.get_seq())


class EsmeTransceiverTestCase(unittest.TestCase):
    def get_esme(self, **callbacks):
        config = ClientConfig(host="127.0.0.1", port="0",
                              system_id="1234", password="password")
        esme_callbacks = EsmeCallbacks(**callbacks)
        esme = FakeEsmeTransceiver(config, FakeRedis(), esme_callbacks)
        return esme

    def get_sm(self, msg, data_coding=3):
        sm = DeliverSM(1, short_message=msg, data_coding=data_coding)
        return unpack_pdu(sm.get_bin())

    def test_deliver_sm_simple(self):
        """A simple message should be delivered."""
        def cb(**kw):
            self.assertEqual(u'hello', kw['short_message'])

        esme = self.get_esme(deliver_sm=cb)
        esme.handle_deliver_sm(self.get_sm('hello'))

    def test_deliver_sm_ucs2(self):
        """A UCS-2 message should be delivered."""
        def cb(**kw):
            self.assertEqual(u'hello', kw['short_message'])

        esme = self.get_esme(deliver_sm=cb)
        esme.handle_deliver_sm(self.get_sm('\x00h\x00e\x00l\x00l\x00o', 8))

    def test_bad_sm_ucs2(self):
        """An invalid UCS-2 message should be discarded."""
        def cb(**kw):
            self.assertEqual(bad_msg, kw['short_message'])
            self.flushLoggedErrors()

        esme = self.get_esme(deliver_sm=cb)
        bad_msg = '\n\x00h\x00e\x00l\x00l\x00o'
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
        esme.connectionMade()

        self.assertEqual(True, esme.transport.connected)
        self.assertNotEqual(None, esme._lose_conn)

        esme.handle_bind_transceiver_resp(unpack_pdu(
            BindTransceiverResp(1).get_bin()))

        self.assertEqual(True, esme.transport.connected)
        self.assertEqual(None, esme._lose_conn)
        esme.lc_enquire.stop()


class EsmeTransmitterTestCase(unittest.TestCase):
    def get_esme(self, **callbacks):
        config = ClientConfig(host="127.0.0.1", port="0",
                              system_id="1234", password="password")
        esme_callbacks = EsmeCallbacks(**callbacks)
        esme = FakeEsmeTransmitter(config, FakeRedis(), esme_callbacks)
        return esme

    def get_sm(self, msg, data_coding=3):
        sm = DeliverSM(1, short_message=msg, data_coding=data_coding)
        return unpack_pdu(sm.get_bin())

    def test_deliver_sm_simple(self):
        """A message delivery should log an error since we're supposed
        to be a transmitter only."""
        def cb(**kw):
            self.assertEqual(u'hello', kw['short_message'])

        with LogCatcher() as log:
            esme = self.get_esme(deliver_sm=cb)
            esme.state = 'BOUND_TX'  # Assume we've bound correctly as a TX
            esme.handle_deliver_sm(self.get_sm('hello'))
            [error] = log.errors
            self.assertTrue('deliver_sm in wrong state' in error['message'][0])


class EsmeReceiverTestCase(unittest.TestCase):
    def get_esme(self, **callbacks):
        config = ClientConfig(host="127.0.0.1", port="0",
                              system_id="1234", password="password")
        esme_callbacks = EsmeCallbacks(**callbacks)
        esme = FakeEsmeReceiver(config, FakeRedis(), esme_callbacks)
        return esme

    def get_sm(self, msg, data_coding=3):
        sm = DeliverSM(1, short_message=msg, data_coding=data_coding)
        return unpack_pdu(sm.get_bin())

    def test_submit_sm_simple(self):
        """A simple message log an error when trying to send over
        a receiver."""
        with LogCatcher() as log:
            esme = self.get_esme()
            esme.state = 'BOUND_RX'  # Fake RX bind
            esme.submit_sm(short_message='hello')
            [error] = log.errors
            self.assertTrue(('submit_sm in wrong state' in
                                            error['message'][0]))


class ESMETestCase(unittest.TestCase):

    def setUp(self):
        self.client_config = ClientConfig(
                host='localhost',
                port=2775,
                system_id='test_system',
                password='password',
                )
        self.kvs = None
        self.esme_callbacks = None
        self.esme = ESME(self.client_config, self.kvs,
                         self.esme_callbacks)

    def test_bind_as_transceiver(self):
        self.esme.bindTransciever()
