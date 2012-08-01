from twisted.trial import unittest
from twisted.internet.task import Clock
from twisted.internet.defer import inlineCallbacks
from smpp.pdu_builder import DeliverSM, BindTransceiverResp
from smpp.pdu import unpack_pdu

from vumi.tests.utils import LogCatcher, PersistenceMixin
from vumi.transports.smpp.clientserver.client import (
    EsmeTransceiver, EsmeReceiver, EsmeTransmitter, EsmeCallbacks, ESME)
from vumi.transports.smpp.clientserver.config import ClientConfig


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


class EsmeTestCaseBase(unittest.TestCase, PersistenceMixin):
    timeout = 5
    ESME_CLASS = None

    def setUp(self):
        self._persist_setUp()

    def tearDown(self):
        return self._persist_tearDown()

    def get_esme(self, **callbacks):
        config = ClientConfig(host="127.0.0.1", port="0",
                              system_id="1234", password="password")
        esme_callbacks = EsmeCallbacks(**callbacks)

        def purge_manager(redis_manager):
            d = redis_manager._purge_all()  # just in case
            d.addCallback(lambda result: redis_manager)
            return d

        redis_d = self.get_redis_manager()
        redis_d.addCallback(purge_manager)
        return redis_d.addCallback(
            lambda r: self.ESME_CLASS(config, r, esme_callbacks))


class EsmeTransceiverTestCase(EsmeTestCaseBase):
    ESME_CLASS = FakeEsmeTransceiver

    def get_sm(self, msg, data_coding=3):
        sm = DeliverSM(1, short_message=msg, data_coding=data_coding)
        return unpack_pdu(sm.get_bin())

    @inlineCallbacks
    def test_deliver_sm_simple(self):
        """A simple message should be delivered."""
        def cb(**kw):
            self.assertEqual(u'hello', kw['short_message'])

        esme = yield self.get_esme(deliver_sm=cb)
        esme.handle_deliver_sm(self.get_sm('hello'))

    @inlineCallbacks
    def test_deliver_sm_ucs2(self):
        """A UCS-2 message should be delivered."""
        def cb(**kw):
            self.assertEqual(u'hello', kw['short_message'])

        esme = yield self.get_esme(deliver_sm=cb)
        esme.handle_deliver_sm(self.get_sm('\x00h\x00e\x00l\x00l\x00o', 8))

    @inlineCallbacks
    def test_bad_sm_ucs2(self):
        """An invalid UCS-2 message should be discarded."""
        def cb(**kw):
            self.assertEqual(bad_msg, kw['short_message'])
            self.flushLoggedErrors()

        esme = yield self.get_esme(deliver_sm=cb)
        bad_msg = '\n\x00h\x00e\x00l\x00l\x00o'
        esme.handle_deliver_sm(self.get_sm(bad_msg, 8))

    @inlineCallbacks
    def test_bind_timeout(self):
        esme = yield self.get_esme()
        yield esme.connectionMade()

        self.assertEqual(True, esme.transport.connected)
        self.assertNotEqual(None, esme._lose_conn)

        esme.clock.advance(esme.smpp_bind_timeout)

        self.assertEqual(False, esme.transport.connected)
        self.assertEqual(None, esme._lose_conn)

    @inlineCallbacks
    def test_bind_no_timeout(self):
        esme = yield self.get_esme()
        yield esme.connectionMade()

        self.assertEqual(True, esme.transport.connected)
        self.assertNotEqual(None, esme._lose_conn)

        esme.handle_bind_transceiver_resp(unpack_pdu(
            BindTransceiverResp(1).get_bin()))

        self.assertEqual(True, esme.transport.connected)
        self.assertEqual(None, esme._lose_conn)
        esme.lc_enquire.stop()
        yield esme.lc_enquire.deferred

    @inlineCallbacks
    def test_sequence_rollover(self):
        esme = yield self.get_esme()
        self.assertEqual(1, (yield esme.get_next_seq()))
        self.assertEqual(2, (yield esme.get_next_seq()))
        yield esme.redis.set('smpp_last_sequence_number', 0xFFFF0000)
        self.assertEqual(0xFFFF0001, (yield esme.get_next_seq()))
        self.assertEqual(1, (yield esme.get_next_seq()))


class EsmeTransmitterTestCase(EsmeTestCaseBase):
    ESME_CLASS = FakeEsmeTransmitter

    def get_sm(self, msg, data_coding=3):
        sm = DeliverSM(1, short_message=msg, data_coding=data_coding)
        return unpack_pdu(sm.get_bin())

    @inlineCallbacks
    def test_deliver_sm_simple(self):
        """A message delivery should log an error since we're supposed
        to be a transmitter only."""
        def cb(**kw):
            self.assertEqual(u'hello', kw['short_message'])

        with LogCatcher() as log:
            esme = yield self.get_esme(deliver_sm=cb)
            esme.state = 'BOUND_TX'  # Assume we've bound correctly as a TX
            esme.handle_deliver_sm(self.get_sm('hello'))
            [error] = log.errors
            self.assertTrue('deliver_sm in wrong state' in error['message'][0])


class EsmeReceiverTestCase(EsmeTestCaseBase):
    ESME_CLASS = FakeEsmeReceiver

    def get_sm(self, msg, data_coding=3):
        sm = DeliverSM(1, short_message=msg, data_coding=data_coding)
        return unpack_pdu(sm.get_bin())

    @inlineCallbacks
    def test_submit_sm_simple(self):
        """A simple message log an error when trying to send over
        a receiver."""
        with LogCatcher() as log:
            esme = yield self.get_esme()
            esme.state = 'BOUND_RX'  # Fake RX bind
            yield esme.submit_sm(short_message='hello')
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
        return self.esme.bindTransciever()
