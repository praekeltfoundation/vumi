from twisted.trial import unittest
from smpp.pdu_builder import DeliverSM
from smpp.pdu import unpack_pdu

from vumi.transports.smpp.client import EsmeTransceiver


class FakeEsmeTransceiver(EsmeTransceiver):
    def __init__(self):
        self.defaults = {}
        pass

    def sendPDU(self, *args):
        pass


class EsmeTransceiverTestCase(unittest.TestCase):
    def get_esme(self):
        esme = FakeEsmeTransceiver()
        esme.setDeliverSMCallback(lambda *a, **k: None)
        return esme

    def get_sm(self, msg, data_coding=3):
        sm = DeliverSM(1, short_message=msg, data_coding=data_coding)
        return unpack_pdu(sm.get_bin())

    def test_deliver_sm_simple(self):
        """A test that should always pass"""
        esme = self.get_esme()

        def _cb(**kw):
            self.assertEqual(u'hello', kw['short_message'])
        esme.setDeliverSMCallback(_cb)
        esme.handle_deliver_sm(self.get_sm('hello'))

    def test_deliver_sm_ucs2(self):
        """A test that should always pass"""
        esme = self.get_esme()

        def _cb(**kw):
            self.assertEqual(u'hello', kw['short_message'])
        esme.setDeliverSMCallback(_cb)
        esme.handle_deliver_sm(self.get_sm('\x00h\x00e\x00l\x00l\x00o', 8))
