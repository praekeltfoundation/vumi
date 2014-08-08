# -*- coding: utf-8 -*-
from vumi.transports.smpp.icodecs import ISmppCodec
from vumi.transports.smpp.codecs import SmppCodec, SmppCodecException

from twisted.trial.unittest import TestCase


class TestSmppCodec(TestCase):

    def setUp(self):
        self.codec = SmppCodec()

    def test_implements(self):
        self.assertTrue(ISmppCodec.implementedBy(SmppCodec))
        self.assertTrue(ISmppCodec.providedBy(self.codec))

    def test_unicode_encode_guard(self):
        self.assertRaises(
            SmppCodecException, self.codec.encode, "byte string", "utf-8")

    def test_bytestring_decode_guard(self):
        self.assertRaises(
            SmppCodecException, self.codec.decode, u"Zoë", "utf-8")

    def test_utf8(self):
        self.assertEqual(self.codec.encode(u"Zoë", "utf-8"), 'Zo\xc3\xab')
