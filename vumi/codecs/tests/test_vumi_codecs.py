# -*- coding: utf-8 -*-
from vumi.codecs.ivumi_codecs import IVumiCodec
from vumi.codecs.vumi_codecs import VumiCodec, VumiCodecException

from twisted.trial.unittest import TestCase


class TestVumiCodec(TestCase):

    def setUp(self):
        self.codec = VumiCodec()

    def test_implements(self):
        self.assertTrue(IVumiCodec.implementedBy(VumiCodec))
        self.assertTrue(IVumiCodec.providedBy(self.codec))

    def test_unicode_encode_guard(self):
        self.assertRaises(
            VumiCodecException, self.codec.encode, "byte string")

    def test_bytestring_decode_guard(self):
        self.assertRaises(
            VumiCodecException, self.codec.decode, u"unicode")

    def test_default_encoding(self):
        self.assertEqual(self.codec.encode(u"a"), "a")
        self.assertRaises(
            UnicodeEncodeError, self.codec.encode, u"ë")

    def test_default_decoding(self):
        self.assertEqual(self.codec.decode("a"), u"a")
        self.assertRaises(
            UnicodeDecodeError, self.codec.decode, '\xc3\xab')  # e-umlaut

    def test_encode_utf8(self):
        self.assertEqual(self.codec.encode(u"Zoë", "utf-8"), 'Zo\xc3\xab')

    def test_decode_utf8(self):
        self.assertEqual(self.codec.decode('Zo\xc3\xab', "utf-8"), u"Zoë")

    def test_encode_utf16be(self):
        self.assertEqual(
            self.codec.encode(u"Zoë", "utf-16be"), '\x00Z\x00o\x00\xeb')

    def test_decode_utf16be(self):
        self.assertEqual(
            self.codec.decode('\x00Z\x00o\x00\xeb', "utf-16be"), u"Zoë")

    def test_encode_ucs2(self):
        self.assertEqual(
            self.codec.encode(u"Zoë", "ucs2"), '\x00Z\x00o\x00\xeb')

    def test_decode_ucs2(self):
        self.assertEqual(
            self.codec.decode('\x00Z\x00o\x00\xeb', "ucs2"), u"Zoë")

    def test_encode_gsm0338(self):
        self.assertEqual(
            self.codec.encode(u"HÜLK", "gsm0338"),
            ''.join([chr(code) for code in [72, 94, 76, 75]]))

    def test_encode_gsm0338_extended(self):
        self.assertEqual(
            self.codec.encode(u"foo €", "gsm0338"),
            ''.join([chr(code) for code in [102, 111, 111, 32, 27, 101]]))

    def test_decode_gsm0338_extended(self):
        self.assertEqual(
            self.codec.decode(
                ''.join([chr(code) for code in [102, 111, 111, 32, 27, 101]]),
                'gsm0338'),
            u"foo €")

    def test_encode_gsm0338_strict(self):
        self.assertRaises(
            UnicodeEncodeError, self.codec.encode, u'Zoë', 'gsm0338')

    def test_encode_gsm0338_ignore(self):
        self.assertEqual(
            self.codec.encode(u"Zoë", "gsm0338", 'ignore'), 'Zo')

    def test_encode_gsm0338_replace(self):
        self.assertEqual(
            self.codec.encode(u"Zoë", "gsm0338", 'replace'), 'Zo?')

    def test_decode_gsm0338_strict(self):
        self.assertRaises(
            UnicodeDecodeError, self.codec.decode,
            u'Zoë'.encode('utf-8'), 'gsm0338')

    def test_decode_gsm0338_ignore(self):
        self.assertEqual(
            self.codec.decode(
                u'Zoë'.encode('utf-8'), "gsm0338", 'ignore'), u'Zo')

    def test_decode_gsm0338_replace(self):
        self.assertEqual(
            self.codec.decode(
                u'Zoë'.encode('utf-8'), "gsm0338", 'replace'), u'Zo??')
