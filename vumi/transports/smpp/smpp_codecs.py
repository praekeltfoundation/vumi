# -*- test-case-name: vumi.transports.smpp.tests.test_smpp_codecs -*-
# -*- coding: utf-8 -*-
import codecs
import sys

from vumi.transports.smpp.ismpp_codecs import ISmppCodec

from zope.interface import implements


class SmppCodecException(Exception):
    pass


class GSM7BitCodec(codecs.Codec):
    """
    This has largely been copied from:
    http://stackoverflow.com/questions/13130935/decode-7-bit-gsm
    """

    gsm_basic_charset = (
        u"@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ\x1bÆæßÉ !\"#¤%&'()*+,-./0123456789:;"
        u"<=>?¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ`¿abcdefghijklmnopqrstuvwxyzäö"
        u"ñüà")
    gsm_extension = (
        u"````````````````````^```````````````````{}`````\\````````````[~]`"
        u"|````````````````````````````````````€``````````````````````````")

    def encode(self, unicode_string, errors='strict'):
        result = []
        for c in unicode_string:
            idx = self.gsm_basic_charset.find(c)
            if idx != -1:
                result.append(chr(idx))
                continue
            idx = self.gsm_extension.find(c)
            if idx != -1:
                result.append(chr(27) + chr(idx))
        obj = ''.join(result).encode('hex', errors)
        return (obj, len(obj))

    def decode(self, hex_byte_string, errors='strict'):
        res = hex_byte_string.decode('hex')
        res = iter(res)
        result = []
        for c in res:
            if c == chr(27):
                c = next(res)
                result.append(self.gsm_extension[ord(c)])
            else:
                result.append(self.gsm_basic_charset[ord(c)])
        obj = u''.join(result)
        return (obj, len(obj))


class UCS2Codec(codecs.Codec):
    """
    UCS2 is for all intents & purposes assumed to be the same as
    big endian UTF16.
    """
    def encode(self, input, errors='strict'):
        return codecs.utf_16_be_encode(input, errors)

    def decode(self, input, errors='strict'):
        return codecs.utf_16_be_decode(input, errors)


class SmppCodec(object):
    implements(ISmppCodec)

    custom_codecs = {
        'gsm0338': GSM7BitCodec(),
        'ucs2': UCS2Codec()
    }

    def encode(self, unicode_string, encoding=None, errors='strict'):
        if not isinstance(unicode_string, unicode):
            raise SmppCodecException(
                'Only Unicode strings accepted for encoding.')
        encoding = encoding or sys.getdefaultencoding()
        if encoding in self.custom_codecs:
            encoder = self.custom_codecs[encoding].encode
        else:
            encoder = codecs.getencoder(encoding)
        obj, length = encoder(unicode_string, errors)
        return obj

    def decode(self, byte_string, encoding=None, errors='strict'):
        if not isinstance(byte_string, str):
            raise SmppCodecException(
                'Only bytestrings accepted for decoding.')
        encoding = encoding or sys.getdefaultencoding()
        if encoding in self.custom_codecs:
            decoder = self.custom_codecs[encoding].decode
        else:
            decoder = codecs.getdecoder(encoding)
        obj, length = decoder(byte_string, errors)
        return obj
