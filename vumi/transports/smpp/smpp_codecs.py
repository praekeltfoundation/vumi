# -*- test-case-name: vumi.transports.smpp.tests.test_smpp_codecs -*-
import sys
import codecs

from vumi.transports.smpp.ismpp_codecs import ISmppCodec

from zope.interface import implements


class SmppCodecException(Exception):
    pass


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
