# -*- test-case-name: vumi.transports.smpp.tests.test_codecs -*-
import sys
import codecs

from vumi.transports.smpp.icodecs import ISmppCodec

from zope.interface import implements


class SmppCodecException(Exception):
    pass


class SmppCodec(object):
    implements(ISmppCodec)

    custom_codecs = {
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
        return encoder(unicode_string, errors)

    def decode(self, byte_string, encoding=None, errors='strict'):
        if not isinstance(byte_string, str):
            raise SmppCodecException(
                'Only bytestrings accepted for decoding.')
        encoding = encoding or sys.getdefaultencoding()
        if encoding in self.custom_codecs:
            decoder = self.custom_codecs[encoding].decode
        else:
            decoder = codecs.getdecoder(encoding)
        return decoder(byte_string, errors)
