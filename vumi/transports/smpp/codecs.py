# -*- test-case-name: vumi.transports.smpp.tests.test_codecs -*-
from vumi.transports.smpp.icodecs import ISmppCodec

from zope.interface import implements


class SmppCodecException(Exception):
    pass


class SmppCodec(object):
    implements(ISmppCodec)

    def encode(self, unicode_string, encoding):
        if not isinstance(unicode_string, unicode):
            raise SmppCodecException(
                'Only Unicode strings accepted for encoding.')
        return unicode_string.encode(encoding)

    def decode(self, byte_string, encoding):
        if not isinstance(byte_string, str):
            raise SmppCodecException(
                'Only bytestrings accepted for decoding.')
        return byte_string.decode(encoding)
