# -*- test-case-name: vumi.codecs.tests.test_vumi_codecs -*-
# -*- coding: utf-8 -*-
import codecs
import sys

from vumi.codecs.ivumi_codecs import IVumiCodec

from zope.interface import implements


class VumiCodecException(Exception):
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

    gsm_basic_charset_map = dict(
        (l, i) for i, l in enumerate(gsm_basic_charset))

    gsm_extension = (
        u"````````````````````^```````````````````{}`````\\````````````[~]`"
        u"|````````````````````````````````````€``````````````````````````")

    gsm_extension_map = dict((l, i) for i, l in enumerate(gsm_extension))

    def encode(self, unicode_string, errors='strict'):
        result = []
        for position, c in enumerate(unicode_string):
            idx = self.gsm_basic_charset_map.get(c)
            if idx is not None:
                result.append(chr(idx))
                continue
            idx = self.gsm_extension_map.get(c)
            if idx is not None:
                result.append(chr(27) + chr(idx))
            else:
                result.append(
                    self.handle_encode_error(
                        c, errors, position, unicode_string))

        obj = ''.join(result)
        return (obj, len(obj))

    def handle_encode_error(self, char, handler_type, position, obj):
        handler = getattr(
            self, 'handle_encode_%s_error' % (handler_type,), None)
        if handler is None:
            raise VumiCodecException(
                'Invalid errors type %s for GSM7BitCodec', handler_type)
        return handler(char, position, obj)

    def handle_encode_strict_error(self, char, position, obj):
        raise UnicodeEncodeError(
            'gsm0338', char, position, position + 1, repr(obj))

    def handle_encode_ignore_error(self, char, position, obj):
        return ''

    def handle_encode_replace_error(self, char, position, obj):
        return chr(self.gsm_basic_charset_map.get('?'))

    def decode(self, byte_string, errors='strict'):
        res = iter(byte_string)
        result = []
        for position, c in enumerate(res):
            try:
                if c == chr(27):
                    c = next(res)
                    result.append(self.gsm_extension[ord(c)])
                else:
                    result.append(self.gsm_basic_charset[ord(c)])
            except IndexError:
                result.append(
                    self.handle_decode_error(c, errors, position, byte_string))

        obj = u''.join(result)
        return (obj, len(obj))

    def handle_decode_error(self, char, handler_type, position, obj):
        handler = getattr(
            self, 'handle_decode_%s_error' % (handler_type,), None)
        if handler is None:
            raise VumiCodecException(
                'Invalid errors type %s for GSM7BitCodec', handler_type)
        return handler(char, position, obj)

    def handle_decode_strict_error(self, char, position, obj):
        raise UnicodeDecodeError(
            'gsm0338', char, position, position + 1, obj)

    def handle_decode_ignore_error(self, char, position, obj):
        return u''

    def handle_decode_replace_error(self, char, position, obj):
        return u'?'


class UCS2Codec(codecs.Codec):
    """
    UCS2 is for all intents & purposes assumed to be the same as
    big endian UTF16.
    """
    def encode(self, input, errors='strict'):
        return codecs.utf_16_be_encode(input, errors)

    def decode(self, input, errors='strict'):
        return codecs.utf_16_be_decode(input, errors)


class VumiCodec(object):
    implements(IVumiCodec)

    custom_codecs = {
        'gsm0338': GSM7BitCodec(),
        'ucs2': UCS2Codec()
    }

    def encode(self, unicode_string, encoding=None, errors='strict'):
        if not isinstance(unicode_string, unicode):
            raise VumiCodecException(
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
            raise VumiCodecException(
                'Only bytestrings accepted for decoding.')
        encoding = encoding or sys.getdefaultencoding()
        if encoding in self.custom_codecs:
            decoder = self.custom_codecs[encoding].decode
        else:
            decoder = codecs.getdecoder(encoding)
        obj, length = decoder(byte_string, errors)
        return obj
