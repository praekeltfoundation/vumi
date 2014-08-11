from zope.interface import Interface


class IVumiCodec(Interface):

    def encode(unicode_string, encoding, errors):
        """
        Encode a unicode_string in a specific encoding and return
        the byte string.
        """

    def decode(byte_string, encoding, errors):
        """
        Decode a bytestring in a specific encoding and return the
        unicode string
        """
