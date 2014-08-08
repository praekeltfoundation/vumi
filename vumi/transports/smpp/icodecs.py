from zope.interface import Interface


class ISmppCodec(Interface):

    def encode(unicode_string, encoding):
        """
        Encode a unicode_string in a specific encoding and return
        the byte string.
        """

    def decode(byte_string, encoding):
        """
        Decode a bytestring in a specific encoding and return the
        unicode string
        """
