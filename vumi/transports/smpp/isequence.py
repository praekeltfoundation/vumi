from zope.interface import Interface


class ISequence(Interface):

    def next():
        """
        Return the next sequence number.
        """
