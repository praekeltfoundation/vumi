from vumi.transports.xmpp import xmpp


class TestXMLStream(object):

    def __init__(self):
        self.outbox = []

    def send(self, message):
        self.outbox.append(message)


class TestXMPPTransportProtocol(xmpp.XMPPTransportProtocol):

    def __init__(self, *args, **kwargs):
        xmpp.XMPPTransportProtocol.__init__(self, *args, **kwargs)
        self.xmlstream = TestXMLStream()
