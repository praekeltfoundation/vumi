from vumi.transports.xmpp import xmpp


class TestXMLStream(object):

    def __init__(self):
        self.outbox = []

    def send(self, message):
        self.outbox.append(message)

    def addObserver(self, event, observerfn, *args, **kwargs):
        """Ignore."""


class TestXMPPClient(xmpp.XMPPClient):
    def __init__(self, *args, **kw):
        xmpp.XMPPClient.__init__(self, *args, **kw)
        self._connection = None

    def startService(self):
        pass

    def stopService(self):
        pass


class TestXMPPTransportProtocol(xmpp.XMPPTransportProtocol):

    def __init__(self, *args, **kwargs):
        xmpp.XMPPTransportProtocol.__init__(self, *args, **kwargs)
        self.xmlstream = TestXMLStream()
