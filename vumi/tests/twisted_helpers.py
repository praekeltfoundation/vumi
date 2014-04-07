from twisted.internet.interfaces import IAddress, ITransport
from twisted.internet.protocol import connectionDone
from zope.interface import implementer


@implementer(IAddress)
class ProtocolConnectorAddress(object):
    pass


@implementer(ITransport)
class ProtocolConnectorTransport(object):
    def __init__(self, protocol, write_func, lose_connection_func):
        self._write_func = write_func
        self._lose_connection_func = lose_connection_func
        self.protocol = protocol
        self.connected = True  # We start off connected.

    def _data_received(self, data):
        self.protocol.dataReceived(data)

    def write(self, data):
        if isinstance(data, unicode):
            raise TypeError("Data must not be unicode")
        self._write_func(data)

    def writeSequence(self, data):
        for item in data:
            self.write(item)

    def _lose_connection(self, reason):
        self.connected = False
        self.protocol.connectionLost(reason)

    def loseConnection(self):
        self._lose_connection_func()

    def getHost(self):
        raise NotImplementedError("TODO")

    def getPeer(self):
        raise NotImplementedError("TODO")


class ProtocolConnector(object):
    def __init__(self, server_protocol, client_protocol):
        self.server_transport = ProtocolConnectorTransport(
            server_protocol, self.write_from_server, self.lose_connection)
        self.client_transport = ProtocolConnectorTransport(
            client_protocol, self.write_from_client, self.lose_connection)
        server_protocol.makeConnection(self.server_transport)
        client_protocol.makeConnection(self.client_transport)

    def write_from_server(self, data):
        self.client_transport._data_received(data)

    def write_from_client(self, data):
        self.server_transport._data_received(data)

    def lose_connection(self):
        self.client_transport._lose_connection(connectionDone)
        self.server_transport._lose_connection(connectionDone)
