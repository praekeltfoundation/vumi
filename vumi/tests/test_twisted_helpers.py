from twisted.internet.interfaces import ITransport
from twisted.internet.protocol import Protocol
from zope.interface.verify import verifyObject

from vumi.tests.helpers import VumiTestCase
from vumi.tests.twisted_helpers import (
    ProtocolConnector, ProtocolConnectorTransport)


class BufferProtocol(Protocol):
    def __init__(self):
        self.data = ''
        self.closed = False
        self.closed_reason = None

    def dataReceived(self, data):
        self.data += data

    def connectionLost(self, reason):
        self.closed = True
        self.closed_reason = reason


class TestProtocolConnectorTransport(VumiTestCase):
    def test_implements_interface(self):
        verifyObject(ITransport, ProtocolConnectorTransport(None, None, None))

    def test_write_bytes(self):
        """
        When given bytes, .write() should call write_func().
        """
        data = []
        transport = ProtocolConnectorTransport(
            BufferProtocol(), data.append, None)
        self.assertEqual(data, [])
        transport.write('foo')
        self.assertEqual(data, ['foo'])
        transport.write('bar')
        self.assertEqual(data, ['foo', 'bar'])

    def test_write_unicode(self):
        """
        When given unicode, .write() should raise TypeError.
        """
        transport = ProtocolConnectorTransport(BufferProtocol(), None, None)
        self.assertRaises(TypeError, transport.write, u'foo')

    def test_writeSequence_bytes(self):
        """
        When given bytes, .writeSequence() should call write_func().
        """
        data = []
        transport = ProtocolConnectorTransport(
            BufferProtocol(), data.append, None)
        self.assertEqual(data, [])
        transport.writeSequence(['fo', 'o'])
        self.assertEqual(data, ['fo', 'o'])
        transport.writeSequence('bar')
        self.assertEqual(data, ['fo', 'o', 'b', 'a', 'r'])

    def test_writeSequence_unicode(self):
        """
        When given unicode, .writeSequence() should raise TypeError.
        """
        transport = ProtocolConnectorTransport(BufferProtocol(), None, None)
        self.assertRaises(TypeError, transport.writeSequence, [u'fo', u'o'])
        self.assertRaises(TypeError, transport.writeSequence, u'bar')

    def test_loseConnection(self):
        """
        .loseConnection() should call lose_connection_func() and set
        .connected=False.
        """
        called = []
        transport = ProtocolConnectorTransport(
            BufferProtocol(), None, lambda: called.append(1))
        transport.loseConnection()
        self.assertEqual(called, [1])


class TestProtocolConnector(VumiTestCase):
    def test_create_protocol_connector(self):
        """
        A new ProtocolConnector instance should connect two protocols together.
        """
        server_protocol = BufferProtocol()
        client_protocol = BufferProtocol()
        connector = ProtocolConnector(server_protocol, client_protocol)
        self.assertEqual(connector.server_transport.protocol, server_protocol)
        self.assertEqual(connector.client_transport.protocol, client_protocol)
        self.assertEqual(server_protocol.transport, connector.server_transport)
        self.assertEqual(client_protocol.transport, connector.client_transport)

    def test_client_write(self):
        """
        Data written to the client transport should be sent to the server.
        """
        server_protocol = BufferProtocol()
        client_protocol = BufferProtocol()
        connector = ProtocolConnector(server_protocol, client_protocol)
        self.assertEqual(server_protocol.data, '')
        connector.client_transport.write('foo')
        self.assertEqual(server_protocol.data, 'foo')

    def test_server_write(self):
        """
        Data written to the server transport should be sent to the client.
        """
        server_protocol = BufferProtocol()
        client_protocol = BufferProtocol()
        connector = ProtocolConnector(server_protocol, client_protocol)
        self.assertEqual(client_protocol.data, '')
        connector.server_transport.write('foo')
        self.assertEqual(client_protocol.data, 'foo')

    def test_client_lose_connection(self):
        """
        Closing the client connection the should also close the server
        connection.
        """
        server_protocol = BufferProtocol()
        client_protocol = BufferProtocol()
        connector = ProtocolConnector(server_protocol, client_protocol)
        self.assertEqual(client_protocol.closed, False)
        self.assertEqual(server_protocol.closed, False)
        connector.client_transport.loseConnection()
        self.assertEqual(client_protocol.closed, True)
        self.assertEqual(server_protocol.closed, True)

    def test_server_lose_connection(self):
        """
        Closing the server connection the should also close the client
        connection.
        """
        server_protocol = BufferProtocol()
        client_protocol = BufferProtocol()
        connector = ProtocolConnector(server_protocol, client_protocol)
        self.assertEqual(client_protocol.closed, False)
        self.assertEqual(server_protocol.closed, False)
        connector.server_transport.loseConnection()
        self.assertEqual(client_protocol.closed, True)
        self.assertEqual(server_protocol.closed, True)
