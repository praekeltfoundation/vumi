from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import (
    ConnectionRefusedError, UnknownHostError, ConnectionDone,
    ConnectionAborted)
from twisted.internet.protocol import Protocol, ClientFactory, ServerFactory
from twisted.trial.unittest import TestCase

from vumi.tests.fake_connection import FakeServer, wait0, ProtocolDouble


class DummyServerProtocol(ProtocolDouble):
    side = "server"


class DummyClientProtocol(ProtocolDouble):
    side = "client"


class TestFakeConnection(TestCase):
    """
    Tests for FakeConnection and friends.
    """

    def setUp(self):
        self.client_factory = ClientFactory.forProtocol(DummyClientProtocol)
        self.server_factory = ServerFactory.forProtocol(DummyServerProtocol)

    def connect_client(self, fake_server):
        """
        Create a client connection to a fake server.

        :returns: (connection, client)
        """
        conn_d = fake_server.await_connection()
        self.assertNoResult(conn_d)  # We don't want an existing connection.
        client_d = fake_server.endpoint.connect(self.client_factory)
        client = self.successResultOf(client_d)
        conn = self.successResultOf(conn_d)
        self.assert_connected(conn, client)
        return (conn, client)

    def assert_pending(self, conn):
        """
        Assert that a connection is not yet connected.
        """
        self.assertEqual(conn.client_protocol, None)
        self.assertEqual(conn.server_protocol, None)
        self.assertEqual(conn.connected, False)
        self.assertEqual(conn.pending, True)
        self.assertNoResult(conn._accept_d)
        self.assertNoResult(conn._connected_d)
        self.assertNoResult(conn._finished_d)

    def assert_connected(self, conn, client):
        """
        Assert that a connection is connected to a client.
        """
        self.assertIsInstance(conn.client_protocol, DummyClientProtocol)
        self.assertEqual(conn.client_protocol.side, "client")
        self.assertEqual(conn.client_protocol.connected, True)
        self.assertEqual(conn.client_protocol.disconnected_reason, None)
        self.assertIsInstance(conn.server_protocol, DummyServerProtocol)
        self.assertEqual(conn.server_protocol.side, "server")
        self.assertEqual(conn.server_protocol.connected, True)
        self.assertEqual(conn.server_protocol.disconnected_reason, None)
        self.assertEqual(conn.connected, True)
        self.assertEqual(conn.pending, False)
        self.successResultOf(conn._accept_d)
        self.successResultOf(conn._connected_d)
        self.assertNoResult(conn._finished_d)
        self.assertEqual(conn.client_protocol, client)

    def assert_connection_rejected(self, conn):
        self.assertEqual(conn.client_protocol, None)
        self.assertEqual(conn.server_protocol, None)
        self.assertEqual(conn.connected, False)
        self.assertEqual(conn.pending, False)
        self.assertNoResult(conn._connected_d)
        self.assertNoResult(conn._finished_d)

    def assert_disconnected(self, conn, client_reason=ConnectionDone,
                            server_reason=ConnectionDone):
        self.assertEqual(conn.client_protocol.connected, False)
        self.assertEqual(conn.server_protocol.connected, False)
        client_reason_f = conn.client_protocol.disconnected_reason
        server_reason_f = conn.server_protocol.disconnected_reason
        self.assertEqual(client_reason_f.check(client_reason), client_reason)
        self.assertEqual(server_reason_f.check(server_reason), server_reason)

    def test_client_connect_auto(self):
        """
        A server will automatically accept client connections by default.
        """
        fake_server = FakeServer(self.server_factory)
        conn_d = fake_server.await_connection()
        self.assertNoResult(conn_d)

        client_d = fake_server.endpoint.connect(self.client_factory)
        client = self.successResultOf(client_d)
        conn = self.successResultOf(conn_d)
        self.assert_connected(conn, client)

    def test_accept_connection(self):
        """
        Connections can be accepted manually if desired.
        """
        fake_server = FakeServer(self.server_factory, auto_accept=False)
        conn_d = fake_server.await_connection()
        self.assertNoResult(conn_d)

        client_d = fake_server.endpoint.connect(self.client_factory)
        self.assertNoResult(client_d)
        conn = self.successResultOf(conn_d)
        self.assert_pending(conn)

        connected_d = conn.await_connected()
        self.assertNoResult(connected_d)

        accepted_d = conn.accept_connection()
        self.successResultOf(accepted_d)
        self.successResultOf(connected_d)
        client = self.successResultOf(client_d)
        self.assert_connected(conn, client)

    def test_reject_connection(self):
        """
        Connections can be rejected manually if desired.
        """
        fake_server = FakeServer(self.server_factory, auto_accept=False)
        conn_d = fake_server.await_connection()
        self.assertNoResult(conn_d)

        client_d = fake_server.endpoint.connect(self.client_factory)
        self.assertNoResult(client_d)
        conn = self.successResultOf(conn_d)
        self.assert_pending(conn)

        connected_d = conn.await_connected()
        self.assertNoResult(connected_d)

        conn.reject_connection()
        self.assertNoResult(connected_d)
        self.failureResultOf(client_d, ConnectionRefusedError)
        self.assert_connection_rejected(conn)

    def test_reject_connection_with_reason(self):
        """
        Connections can be rejected with a custom reason.
        """
        fake_server = FakeServer(self.server_factory, auto_accept=False)
        conn_d = fake_server.await_connection()
        self.assertNoResult(conn_d)

        client_d = fake_server.endpoint.connect(self.client_factory)
        self.assertNoResult(client_d)
        conn = self.successResultOf(conn_d)
        self.assert_pending(conn)

        connected_d = conn.await_connected()
        self.assertNoResult(connected_d)

        conn.reject_connection(UnknownHostError())
        self.assertNoResult(connected_d)
        self.failureResultOf(client_d, UnknownHostError)
        self.assert_connection_rejected(conn)

    @inlineCallbacks
    def test_client_disconnect(self):
        """
        If the client disconnects, the server's connection is also lost.
        """
        fake_server = FakeServer(self.server_factory)
        conn, client = self.connect_client(fake_server)
        finished_d = conn.await_finished()
        self.assertNoResult(finished_d)

        # The disconnection gets scheduled, but doesn't actually happen until
        # the next reactor tick.
        client.transport.loseConnection()
        self.assert_connected(conn, client)
        self.assertNoResult(finished_d)

        # Allow the reactor to run so the disconnection gets processed.
        yield wait0()
        self.assert_disconnected(conn)
        self.successResultOf(finished_d)

    @inlineCallbacks
    def test_server_disconnect(self):
        """
        If the server disconnects, the client's connection is also lost.
        """
        fake_server = FakeServer(self.server_factory)
        conn, client = self.connect_client(fake_server)
        finished_d = conn.await_finished()
        self.assertNoResult(finished_d)

        # The disconnection gets scheduled, but doesn't actually happen until
        # the next reactor tick.
        conn.server_protocol.transport.loseConnection()
        self.assert_connected(conn, client)
        self.assertNoResult(finished_d)

        # Allow the reactor to run so the disconnection gets processed.
        yield wait0()
        self.assert_disconnected(conn)
        self.successResultOf(finished_d)

    @inlineCallbacks
    def test_client_abort(self):
        """
        If the client aborts, the server's connection is also lost.
        """
        fake_server = FakeServer(self.server_factory)
        conn, client = self.connect_client(fake_server)
        finished_d = conn.await_finished()
        self.assertNoResult(finished_d)

        # The disconnection gets scheduled, but doesn't actually happen until
        # the next reactor tick.
        client.transport.abortConnection()
        self.assert_connected(conn, client)
        self.assertNoResult(finished_d)

        # Allow the reactor to run so the disconnection gets processed.
        yield wait0()
        self.assert_disconnected(conn, client_reason=ConnectionAborted)
        self.successResultOf(finished_d)

    @inlineCallbacks
    def test_server_abort(self):
        """
        If the server aborts, the client's connection is also lost.
        """
        fake_server = FakeServer(self.server_factory)
        conn, client = self.connect_client(fake_server)
        finished_d = conn.await_finished()
        self.assertNoResult(finished_d)

        # The disconnection gets scheduled, but doesn't actually happen until
        # the next reactor tick.
        conn.server_protocol.transport.abortConnection()
        self.assert_connected(conn, client)
        self.assertNoResult(finished_d)

        # Allow the reactor to run so the disconnection gets processed.
        yield wait0()
        self.assert_disconnected(conn, server_reason=ConnectionAborted)
        self.successResultOf(finished_d)

    @inlineCallbacks
    def test_send_client_to_server(self):
        """
        Bytes can be sent from the client to the server.
        """
        fake_server = FakeServer(self.server_factory)
        conn, client = self.connect_client(fake_server)
        server = conn.server_protocol
        self.assertEqual(server.received, b"")

        # Bytes sent, but not received until reactor runs.
        d = client.write(b"foo")
        self.assertEqual(server.received, b"")
        yield d
        self.assertEqual(server.received, b"foo")

        client.write(b"bar")
        d = client.write(b"baz")
        self.assertEqual(server.received, b"foo")
        yield d
        self.assertEqual(server.received, b"foobarbaz")

    @inlineCallbacks
    def test_send_server_to_client(self):
        """
        Bytes can be sent from the server to the client.
        """
        fake_server = FakeServer(self.server_factory)
        conn, client = self.connect_client(fake_server)
        server = conn.server_protocol
        self.assertEqual(server.received, b"")

        # Bytes sent, but not received until reactor runs.
        d = server.write(b"foo")
        self.assertEqual(client.received, b"")
        yield d
        self.assertEqual(client.received, b"foo")

        # Send two sets of bytes at once, waiting for the second.
        server.write(b"bar")
        d = server.write(b"baz")
        self.assertEqual(client.received, b"foo")
        yield d
        self.assertEqual(client.received, b"foobarbaz")

    @inlineCallbacks
    def test_server_for_protocol(self):
        """
        A FakeServer can also be constructed from a a protocol class instead of
        a factory.
        """
        class MyProtocol(Protocol):
            pass
        fake_server = FakeServer.for_protocol(MyProtocol)
        self.assertEqual(fake_server.server_factory.protocol, MyProtocol)
        self.assertNotEqual(fake_server.server_factory.protocol, Protocol)

        yield fake_server.endpoint.connect(self.client_factory)
        conn = yield fake_server.await_connection()
        self.assertIsInstance(conn.server_protocol, MyProtocol)
