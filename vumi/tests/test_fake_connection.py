from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import (
    ConnectionRefusedError, UnknownHostError, ConnectionDone,
    ConnectionAborted)
from twisted.internet.protocol import Protocol, ClientFactory, ServerFactory
from twisted.trial.unittest import TestCase
from twisted.web.client import readBody
from twisted.web.iweb import IAgent
from twisted.web.server import NOT_DONE_YET

from vumi.tests.fake_connection import (
    FakeServer, wait0, ProtocolDouble, FakeHttpServer)


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

    def test_client_connect_hook(self):
        """
        An on_connect function can be passed to the server to be called
        whenever a connection is made.
        """
        def on_connect(conn):
            conn.hook_was_called = True
            conn.client_id_from_hook = id(conn.client_protocol)
            conn.server_id_from_hook = id(conn.server_protocol)

        fake_server = FakeServer(self.server_factory, on_connect=on_connect)
        conn_d = fake_server.await_connection()
        self.assertNoResult(conn_d)

        client_d = fake_server.endpoint.connect(self.client_factory)
        client = self.successResultOf(client_d)
        conn = self.successResultOf(conn_d)
        self.assert_connected(conn, client)
        self.assertEqual(conn.hook_was_called, True)
        self.assertEqual(conn.client_id_from_hook, id(client))
        self.assertEqual(conn.server_id_from_hook, id(conn.server_protocol))

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


class TestFakeHttpServer(TestCase):
    @inlineCallbacks
    def assert_response(self, response, code, body):
        self.assertEqual(response.code, code)
        response_body = yield readBody(response)
        self.assertEqual(response_body, body)

    @inlineCallbacks
    def test_simple_request(self):
        """
        FakeHttpServer can handle a simple HTTP request using the IAgent
        provider it supplies.
        """
        requests = []
        fake_http = FakeHttpServer(lambda req: requests.append(req) or "hi")
        agent = fake_http.get_agent()
        self.assertTrue(IAgent.providedBy(agent))
        response = yield agent.request("GET", "http://example.com/hello")
        # We got a valid request and returned a valid response.
        [request] = requests
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.path, "http://example.com/hello")
        yield self.assert_response(response, 200, "hi")

    @inlineCallbacks
    def test_async_response(self):
        """
        FakeHttpServer supports asynchronous responses.
        """
        requests = []
        fake_http = FakeHttpServer(
            lambda req: requests.append(req) or NOT_DONE_YET)
        response1_d = fake_http.get_agent().request(
            "GET", "http://example.com/hello/1")
        response2_d = fake_http.get_agent().request(
            "HEAD", "http://example.com/hello/2")

        # Wait for the requests to arrive.
        yield wait0()
        [request1, request2] = requests
        self.assertNoResult(response1_d)
        self.assertNoResult(response2_d)
        self.assertEqual(request1.method, "GET")
        self.assertEqual(request1.path, "http://example.com/hello/1")
        self.assertEqual(request2.method, "HEAD")
        self.assertEqual(request2.path, "http://example.com/hello/2")

        # Send a response to the second request.
        request2.finish()
        response2 = yield response2_d
        self.assertNoResult(response1_d)
        yield self.assert_response(response2, 200, "")

        # Send a response to the first request.
        request1.write("Thank you for waiting.")
        request1.finish()
        response1 = yield response1_d
        yield self.assert_response(response1, 200, "Thank you for waiting.")

    @inlineCallbacks
    def test_POST_request(self):
        """
        FakeHttpServer can handle a POST request.
        """
        requests = []
        fake_http = FakeHttpServer(lambda req: requests.append(req) or "hi")
        agent = fake_http.get_agent()
        self.assertTrue(IAgent.providedBy(agent))
        response = yield agent.request("POST", "http://example.com/hello")
        # We got a valid request and returned a valid response.
        [request] = requests
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.path, "http://example.com/hello")
        yield self.assert_response(response, 200, "hi")

    @inlineCallbacks
    def test_PUT_request(self):
        """
        FakeHttpServer can handle a PUT request.
        """
        requests = []
        fake_http = FakeHttpServer(lambda req: requests.append(req) or "hi")
        agent = fake_http.get_agent()
        self.assertTrue(IAgent.providedBy(agent))
        response = yield agent.request("PUT", "http://example.com/hello")
        # We got a valid request and returned a valid response.
        [request] = requests
        self.assertEqual(request.method, "PUT")
        self.assertEqual(request.path, "http://example.com/hello")
        yield self.assert_response(response, 200, "hi")
