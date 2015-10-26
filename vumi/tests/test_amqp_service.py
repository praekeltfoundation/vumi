from pika.frame import decode_frame, Method
from pika.spec import Connection
from twisted.internet.defer import (
    Deferred, DeferredQueue, inlineCallbacks, returnValue)
from twisted.internet.protocol import Protocol

from vumi.amqp_service import AMQPClientService
from vumi.tests.fake_connection import FakeServer, wait0
from vumi.tests.helpers import VumiTestCase


class AMQPServerProtocol(Protocol):
    """
    A very basic wrapper around pika's AMQP wire protocol implementation.
    """

    def __init__(self):
        self._buffer = b""
        self.frame_queue = DeferredQueue()
        self.disconnected_reason = None

    def dataReceived(self, data):
        self._buffer = self._parse_frames(self._buffer + data)

    def _parse_frames(self, data):
        bytes_consumed, frame = decode_frame(data)
        while bytes_consumed > 0:
            self.frame_queue.put(frame)
            data = data[bytes_consumed:]
            bytes_consumed, frame = decode_frame(data)
        return data

    def connectionLost(self, reason):
        self.connected = False
        self.disconnected_reason = reason

    def send_method(self, channel, method):
        """
        Write an AMQP method frame.
        """
        frame = Method(channel, method)
        return self.write(frame.marshal())

    def write(self, data):
        """
        Write some bytes and allow the reactor to send them.
        """
        print "WRITE", repr(data)
        self.transport.write(data)
        return wait0()

    def assert_empty(self):
        assert len(self.frame_queue.pending) == 0, "Pending AMQP frames."

    @inlineCallbacks
    def expect_method(self, method_class):
        frame = yield self.frame_queue.get()
        print "received", frame
        assert frame.NAME == "METHOD"
        assert frame.method.NAME == method_class.NAME
        returnValue(frame.method)


class TestAMQPService(VumiTestCase):
    def setUp(self):
        self.fake_server = FakeServer.for_protocol(AMQPServerProtocol)

    @inlineCallbacks
    def get_server_protocol(self):
        """
        Get a server protocol object which will assert that it has no pending
        frames at cleanup.
        """
        conn = yield self.fake_server.await_connection()
        yield conn.await_connected()
        server = conn.server_protocol
        self.add_cleanup(server.assert_empty)
        returnValue(server)

    @inlineCallbacks
    def get_connected_service(self):
        """
        Get a started service and the server protocol it's connected to.
        """
        service = AMQPClientService(self.fake_server.endpoint)
        service.startService()
        self.add_cleanup(service.stopService)
        server = yield self.get_server_protocol()
        header = yield server.frame_queue.get()
        self.assertEqual(header.NAME, "ProtocolHeader")
        returnValue((service, server))

    @inlineCallbacks
    def test_connect(self):
        """
        The service should successfully connect.
        """
        service = AMQPClientService(self.fake_server.endpoint)
        service.startService()
        self.add_cleanup(service.stopService)
        server = yield self.get_server_protocol()
        header = yield server.frame_queue.get()
        self.assertEqual(header.NAME, "ProtocolHeader")

    @inlineCallbacks
    def test_protocol_ready(self):
        """
        After the AMQP connection setup handshake, any registered connection
        callbacks are called.
        """
        service = AMQPClientService(self.fake_server.endpoint)
        d = Deferred()
        service.connect_callbacks.append(d.callback)
        service.startService()
        self.add_cleanup(service.stopService)
        server = yield self.get_server_protocol()
        header = yield server.frame_queue.get()
        self.assertEqual(header.NAME, "ProtocolHeader")
        self.assertNoResult(d)
        # Server connection handshake.
        yield server.send_method(0, Connection.Start())
        yield server.expect_method(Connection.StartOk)
        yield server.send_method(0, Connection.Tune())
        yield server.expect_method(Connection.TuneOk)
        yield server.expect_method(Connection.Open)
        self.assertNoResult(d)
        yield server.send_method(0, Connection.OpenOk())
        self.successResultOf(d)
