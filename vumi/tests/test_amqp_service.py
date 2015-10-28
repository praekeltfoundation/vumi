from pika.spec import Connection
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue

from vumi.amqp_service import AMQPClientService
from vumi.tests.fake_connection import FakeServer, wait0
from vumi.tests.new_fake_amqp import FakeAMQPFactory
from vumi.tests.helpers import VumiTestCase


class TestAMQPService(VumiTestCase):
    def setUp(self):
        self.fake_server = FakeServer(FakeAMQPFactory())
        self.broker = self.fake_server.server_factory.broker

    @inlineCallbacks
    def get_server_protocol(self):
        """
        Get a server protocol object which will assert that it has no pending
        frames at cleanup.
        """
        conn = yield self.fake_server.await_connection()
        yield conn.await_connected()
        server = conn.server_protocol
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
        self.assertEqual(self.broker.server_protocols, [])
        service.startService()
        self.add_cleanup(service.stopService)
        server = yield self.get_server_protocol()
        self.assertEqual(self.broker.server_protocols, [server])

    @inlineCallbacks
    def test_protocol_ready_quick(self):
        """
        After the AMQP connection setup handshake, any registered connection
        callbacks are called. In this case, we assume the AMQP channel is
        already open by the time we notice we're connected.
        """
        service = AMQPClientService(self.fake_server.endpoint)
        d = Deferred()
        service.connect_callbacks.append(d.callback)
        self.assertNoResult(d)
        service.startService()
        yield self.get_server_protocol()
        yield wait0()
        self.successResultOf(d)

    @inlineCallbacks
    def test_protocol_ready_slow(self):
        """
        After the AMQP connection setup handshake, any registered connection
        callbacks are called. In this case, we assume the AMQP channel is not
        yet open when we notice we're connected.
        """
        self.broker.delay_server = True
        service = AMQPClientService(self.fake_server.endpoint)
        d = Deferred()
        service.connect_callbacks.append(d.callback)
        self.assertNoResult(d)
        service.startService()
        server = yield self.get_server_protocol()
        yield wait0()
        self.assertNoResult(d)
        yield server.finish_connecting()
        self.successResultOf(d)
