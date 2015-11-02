from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.amqp_service import AMQPClientService
from vumi.tests.fake_amqp import make_fake_server
from vumi.tests.helpers import VumiTestCase


class TestAMQPService(VumiTestCase):
    def setUp(self):
        self.fake_server = make_fake_server()
        self.broker = self.fake_server.server_factory.broker

    @inlineCallbacks
    def get_server_protocol(self):
        """
        Get a server protocol from our fake server.
        """
        conn = yield self.fake_server.await_connection()
        yield conn.await_connected()
        server = conn.server_protocol
        returnValue(server)

    def get_connected_service(self):
        """
        Get a started service.
        """
        service = AMQPClientService(self.fake_server.endpoint)
        service.startService()
        return service.await_connected()

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
        d = service.await_connected()
        self.assertNoResult(d)
        service.startService()
        yield self.get_server_protocol()
        yield self.broker.wait0()
        self.assertEqual(self.successResultOf(d), service)

    @inlineCallbacks
    def test_protocol_ready_slow(self):
        """
        After the AMQP connection setup handshake, any registered connection
        callbacks are called. In this case, we assume the AMQP channel is not
        yet open when we notice we're connected.
        """
        self.broker.delay_server = True
        service = AMQPClientService(self.fake_server.endpoint)
        d = service.await_connected()
        self.assertNoResult(d)
        service.startService()
        server = yield self.get_server_protocol()
        yield self.broker.wait0()
        self.assertNoResult(d)
        yield server.finish_connecting()
        self.assertEqual(self.successResultOf(d), service)

    @inlineCallbacks
    def test_new_channel(self):
        """
        Once we have a connected client, we can create channels.
        """
        service = yield self.get_connected_service()
        server = yield self.get_server_protocol()
        channel1 = yield service.get_channel()
        self.assertEqual(channel1.channel_number, 1)
        self.assertEqual(server.channels.keys(), [1])
        channel2 = yield service.get_channel()
        self.assertEqual(channel2.channel_number, 2)
        self.assertEqual(set(server.channels.keys()), set([1, 2]))
