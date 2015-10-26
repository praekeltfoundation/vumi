import pika
from pika.adapters.twisted_connection import TwistedProtocolConnection
from twisted.internet.protocol import ClientFactory

from vumi.reconnecting_client import ReconnectingClientService


class AMQPClientService(ReconnectingClientService):
    """
    A service that manages an AMQP client connection
    """
    def __init__(self, endpoint):
        factory = PikaClientFactory(pika.ConnectionParameters())
        ReconnectingClientService.__init__(self, endpoint, factory)
        self.connect_callbacks = []
        self.disconnect_callbacks = []

    def clientConnected(self, protocol):
        ReconnectingClientService.clientConnected(self, protocol)
        print "clientConnected!", protocol
        protocol.ready.addCallback(self.ready_callback)

    def clientConnectionLost(self, reason):
        ReconnectingClientService.clientConnectionLost(self, reason)
        print "clientConnectionLost!", reason
        for cb in self.disconnect_callbacks:
            cb(reason)

    def ready_callback(self, connection):
        print "ready!", connection
        for cb in self.connect_callbacks:
            cb(connection)


class PikaClientFactory(ClientFactory):
    def __init__(self, connection_parameters):
        self.connection_parameters = connection_parameters

    def buildProtocol(self, addr):
        p = TwistedProtocolConnection(self.connection_parameters)
        p.factory = self
        return p
