from twisted.internet.defer import (
    Deferred, inlineCallbacks, gatherResults, maybeDeferred)
from twisted.internet import reactor
from twisted.internet.protocol import Protocol
from twisted.internet.protocol import Factory, ClientCreator

from vumi.transports.mtn_nigeria.xml_over_tcp import XmlOverTcpClient


def mk_packet(session_id, body):
    return XmlOverTcpClient.serialize_header(session_id, body) + body


class WaitForDataMixin(object):
    waiting_for_data = False
    deferred_data = Deferred()

    def wait_for_data(self):
        d = Deferred()
        self.deferred_data = d
        self.waiting_for_data = True
        return d

    def callback_deferred_data(self, data):
        if self.waiting_for_data and not self.deferred_data.called:
            self.waiting_for_data = False
            self.deferred_data.callback(data)


class MockServerFactory(Factory):
    def __init__(self):
        self.deferred_server = Deferred()


class MockServer(Protocol):
    def connectionMade(self):
        self.factory.deferred_server.callback(self)

    def connectionLost(self, reason):
        self.factory.on_connection_lost.callback(None)


class MockServerMixin(object):
    server_protocol = None

    @inlineCallbacks
    def start_server(self):
        self.server_disconnected = Deferred()
        factory = MockServerFactory()
        factory.on_connection_lost = self.server_disconnected
        factory.protocol = self.server_protocol
        self.server_port = reactor.listenTCP(0, factory, interface='127.0.0.1')
        self.server = yield factory.deferred_server

    def stop_server(self):
        # Turns out stopping these things is tricky.
        # See http://mumak.net/stuff/twisted-disconnect.html
        return gatherResults([
            maybeDeferred(self.server_port.loseConnection),
            self.server_disconnected])

    def get_server_port(self):
        return self.server_port.getHost().port


class MockXmlOverTcpServer(MockServer, WaitForDataMixin):
    def __init__(self):
        self.responses = {}

    def send_data(self, data):
        self.transport.write(data)

    def dataReceived(self, data):
        response = self.responses.get(data)
        if response is not None:
            self.transport.write(response)
        self.callback_deferred_data(data)


class MockXmlOverTcpServerMixin(MockServerMixin):
    server_protocol = MockXmlOverTcpServer


class MockClientMixin(object):
    client_protocol = None

    @inlineCallbacks
    def start_client(self, port):
        self.client_disconnected = Deferred()
        self.client_creator = ClientCreator(reactor, self.client_protocol)
        self.client = yield self.client_creator.connectTCP('127.0.0.1', port)
        conn_lost = self.client.connectionLost

        def connectionLost_wrapper(reason):
            d = maybeDeferred(conn_lost, reason)
            d.chainDeferred(self.client_disconnected)
            return d
        self.client.connectionLost = connectionLost_wrapper

    def stop_client(self):
        self.client.transport.loseConnection()
        return self.client_disconnected


class MockClientServerMixin(MockClientMixin, MockServerMixin):
    @inlineCallbacks
    def start_protocols(self):
        deferred_server = self.start_server()
        yield self.start_client(self.get_server_port())
        yield deferred_server  # we need to wait for the client to connect

    @inlineCallbacks
    def stop_protocols(self):
        yield self.stop_client()
        yield self.stop_server()
