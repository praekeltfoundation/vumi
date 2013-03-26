# -*- test-case-name: vumi.transports.telnet.tests.test_telnet -*-

"""Transport that sends and receives to telnet clients."""

from twisted.internet import reactor
from twisted.internet.protocol import ServerFactory
from twisted.internet.defer import inlineCallbacks
from twisted.conch.telnet import TelnetTransport, TelnetProtocol
from twisted.python import log

from vumi.transports import Transport
from vumi.message import TransportUserMessage


class TelnetTransportProtocol(TelnetProtocol):
    """Extends Twisted's TelnetProtocol for the Telnet transport."""

    def __init__(self, vumi_transport):
        self.vumi_transport = vumi_transport

    def connectionMade(self):
        self.vumi_transport.register_client(self)

    def connectionLost(self, reason):
        self.vumi_transport.deregister_client(self)

    def dataReceived(self, data):
        data = data.rstrip('\r\n')
        if data.lower() == '/quit':
            self.loseConnection()
        else:
            self.vumi_transport.handle_input(self, data)


class TelnetServerTransport(Transport):
    """Telnet based transport.

    This transport listens on a specified port for telnet
    clients and routes lines to and from connected clients.

    Telnet transport options:

    :type telnet_port: int
    :param telnet_port:
        Port for the telnet server to listen on.
    :type to_addr: str
    :param to_addr:
        The to_addr to use for the telnet server.
        Defaults to 'host:port'.
    """

    def validate_config(self):
        self.telnet_port = int(self.config['telnet_port'])
        self._to_addr = self.config.get('to_addr')

    @inlineCallbacks
    def setup_transport(self):
        self._clients = {}

        def protocol():
            return TelnetTransport(TelnetTransportProtocol, self)

        factory = ServerFactory()
        factory.protocol = protocol
        self.telnet_server = yield reactor.listenTCP(self.telnet_port,
                                                     factory)
        if self._to_addr is None:
            self._to_addr = self._format_addr(self.telnet_server.getHost())

    @inlineCallbacks
    def teardown_transport(self):
        if hasattr(self, 'telnet_server'):
            yield self.telnet_server.loseConnection()

    def _format_addr(self, addr):
        return "%s:%s" % (addr.host, addr.port)

    def _client_addr(self, client):
        return self._format_addr(client.transport.getPeer())

    def register_client(self, client):
        client_addr = self._client_addr(client)
        log.msg("Registering client connected from %r" % client_addr)
        self._clients[client_addr] = client
        self.send_inbound_message(client, None,
                                  TransportUserMessage.SESSION_NEW)

    def deregister_client(self, client):
        log.msg("Deregistering client.")
        self.send_inbound_message(client, None,
                                  TransportUserMessage.SESSION_CLOSE)
        client_addr = self._client_addr(client)
        del self._clients[client_addr]

    def handle_input(self, client, text):
        self.send_inbound_message(client, text,
                                  TransportUserMessage.SESSION_RESUME)

    def send_inbound_message(self, client, text, session_event):
        client_addr = self._client_addr(client)
        self.publish_message(
            from_addr=client_addr,
            to_addr=self._to_addr,
            session_event=session_event,
            content=text,
            transport_name=self.transport_name,
            transport_type="telnet",
            )

    def handle_outbound_message(self, message):
        text = message['content']
        if text is None:
            text = u''
        text = u"\n".join(text.splitlines())

        client_addr = message['to_addr']
        client = self._clients.get(client_addr)
        if client is None:
            # unknown addr, deliver to all
            clients = self._clients.values()
            text = u"UNKNOWN ADDR [%s]: %s" % (client_addr, text)
        else:
            clients = [client]

        text = text.encode('utf-8')

        for client in clients:
            client.transport.write("%s\n" % text)
            if message['session_event'] == TransportUserMessage.SESSION_CLOSE:
                client.transport.loseConnection()
