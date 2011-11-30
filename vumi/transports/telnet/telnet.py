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
    """

    def validate_config(self):
        self.telnet_port = int(self.config['telnet_port'])

    @inlineCallbacks
    def setup_transport(self):
        self._clients = {}

        def protocol():
            return TelnetTransport(TelnetTransportProtocol, self)

        factory = ServerFactory()
        factory.protocol = protocol
        self.telnet_server = yield reactor.listenTCP(self.telnet_port,
                                                     factory)
        self._to_addr = self._format_addr(self.telnet_server.getHost())

    @inlineCallbacks
    def teardown_transport(self):
        if hasattr(self, 'telnet_server'):
            yield self.telnet_server.loseConnection()

    def _format_addr(self, addr):
        return "%s:%s" % (addr.host, addr.port)

    def register_client(self, client):
        client_addr = self._format_addr(client.transport.getPeer())
        log.msg("Registering client connected from %r" % client_addr)
        self._clients[id(client)] = client
        self.send_inbound_message(client, None,
                                  TransportUserMessage.SESSION_NEW)

    def deregister_client(self, client):
        log.msg("Deregistering client.")
        self.send_inbound_message(client, None,
                                  TransportUserMessage.SESSION_CLOSE)
        del self._clients[id(client)]

    def handle_input(self, client, text):
        self.send_inbound_message(client, text,
                                  TransportUserMessage.SESSION_RESUME)

    def send_inbound_message(self, client, text, session_event):
        from_addr = str(client.transport.getPeer().host)
        transport_metadata = {'session_id': id(client)}
        self.publish_message(
            from_addr=from_addr,
            to_addr=self._to_addr,
            session_event=session_event,
            content=text,
            transport_name=self.transport_name,
            transport_type="telnet",
            transport_metadata=transport_metadata,
            )

    def handle_outbound_message(self, message):
        client_id = message['transport_metadata']['session_id']
        client = self._clients.get(client_id)
        if client is None:
            # client gone, don't deliver
            return

        text = message['content']
        if text is None:
            text = ''
        else:
            text = text.encode("UTF-8")

        text = "\n".join(text.splitlines())
        client.transport.write("%s\n" % text)

        if message['session_event'] == TransportUserMessage.SESSION_CLOSE:
            client.transport.loseConnection()
