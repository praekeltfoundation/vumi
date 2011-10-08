"""Transport that writes to and from a console."""

from twisted.internet import reactor
from twisted.internet.protocol import ServerFactory
from twisted.internet.defer import inlineCallbacks
from twisted.conch.telnet import TelnetTransport, TelnetProtocol
from twisted.python import log

from vumi.transports import Transport
from vumi.message import TransportUserMessage


class TelnetTransportProtocol(TelnetProtocol):
    """Extends Twisted's TelnetProtocol for the Telnet transport."""

    def __init__(self, input_handler):
        self.input_handler = input_handler

    def dataReceived(self, data):
        data = data.rstrip('\r\n')
        if data.lower() == '/quit':
            self.loseConnection()
        else:
            self.transport.write("-> %s\r\n" % data)
            self.input_handler(self, data)


class TelnetServerTransport(Transport):

    def validate_config(self):
        print "VALID"
        self.telnet_port = int(self.config['telnet_port'])

    @inlineCallbacks
    def setup_transport(self):
        factory = ServerFactory()
        factory.protocol = lambda: TelnetTransport(TelnetTransportProtocol,
                                                   self.handle_input)
        self.telnet_server = yield reactor.listenTCP(self.telnet_port,
                                                     factory)
        import pdb; pdb.set_trace()
        self._to_addr = self._format_addr(self.telnet_server.port)

    @inlineCallbacks
    def teardown_transport(self):
        if hasattr(self, 'telnet_server'):
            yield self.telnet_server.loseConnection()

    def _format_addr(self, port):
        return "foo"

    def handle_input(self, protocol, text):
        transport_metadata = {
            }
        import pdb; pdb.set_trace()
        self.publish_message(
            from_addr="TODO",
            to_addr=self._to_addr,
            session_event=TransportUserMessage.SESSION_RESUME,
            content=text,
            transport_name=self.transport_name,
            transport_type="telnet",
            transport_metadata=transport_metadata,
            )

    @inlineCallbacks
    def handle_outbound_message(self, message):
        text = message['content']
        if text is None:
            text = ''
        print text
        # session_id = message['transport_metadata']['session_id']
        # lookup transport
        # transport.write("%s\r\n" % text)
