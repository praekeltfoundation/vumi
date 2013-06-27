from twisted.internet import reactor
from twisted.web import xmlrpc, server

from vumi import log
from vumi.transports.base import Transport
from vumi.config import ConfigText


class MTNRwandaUSSDTransportConfig(Transport.CONFIG_CLASS):
    """
    MTN Rwanda USSD transport configuration.
    """
    # TODO: Configure elements described in chapter 8 of the spec


class MTNRwandaUSSDTransport(Transport):
    """

    """

    transport_type = 'ussd'
    xmlrpc_server = None


    CONFIG_CLASS = MTNRwandaUSSDTransportConfig

    def validate_config(self):
        # Hard-coded for now.
        # TODO: self.config.get()
        self.port = 7080


    @inlineCallbacks
    def setup_transport(self):
        """
        Transport specific setup - it initiates things, sets up a
        connection, for example.

        :self.xmlrpc_server: An IListeningPort instance.
        """
        r = MTNRwandaXMLRPCResource()
        factory = server.Site(r)
        self.xmlrpc_server = yield reactor.listenTCP(self.port, factory)


    @inlineCallbacks
    def teardown_transport(self):
        """
        Clean-up of setup done in setup_transport.
        """
        if self.xmlrpc_server is not None:
            yield self.xmlrpc_server.stopListening()


    def handle_outbound_message(self, message):
        """
        Read outbound message and do what needs to be done with them.
        """


    def handle_raw_inbound_message(self):
        """
        Called by the XML-RPC server when it receives a payload that
        needs processing.
        """


class MTNRwandaXMLRPCResource(xmlrpc.XMLRPC):
    """
    A Resource object implementing XML-RPC, can be published using
    twisted.web.server.Site.
    """

    def __init__(self, transport):
        xmlrpc.XMLRPC.__init__(self)
