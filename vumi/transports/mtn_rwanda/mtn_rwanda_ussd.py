# -*- test-case-name: vumi.transports.mtn_rwanda.tests.test_mtn_rwanda_ussd -*-

from twisted.internet import reactor
from twisted.web import xmlrpc, server, http
from twisted.internet.defer import inlineCallbacks

from vumi.transports.base import Transport
from vumi.config import ConfigServerEndpoint


class MTNRwandaUSSDTransportConfig(Transport.CONFIG_CLASS):
    """
    MTN Rwanda USSD transport configuration.
    """
    server_endpoint = ConfigServerEndpoint(
        "The listening endpoint that the remote client will connect to.",
        required=True, static=True)


class MTNRwandaUSSDTransport(Transport):
    """

    """

    transport_type = 'ussd'
    xmlrpc_server = None

    CONFIG_CLASS = MTNRwandaUSSDTransportConfig
    ENCODING = 'UTF-8'

    @inlineCallbacks
    def setup_transport(self):
        """
        Transport specific setup - it initiates things, sets up a
        connection, for example.

        self.xmlrpc_server: An IListeningPort instance.
        """

        config = self.get_static_config()
        self.endpoint = config.server_endpoint
        r = MTNRwandaXMLRPCResource(self)
        self.factory = server.Site(r)
        self.xmlrpc_server = yield self.endpoint.listen(self.factory)

    @inlineCallbacks
    def teardown_transport(self):
        """
        Clean-up of setup done in setup_transport.
        """
        if self.xmlrpc_server is not None:
            yield self.xmlrpc_server.stopListening()

    def handle_raw_inbound_request(self, message_id, request):
        """
        Called by the XML-RPC server when it receives a payload that
        needs processing.
        """
        # this should be called when the relevant XML-RPC function
        # is called by the XML-RPC client at MTN.
        #
        # The tricky bit here is that the XML-RPC interface is synchronous
        # while our internal architecture is async. This means we need to
        # hold on to the connection (and keep a UUID reference to it)
        # so we can map the async-reply arriving over AMQP back to the
        # HTTP-request that's still open and waiting for a response.
        #
        # When this is called the resource holding on to the request
        # w/ NOT_DONE_YET generates the message_id and links the message_id
        # in memory to the actual request object.
        #
        # In the message we publish over AMQP we use this message_id.
        # When a reply arrives via AMQP on `handle_outbound_message` it refers
        # back to that message_id again in the `in_reply_to` field.
        #
        # That way you can map the reply to the HTTP request still waiting
        # a response. You generate the correct XML-RPC reply from the
        # message that arrived over AMQP and then you close the HTTP Request.

    def send_response(self, message_id, **args):
        # TODO


    @inlineCallbacks
    def handle_outbound_message(self, message):
        """
        Read outbound message and do what needs to be done with them.
        """
        # here we look up the message['in_reply_to'] field and determine
        # which of the /n/ pending requests it needs to be given to.
        #
        # You will need to determine whether that should happen here
        # or inside the resource itself.

        metadata = message['transport_metadata']['mtn_rwanda_ussd']

        yield self.send_response(
                message_id=message['message_id'],
                request_id=message['in_reply_to'],
                transaction_id=metadata['transaction_id'],
                transaction_time=metadata['transaction_time'],
                response_code=metadata['response_code'],
                action=metadata['action'],
                msisdn=message['to_addr'],
                ussd_response_string=message['content'].encode(self.ENCODING))




class MTNRwandaXMLRPCResource(xmlrpc.XMLRPC):
    """
    A Resource object implementing XML-RPC, can be published using
    twisted.web.server.Site.
    """

    def __init__(self, transport):
        self.transport = transport
        xmlrpc.XMLRPC.__init__(self)

    def xmlrpc_(self, request, request_id=None):
        request_id = request_id or Transport.generate_message_id()
        request.setHeader("content-type", self.transport.content_type)
        self.transport.set_request(request_id, request)
        self.transport.handle_raw_inbound_message(request_id, request)
        return server.NOT_DONE_YET

    def xmlrpc_healthResource(self, request):
        request.setResponseCode(http.OK)
        request.do_not_log = True
        return self.transport.get_health_response()
