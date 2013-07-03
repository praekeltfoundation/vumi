# -*- test-case-name: vumi.transports.mtn_rwanda.tests.test_mtn_rwanda_ussd -*-

from twisted.internet import reactor
from twisted.web import xmlrpc, server
from twisted.internet.defer import inlineCallbacks

from vumi.transports.base import Transport
from vumi.config import ConfigServerEndpoint, ConfigInt


class MTNRwandaUSSDTransportConfig(Transport.CONFIG_CLASS):
    """
    MTN Rwanda USSD transport configuration.
    """
    twisted_endpoint = ConfigServerEndpoint(
        "The listening endpoint that the remote client will connect to.",
        required=True, static=True)
    timeout = ConfigInt(
        "No. of seconds to wait before removing a request that hasn't "
        "received a response yet.",
        default=30, static=True)


class MTNRwandaUSSDTransport(Transport):

    transport_type = 'ussd'
    xmlrpc_server = None

    CONFIG_CLASS = MTNRwandaUSSDTransportConfig
    ENCODING = 'UTF-8'

    @inlineCallbacks
    def setup_transport(self):
        """
        Transport specific setup - it initiates things, sets up a
        connection, for example.
        """
        self._requests = {}
        self.callLater = reactor.callLater

        config = self.get_static_config()
        self.endpoint = config.twisted_endpoint
        self.timeout = config.timeout
        self.r = MTNRwandaXMLRPCResource(self)
        self.factory = server.Site(self.r)
        self.xmlrpc_server = yield self.endpoint.listen(self.factory)

    @inlineCallbacks
    def teardown_transport(self):
        """
        Clean-up of setup done in setup_transport.
        """
        if self.xmlrpc_server is not None:
            yield self.xmlrpc_server.stopListening()

    def set_request(self, request_id, request_object):
        self._requests[request_id] = request_object

    def get_request(self, request_id):
        if request_id in self._requests:
            request = self._requests[request_id]
            return request

    def remove_request(self, request_id):
        del self._requests[request_id]

    @inlineCallbacks
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

        values = {}
        print "inside handle_raw_inbound_request"

        for field in request:      # XXX: This doesn't work - need to parse.
            values[field] = request[field].decode(self.ENCODING)

        metadata = {
                'transaction_id': values['TransactionId'],
                'transaction_time': values['TransactionTime'],
                'response_flag': values['response'],
                }

        yield self.publish_message(
                message_id=message_id,
                content=values['USSDRequestString'],
                from_addr=values['MSISDN'],
                to_addr=values['USSDServiceCode'],
                transport_metadata={'mtn_rwanda_ussd': metadata}
                )
        self.timeout_request = self.callLater(self.timeout,
                                              self.remove_request, message_id)
        yield server.NOT_DONE_YET

    def finish_request(self, request_id, data):
        request = self.get_request(request_id)
        request.write(data)
        request.finish()
        self.remove_request(request_id)

    def handle_outbound_message(self, message):
        """
        Read outbound message and do what needs to be done with them.
        """
        # here we look up the message['in_reply_to'] field and determine
        # which of the /n/ pending requests it needs to be given to.
        #
        # You will need to determine whether that should happen here
        # or inside the resource itself.
        self.timeout_request.cancel()
        request_id = message['in_reply_to']
        self.finish_request(request_id,
                data=message['content'].encode(self.ENCODING))
        return self.publish_ack(user_message_id=message['message_id'],
                sent_message_id=message['message_id'])


class MTNRwandaXMLRPCResource(xmlrpc.XMLRPC):
    """
    A Resource object implementing XML-RPC, can be published using
    twisted.web.server.Site.
    """

    def __init__(self, transport):
        self.transport = transport
        xmlrpc.XMLRPC.__init__(self)

    def xmlrpc_handleUSSD(self, request):
        request_id = Transport.generate_message_id()
        self.transport.set_request(request_id, request)
        print "inside handleUSSD...\n request_id :", request_id, "\nrequest string :", request
        return self.transport.handle_raw_inbound_request(request_id, request)
