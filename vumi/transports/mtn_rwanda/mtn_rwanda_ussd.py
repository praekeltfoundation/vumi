# -*- test-case-name: vumi.transports.mtn_rwanda.tests.test_mtn_rwanda_ussd -*-
import xmlrpclib
from twisted.internet import reactor
from twisted.web import xmlrpc, server
from twisted.web.xmlrpc import withRequest
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

    def set_request(self, request_id, request_args):
#        print "Stuff I got is:", stuff, request_id
        self._requests[request_id] = request_args
        return request_args

    def get_request(self, request_id):
        if request_id in self._requests:
            request = self._requests[request_id]
            return request

    def remove_request(self, request_id):
        del self._requests[request_id]

    def get_request_and_remove(self, obj, request_id):
        request = self.get_request(request_id)
        print "I should return: ", request
#        self.remove_request(request_id)
        return request

    REQUIRED_INBOUND_MESSAGE_FIELDS = set([
        'TransactionId', 'TransactionTime', 'MSISDN', 'USSDServiceCode',
        'USSDRequestString'])

    def validate_inbound_data(self, msg_params):
        missing_fields = (self.REQUIRED_INBOUND_MESSAGE_FIELDS - set(msg_params))
        if missing_fields:
            return False
        else:
            return True

#    @inlineCallbacks
    def handle_raw_inbound_request(self, message_id, request_data):
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
#        print "inside handle_raw_inbound_request, _requests = ", self._requests
        self.timeout_request = self.callLater(self.timeout,
                                              self.remove_request, message_id)
        params = request_data[::2]
        body = request_data[1::2]
        for index in range(len(params)):
            values[params[index]] = body[index].decode(self.ENCODING)
        self._requests[message_id] = values

        if not self.validate_inbound_data(params):
            # XXX: Doesn't work yet.
            # TODO: Send a response with a fauld code
           return

        metadata = {
                'transaction_id': values['TransactionId'],
                'transaction_time': values['TransactionTime'],
                'response_flag': values['response'],
                }
        d = self.publish_message(
                message_id=message_id,
                content=values['USSDRequestString'],
                from_addr=values['MSISDN'],
                to_addr=values['USSDServiceCode'],
                transport_type=self.transport_type,
                transport_metadata={'mtn_rwanda_ussd': metadata}
                )
        def set_not_done(obj):
		return server.NOT_DONE_YET
	d.addCallback(set_not_done)
	return d

    def finish_request(self, request_id, data):
        request = self.get_request(request_id)
        del request['USSDRequestString']
        print "Received reply:", data
        request['USSDResponseString'] = data
        self.set_request(request_id, request)

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
        self.finish_request(request_id, message.payload['content'].encode('utf-8'))
        return self.publish_ack(user_message_id=message['message_id'],
                sent_message_id=message['message_id'])


class MTNRwandaXMLRPCResource(xmlrpc.XMLRPC):
    """
    A Resource object implementing XML-RPC, can be published using
    twisted.web.server.Site.
    """

    def __init__(self, transport):
        self.transport = transport
        xmlrpc.XMLRPC.__init__(self, allowNone=True)

    def xmlrpc_handleUSSD(self, *args):
        request_id = Transport.generate_message_id()
#        print "args = ", args
        self.transport.set_request(request_id, args)
#        print "_requests = ", self.transport._requests

#        self.transport.timeout_request = self.transport.callLater(self.transport.timeout,
#                                              self.transport.remove_request, request_id)
 #       print "inside handleUSSD..."
        d = self.transport.handle_raw_inbound_request(request_id, args)
#        d.addCallback(self.transport.set_request, request_id)
        d.addCallback(self.transport.get_request_and_remove, request_id)
        return d
