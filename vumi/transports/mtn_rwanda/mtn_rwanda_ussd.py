# -*- test-case-name: vumi.transports.mtn_rwanda.tests.test_mtn_rwanda_ussd -*-
from twisted.internet import reactor
from twisted.web import xmlrpc, server
from twisted.internet.defer import inlineCallbacks, Deferred

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


class RequestTimedOutError(Exception):
    pass


class MTNRwandaUSSDTransport(Transport):

    transport_type = 'ussd'
    xmlrpc_server = None

    CONFIG_CLASS = MTNRwandaUSSDTransportConfig
    ENCODING = 'UTF-8'

    @inlineCallbacks
    def setup_transport(self):
        """
        Transport specific setup - it sets up a connection.
        """
        self._requests = {}
        self._requests_deferreds = {}
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
        self._requests[request_id] = request_args
        return request_args

    def get_request(self, request_id):
        if request_id in self._requests:
            request = self._requests[request_id]
            return request

    def remove_request(self, request_id):
        del self._requests[request_id]

    def timed_out(self, request_id):
        d = self._requests_deferreds[request_id]
        self.remove_request(request_id)
        d.errback(RequestTimedOutError(
            "Request %r timed out." % (request_id,)))

    REQUIRED_INBOUND_MESSAGE_FIELDS = set([
        'TransactionId', 'TransactionTime', 'MSISDN', 'USSDServiceCode',
        'USSDRequestString'])

    def validate_inbound_data(self, msg_params):
        missing_fields = (
                self.REQUIRED_INBOUND_MESSAGE_FIELDS - set(msg_params))
        if missing_fields:
            return False
        else:
            return True

    def handle_raw_inbound_request(self, message_id, values, d):
        """
        Called by the XML-RPC server when it receives a payload that
        needs processing.
        """
        self.timeout_request = self.callLater(self.timeout,
                                              self.timed_out, message_id)
        self._requests[message_id] = values
        self._requests_deferreds[message_id] = d
        if not self.validate_inbound_data(values.keys()):
            metadata = {
                'fault_code': '4001',
                'fault_string': 'Missing parameters'
                }
            self.timeout_request.cancel()
            self._requests_deferreds[message_id].callback(
                    self.get_request(message_id))
            self.remove_request(message_id)
            return self.publish_message(
                message_id=message_id,
                content=values['USSDRequestString'],
                from_addr=values['MSISDN'],
                to_addr=values['USSDServiceCode'],
                transport_type=self.transport_type,
                transport_metadata={'mtn_rwanda_ussd': metadata}
                )

        metadata = {
                'transaction_id': values['TransactionId'],
                'transaction_time': values['TransactionTime'],
                }

        return self.publish_message(
                message_id=message_id,
                content=values['USSDRequestString'],
                from_addr=values['MSISDN'],
                to_addr=values['USSDServiceCode'],
                transport_type=self.transport_type,
                transport_metadata={'mtn_rwanda_ussd': metadata}
                )

    def finish_request(self, request_id, data):
        request = self.get_request(request_id)
        del request['USSDRequestString']
        request['USSDResponseString'] = data
        self.set_request(request_id, request)
        d = self._requests_deferreds[request_id]
        self.remove_request(request_id)
        d.callback(request)

    def handle_outbound_message(self, message):
        """
        Read outbound message and do what needs to be done with them.
        """
        self.timeout_request.cancel()
        request_id = message['in_reply_to']

        if self.get_request(request_id) is None:
            return self.publish_nack(user_message_id=request_id,
                    sent_message_id=request_id, reason='Request not found')

        self.finish_request(request_id,
                message.payload['content'].encode('utf-8'))
        return self.publish_ack(user_message_id=request_id,
                sent_message_id=request_id)


class MTNRwandaXMLRPCResource(xmlrpc.XMLRPC):
    """
    A Resource object implementing XML-RPC, can be published using
    twisted.web.server.Site.
    """

    def __init__(self, transport):
        self.transport = transport
        xmlrpc.XMLRPC.__init__(self, allowNone=True)

    def xmlrpc_handleUSSD(self, request_data):
        request_id = Transport.generate_message_id()
        d = Deferred()
        self.transport.handle_raw_inbound_request(request_id, request_data, d)
        return d
