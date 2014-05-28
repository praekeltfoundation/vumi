# -*- test-case-name: vumi.transports.mtn_rwanda.tests.test_mtn_rwanda_ussd -*-
from datetime import datetime
from twisted.internet import reactor
from twisted.web import xmlrpc
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue

from vumi.message import TransportUserMessage
from vumi.transports.base import Transport
from vumi.config import (
    ConfigServerEndpoint, ConfigInt, ConfigDict, ConfigText,
    ServerEndpointFallback)
from vumi.components.session import SessionManager
from vumi.transports.httprpc.httprpc import HttpRpcHealthResource
from vumi.utils import build_web_site


class MTNRwandaUSSDTransportConfig(Transport.CONFIG_CLASS):
    """
    MTN Rwanda USSD transport configuration.
    """
    twisted_endpoint = ConfigServerEndpoint(
        "The listening endpoint that the remote client will connect to.",
        required=True, static=True,
        fallbacks=[ServerEndpointFallback()])
    timeout = ConfigInt(
        "No. of seconds to wait before removing a request that hasn't "
        "received a response yet.",
        default=30, static=True)
    redis_manager = ConfigDict(
        "Parameters to connect to redis with",
        default={}, static=True)
    session_timeout_period = ConfigInt(
        "Maximum length of a USSD session",
        default=600, static=True)
    web_path = ConfigText(
        "The path to serve this resource on.", required=True, static=True)
    health_path = ConfigText(
        "The path to serve the health resource on.", default='/health/',
        static=True)

    # TODO: Deprecate these fields when confmodel#5 is done.
    host = ConfigText(
        "*DEPRECATED* 'host' and 'port' fields may be used in place of the"
        " 'twisted_endpoint' field.", static=True)
    port = ConfigInt(
        "*DEPRECATED* 'host' and 'port' fields may be used in place of the"
        " 'twisted_endpoint' field.", static=True)


class RequestTimedOutError(Exception):
    pass


class InvalidRequest(Exception):
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

        r_prefix = "vumi.transports.mtn_rwanda:%s" % self.transport_name
        self.session_manager = yield SessionManager.from_redis_config(
            config.redis_manager, r_prefix,
            config.session_timeout_period)

        self.factory = build_web_site({
            config.health_path: HttpRpcHealthResource(self),
            config.web_path: MTNRwandaXMLRPCResource(self),
        })

        self.xmlrpc_server = yield self.endpoint.listen(self.factory)

    @inlineCallbacks
    def teardown_transport(self):
        """
        Clean-up of setup done in setup_transport.
        """
        self.session_manager.stop()
        if self.xmlrpc_server is not None:
            yield self.xmlrpc_server.stopListening()

    def get_health_response(self):
        return "OK"

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

    @inlineCallbacks
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
            self.timeout_request.cancel()
            self.remove_request(message_id)
            d.errback(InvalidRequest("4001: Missing Parameters"))

        else:
            session_id = values['TransactionId']
            session = yield self.session_manager.load_session(session_id)
            if session:
                session_event = TransportUserMessage.SESSION_RESUME
                content = values['USSDRequestString']
            else:
                yield self.session_manager.create_session(
                    session_id, from_addr=values['MSISDN'],
                    to_addr=values['USSDServiceCode'])
                session_event = TransportUserMessage.SESSION_NEW
                content = None

            metadata = {
                    'transaction_id': values['TransactionId'],
                    'transaction_time': values['TransactionTime'],
                    }

            res = yield self.publish_message(
                message_id=message_id,
                content=content,
                from_addr=values['MSISDN'],
                to_addr=values['USSDServiceCode'],
                session_event=session_event,
                transport_type=self.transport_type,
                transport_metadata={'mtn_rwanda_ussd': metadata}
                )

            returnValue(res)

    @inlineCallbacks
    def finish_request(self, request_id, data, session_event):
        request = self.get_request(request_id)
        del request['USSDRequestString']
        request['USSDResponseString'] = data
        request['TransactionTime'] = datetime.now().isoformat()
        if session_event == TransportUserMessage.SESSION_NEW:
            request['action'] = 'request'
        elif session_event == TransportUserMessage.SESSION_CLOSE:
            request['action'] = 'end'
            yield self.session_manager.clear_session(request['TransactionId'])
        elif session_event == TransportUserMessage.SESSION_RESUME:
            request['action'] = 'notify'
        self.set_request(request_id, request)
        d = self._requests_deferreds[request_id]
        self.remove_request(request_id)
        d.callback(request)

    def handle_outbound_message(self, message):
        """
        Read outbound message and do what needs to be done with them.
        """
        request_id = message['in_reply_to']

        if self.get_request(request_id) is None:
            return self.publish_nack(user_message_id=message['message_id'],
                    sent_message_id=message['message_id'],
                    reason='Request not found')

        self.timeout_request.cancel()
        self.finish_request(request_id,
                message.payload['content'].encode('utf-8'),
                message['session_event'])
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
