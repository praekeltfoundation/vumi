# -*- test-case-name: vumi.transports.trueafrican.tests.test_transport -*-

"""
USSD Transport for TrueAfrican (Uganda)
"""

import collections

from twisted.internet.defer import Deferred, returnValue, inlineCallbacks
from twisted.internet.task import LoopingCall
from twisted.internet import reactor
from twisted.web import xmlrpc, server

from vumi import log
from vumi.message import TransportUserMessage
from vumi.transports.base import Transport
from vumi.components.session import SessionManager
from vumi.config import ConfigText, ConfigInt, ConfigDict


class TrueAfricanUssdTransportConfig(Transport.CONFIG_CLASS):
    """TrueAfrican USSD transport configuration."""

    port = ConfigInt(
        "Bind to this port",
        required=True, static=True)
    interface = ConfigText(
        "Bind to this interface",
        default='', static=True)
    redis_manager = ConfigDict(
        "Parameters to connect to Redis with",
        default={}, static=True)
    session_timeout = ConfigInt(
        "Number of seconds before USSD session information stored in"
        " Redis expires.",
        default=600, static=True)
    request_timeout = ConfigInt(
        "How long should we wait for the remote side generating the response"
        " for this synchronous operation to come back. Any connection that has"
        " waited longer than `request_timeout` seconds will manually be"
        " closed.",
        default=(4 * 60), static=True)


class TrueAfricanUssdTransport(Transport):

    CONFIG_CLASS = TrueAfricanUssdTransportConfig

    TRANSPORT_TYPE = 'ussd'

    SESSION_STATE_MAP = {
        TransportUserMessage.SESSION_NONE: 'cont',
        TransportUserMessage.SESSION_RESUME: 'cont',
        TransportUserMessage.SESSION_CLOSE: 'end',
    }

    TIMEOUT_TASK_INTERVAL = 10

    @inlineCallbacks
    def setup_transport(self):
        super(TrueAfricanUssdTransport, self).setup_transport()
        config = self.get_static_config()

        # Session handling
        key_prefix = "trueafrican:%s" % self.transport_name
        self.session_manager = yield SessionManager.from_redis_config(
            config.redis_manager,
            key_prefix,
            config.session_timeout
        )

        # XMLRPC Resource
        self.web_resource = reactor.listenTCP(
            config.port,
            server.Site(XmlRpcResource(self)),
            interface=config.interface
        )

        # request tracking
        self.clock = self.get_clock()
        self._requests = {}
        self.request_timeout = config.request_timeout
        self.timeout_task = LoopingCall(self.request_timeout_cb)
        self.timeout_task.clock = self.clock
        self.timeout_task_d = self.timeout_task.start(
            self.TIMEOUT_TASK_INTERVAL,
            now=False
        )
        self.timeout_task_d.addErrback(
            log.err,
            "Request timeout handler failed"
        )

    @inlineCallbacks
    def teardown_transport(self):
        yield self.web_resource.loseConnection()
        if self.timeout_task.running:
            self.timeout_task.stop()
            yield self.timeout_task_d
        yield self.session_manager.stop()
        yield super(TrueAfricanUssdTransport, self).teardown_transport()

    def get_clock(self):
        """
        For easier stubbing in tests
        """
        return reactor

    def request_timeout_cb(self):
        for request_id, request in self._requests.items():
            timestamp = request.timestamp
            if timestamp < self.clock.seconds() - self.request_timeout:
                self.finish_expired_request(request_id, request)

    def track_request(self, request_id, http_request, session):
        d = Deferred()
        self._requests[request_id] = Request(d,
                                             http_request,
                                             session,
                                             self.clock.seconds())
        return d

    def _send_inbound(self, session_id, session, session_event, content):
        transport_metadata = {'session_id': session_id}
        request_id = self.generate_message_id()
        self.publish_message(
            message_id=request_id,
            content=content,
            to_addr=session['to_addr'],
            from_addr=session['from_addr'],
            session_event=session_event,
            transport_name=self.transport_name,
            transport_type=self.TRANSPORT_TYPE,
            transport_metadata=transport_metadata,
        )
        return request_id

    @inlineCallbacks
    def handle_session_new(self, request, session_id, msisdn, to_addr):
        session = yield self.session_manager.create_session(
            session_id,
            from_addr=msisdn,
            to_addr=to_addr
        )
        session_event = TransportUserMessage.SESSION_NEW
        request_id = self._send_inbound(
            session_id, session, session_event, None)
        r = yield self.track_request(request_id, request, session)
        returnValue(r)

    @inlineCallbacks
    def handle_session_resume(self, request, session_id, content):
        # This is an existing session.
        session = yield self.session_manager.load_session(session_id)
        if not session:
            returnValue(self.response_for_error())
        session_event = TransportUserMessage.SESSION_RESUME
        request_id = self._send_inbound(
            session_id, session, session_event, content)
        r = yield self.track_request(request_id, request, session)
        returnValue(r)

    @inlineCallbacks
    def handle_session_end(self, request, session_id):
        session = yield self.session_manager.load_session(session_id)
        if not session:
            returnValue(self.response_for_error())
        session_event = TransportUserMessage.SESSION_CLOSE
        # send a response immediately, and don't (n)ack
        # since this is not application-initiated
        self._send_inbound(session_id, session, session_event, None)
        response = {}
        returnValue(response)

    def handle_outbound_message(self, message):
        in_reply_to = message['in_reply_to']
        session_id = message['transport_metadata'].get('session_id')
        content = message['content']
        if not (in_reply_to and session_id and content):
            return self.publish_nack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                reason="Missing in_reply_to, content or session_id fields"
            )
        response = {
            'session': session_id,
            'type': self.SESSION_STATE_MAP[message['session_event']],
            'message': content
        }
        log.msg("Sending outbound message %s: %s" % (
            message['message_id'], response)
        )
        self.finish_request(in_reply_to,
                            message['message_id'],
                            response)

    def response_for_error(self):
        """
        Generic response for abnormal server side errors.
        """
        response = {
            'message': 'We encountered an error while processing your message',
            'type': 'end'
        }
        return response

    def finish_request(self, request_id, message_id, response):
        request = self._requests.get(request_id)
        if request is None:
            # send a nack back, indicating that the original request had
            # timed out before the outbound message reached us.
            self.publish_nack(
                user_message_id=message_id,
                sent_message_id=message_id,
                reason='Exceeded request timeout'
            )
        else:
            del self._requests[request_id]
            # (n)ack publishing.
            #
            # Add a callback and errback, either of which will be invoked
            # depending on whether the response was written to the client
            # successfully or not
            request.http_request.notifyFinish().addCallbacks(
                lambda _: self._finish_success_cb(message_id),
                lambda f: self._finish_failure_cb(f, message_id)
            )
            request.deferred.callback(response)

    def finish_expired_request(self, request_id, request):
        """
        Called on requests that timed out.
        """
        del self._requests[request_id]
        log.msg('Timing out on response for %s' % request.session['from_addr'])
        request.deferred.callback(self.response_for_error())

    def _finish_success_cb(self, message_id):
        self.publish_ack(message_id, message_id)

    def _finish_failure_cb(self, failure, message_id):
        self.publish_nack(
            user_message_id=message_id,
            sent_message_id=message_id,
            reason=str(failure)
        )


# The transport keeps tracks of requests which are still waiting on a response
# from an application worker. These requests are stored in a dict and are keyed
# by the message_id of the transport message dispatched by the
# transport in response to the request.
#
# For each logical request, we keep track of the following:
#
# deferred:     When the response is available, we fire this deferred
# http_request: The Twisted HTTP request associated with the XML RPC request
# session:      Reference to session data
# timestamp:    The time the request was received. Used for timeouts.

Request = collections.namedtuple('Request', ['deferred',
                                             'http_request',
                                             'session',
                                             'timestamp'])


class XmlRpcResource(xmlrpc.XMLRPC):
    def __init__(self, transport):
        xmlrpc.XMLRPC.__init__(self, allowNone=True, useDateTime=False)
        self.putSubHandler("USSD", USSDXmlRpcResource(transport))


class USSDXmlRpcResource(xmlrpc.XMLRPC):
    def __init__(self, transport):
        xmlrpc.XMLRPC.__init__(self, allowNone=True, useDateTime=False)
        self.transport = transport

    @xmlrpc.withRequest
    def xmlrpc_INIT(self, request, session_data):
        """ handler for USSD.INIT """
        msisdn = session_data['msisdn']
        to_addr = session_data['shortcode']
        session_id = session_data['session']
        return self.transport.handle_session_new(request,
                                                 session_id,
                                                 msisdn,
                                                 to_addr)

    @xmlrpc.withRequest
    def xmlrpc_CONT(self, request, session_data):
        """ handler for USSD.CONT """
        session_id = session_data['session']
        content = session_data['response']
        return self.transport.handle_session_resume(request,
                                                    session_id,
                                                    content)

    @xmlrpc.withRequest
    def xmlrpc_END(self, request, session_data):
        """ handler for USSD.END """
        session_id = session_data['session']
        return self.transport.handle_session_end(request,
                                                 session_id)
