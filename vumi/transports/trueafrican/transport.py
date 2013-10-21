# -*- test-case-name: vumi.transports.trueafrican.tests.test_transport -*-

""" USSD Transport for TrueAfrican (Uganda) """

from twisted.internet.defer import Deferred, returnValue, inlineCallbacks
from twisted.internet import reactor
from twisted.web import xmlrpc, server
from twisted.application import internet

from vumi import log
from vumi.message import TransportUserMessage
from vumi.transports.base import Transport
from vumi.components.session import SessionManager
from vumi.config import ConfigText, ConfigInt, ConfigDict


Request = collections.namedtuple('Request', ['deferred', 'start_time'])


class XmlRpcResource(xmlrpc.XMLRPC):

    def __init__(self, transport):
        xmlrpc.XMLRPC.__init__(self, allowNone=False, useDateTime=False)
        self.transport = transport
        self._handlers = {
            "USSD.INIT": self.ussd_session_init,
            "USSD.CONT": self.ussd_session_cont,
            "USSD.END": self.ussd_session_end,
        }

    def lookupProcedure(self, procedurePath):
        try:
            return self._handlers[procedurePath]
        except KeyError:
            raise xmlrpc.NoSuchFunction(
                self.NOT_FOUND,
                "Procedure '%s' not found" % procedurePath
            )

    def listProcedures(self):
        return ['ussd_session_init',
                'ussd_session_cont',
                'ussd_session_end']

    @inlineCallbacks
    def ussd_session_init(self, session_data):
        """ handler for USSD.INIT """
        msisdn = session_data['msisdn']
        to_addr = session_data['shortcode']
        session_id = session_data['session']
        response = yield self.transport.handle_session_new(session_id,
                                                           msisdn,
                                                           to_addr)
        returnValue(response)

    @inlineCallbacks
    def ussd_session_cont(self, session_data):
        """ handler for USSD.CONT """
        session_id = session_data['session']
        content = session_data['response']
        response = self.transport.handle_session_resume(session_id,
                                                        content)
        returnValue(response)

    @inlineCallbacks
    def ussd_session_end(self, session_data):
        """ handler for USSD.END """
        session_id = session_data['session']
        response = yield self.transport.handle_session_end(session_id)
        returnValue(response)


class TrueAfricanUssdTransportConfig(Transport.CONFIG_CLASS):
    """TrueAfrican USSD transport configuration."""

    server_hostname = ConfigText(
        "Bind to this hostname",
        required=True, static=True)
    server_port = ConfigInt(
        "Bind to this port",
        required=True, static=True)
    redis_manager = ConfigDict(
        "Parameters to connect to Redis with",
        default={}, static=True)
    session_timeout_period = ConfigInt(
        "Max length (in seconds) of a USSD session",
        default=600, static=True)


class TrueAfricanUssdTransport(Transport):

    CONFIG_CLASS = TrueAfricanUssdTransportConfig

    TRANSPORT_TYPE = 'ussd'
    SESSION_KEY_PREFIX = "vumi:transport:trueafrican:ussd"

    SESSION_STATE_MAP = {
        TransportUserMessage.SESSION_RESUME: 'cont',
        TransportUserMessage.SESSION_CLOSE: 'end',
    }

    @inlineCallbacks
    def setup_transport(self):
        super(TrueAfricanUssdTransport, self).setup_transport()
        config = self.get_static_config()

        # Session handling
        key_prefix = "%s:%s" % (self.SESSION_KEY_PREFIX, self.transport_name)
        self.session_manager = yield SessionManager.from_redis_config(
            config.redis_manager, key_prefix,
            config.session_timeout_period
        )

        # XMLRPC Resource
        site = server.Site(XmlRpcResource(self))
        service = internet.TCPServer(config.server_port, site,
                                     interface=config.server_hostname)
        service.setServiceParent(self)

        # request tracking
        self._requests = {}
        self.request_timeout = config.request_timeout
        self.request_timeout_task = LoopingCall(self.request_timeout_cb)
        self.request_timeout_task.clock = self.clock
        self.request_timeout_task.start(config.request_timeout)

    @property
    def clock(self):
        return reactor

    def self.request_timeout_cb(self):
        for request_id, request in self._requests.items():
            start_time = request.start_time
            if start_time < self.clock.seconds() - self.request_timeout:
                self.finish_expired_request(request_id)

    def get_transport_url(self, suffix=''):
        """
        Get the URL for the HTTP resource. Requires the worker to be started.

        This is mostly useful in tests, and probably shouldn't be used
        in non-test code, because the API might live behind a load
        balancer or proxy.
        """
        addr = self.web_resource.getHost()
        return "http://%s:%s/%s" % (addr.host, addr.port, suffix.lstrip('/'))

    def track_request(self, request_id):
        d = Deferred()
        self._requests[request_id] = Request(d, self.clock.seconds())
        return d

    @inlineCallbacks
    def handle_session_new(self, session_id, msisdn, to_addr):
        yield self.session_manager.create_session(
            session_id,
            from_addr=msisdn,
            to_addr=to_addr
        )
        session_event = TransportUserMessage.SESSION_NEW
        transport_metadata = {'session_id': session_id}
        request_id = self.generate_message_id()
        self.publish_message(
            message_id=request_id,
            content=None,
            to_addr=to_addr,
            from_addr=msisdn,
            session_event=session_event,
            transport_name=self.transport_name,
            transport_type=self.TRANSPORT_TYPE,
            transport_metadata=transport_metadata,
        )
        returnValue(self.track_request(request_id))

    @inlineCallbacks
    def handle_session_resume(self, session_id, content):
        # This is an existing session.
        session = yield self.session_manager.load_session(session_id)
        if not session:
            returnValue(self.response_for_error())
        session_event = TransportUserMessage.SESSION_RESUME
        transport_metadata = {'session_id': session_id}
        request_id = self.generate_message_id()
        self.publish_message(
            message_id=request_id,
            content=content,
            to_addr=session['to_addr'],
            from_addr=session['msisdn'],
            session_event=session_event,
            transport_name=self.transport_name,
            transport_type=self.TRANSPORT_TYPE,
            transport_metadata=transport_metadata,
        )
        returnValue(self.track_request(request_id))

    @inlineCallbacks
    def handle_session_end(self, session_id):
        session = yield self.session_manager.load_session(session_id)
        if not session:
            returnValue(self.response_for_error())
        session_event = TransportUserMessage.SESSION_CLOSE
        transport_metadata = {'session_id': session_id}
        request_id = self.generate_message_id()
        self.publish_message(
            message_id=request_id,
            content=None,
            to_addr=session['to_addr'],
            from_addr=session['msisdn'],
            session_event=session_event,
            transport_name=self.transport_name,
            transport_type=self.TRANSPORT_TYPE,
            transport_metadata=transport_metadata,
        )
        returnValue(self.track_request(request_id))

    def handle_outbound_message(self, message):
        in_reply_to = message['in_reply_to']
        session_id = message['transport_metadata'].get('session_id')
        content = message['content']
        if not (in_reply_to and session_id and content):
            return self.publish_nack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                reason="Missing 'in_reply_to', 'content' or 'session_id' field"
            )
        response = {
            'status': 'OK',
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
            'status': 'ERROR',
            'message': ('We encountered an error processing '
                        'this request')
            'type': 'end'
        }
        return response

    def finish_request(self, request_id, message_id, response):
        request = self._requests.get(request_id)
        if request is None:
            # send a nack back, indicating that the original request had
            # timed out before the outbound message reached us.
            self.publish_nack(message_id, message_id,
                              "Failed to send outbound message %s: "
                              "Exceeded request timeout")
        else:
            del self._requests[request_id]
            deferred.addCallbacks(
                lambda _: self.on_finish_success_cb(message_id),
                lambda f: self.on_finish_failure_cb(f, message_id)
            )
            request.deferred.callback(response)

    def finish_expired_request(self, request_id, request):
        """
        Called on requests that timed out.
        """
        del self._requests[request_id]
        request.deferred.callback(self.response_for_error())

    def _finish_success_cb(self, message_id):
        return self.publish_ack(message_id, message_id)

    def _finish_failure_cb(self, failure, message_id):
        failure_message = "Failed to send outbound message %s: %s" % (
            message_id,
            repr(failure)
        )
        return self.publish_nack(message_id, message_id, failure_message)
