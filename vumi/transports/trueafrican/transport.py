# -*- test-case-name: vumi.transports.trueafrican.tests.test_transport -*-

""" USSD Transport for TrueAfrican (Uganda) """

from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.web import xmlrpc, server
from twisted.application import internet

from vumi import log
from vumi.message import TransportUserMessage
from vumi.transports.base import Transport
from vumi.components.session import SessionManager
from vumi.config import ConfigText, ConfigInt, ConfigDict


class XmlRpcResource(xmlrpc.XMLRPC):

    def __init__(self, transport):
        xmlrpc.XMLRPC.__init__(self, allowNone=False, useDateTime=False)
        self.transport = transport
        self._handlers = {
            "USSD.INIT": self.ussd_session_init,
            "USSD.CONT": self.ussd_session_cont,
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
        return ['ussd_session_init', 'ussd_session_cont']

    def ussd_session_init(self, session_data):
        """ handler for USSD.INIT """
        msisdn = session_data['msisdn']
        to_addr = session_data['shortcode']
        session_id = session_data['session']
        return self.transport.handle_session_new(session_id,
                                                 msisdn,
                                                 to_addr)

    def ussd_session_cont(self, data):
        """ handler for USSD.CONT """
        return server.NOT_DONE_YET


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
    ENCODING = 'UTF-8'
    SESSION_KEY_PREFIX = "vumi:transport:trueafrican:ussd"

    @inlineCallbacks
    def setup_transport(self):
        super(TrueAfricanUssdTransport, self).setup_transport()
        config = self.get_static_config()
        key_prefix = "%s:%s" % (self.SESSION_KEY_PREFIX, self.transport_name)
        self.session_manager = yield SessionManager.from_redis_config(
            config.redis_manager, key_prefix,
            config.session_timeout_period
        )

        site = server.Site(XmlRpcResource(self))
        service = internet.TCPServer(config.server_port, site,
                                     interface=config.server_hostname)
        service.setServiceParent(self)
        self._requests = {}

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
        self._requests[request_id] = d
        return d

    def untrack_request(self, request_id):
        del self._requests[request_id]

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
        return self.track_request(request_id)

    @inlineCallbacks
    def handle_session_resume(self, session_id, msisdn, to_addr):
        pass

    def handle_outbound_message(self, message):
        # pilfered from mtech_ussd transport
        in_reply_to = message['in_reply_to']
        session_id = message['transport_metadata'].get('session_id')
        content = message['content']
        if not (in_reply_to and session_id and content):
            return self.publish_nack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                reason="Missing 'in_reply_to', 'content' or 'session_id' field"
            )

        if message['session_event'] == TransportUserMessage.SESSION_CLOSE:
            response_type = 'end'
        else:
            response_type = 'cont'

        response = {
            'session': session_id,
            'type': response_type,
            'message': content
        }

        log.msg("Outbound message: %s" % response)
        self.finish_request(in_reply_to, response)
        return self.publish_ack(user_message_id=message['message_id'],
                                sent_message_id=message['message_id'])

    def finish_request(self, request_id, response):
        deferred = self._requests[request_id]
        deferred.callback(response)
        del self._requests[request_id]
