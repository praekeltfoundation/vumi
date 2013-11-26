from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import ReconnectingClientFactory

from vumi import log
from vumi.transports.base import Transport
from vumi.message import TransportUserMessage
from vumi.config import ConfigInt, ConfigText, ConfigDict
from vumi.components.session import SessionManager
from vumi.transports.mtn_nigeria.xml_over_tcp import (
    XmlOverTcpError, CodedXmlOverTcpError, XmlOverTcpClient)


class MtnNigeriaUssdTransportConfig(Transport.CONFIG_CLASS):
    """MTN Nigeria USSD transport configuration."""

    server_hostname = ConfigText(
        "Hostname of the server the transport's client should connect to.",
        required=True, static=True)
    server_port = ConfigInt(
        "Port that the server is listening on.",
        required=True, static=True)
    username = ConfigText(
        "The username for this transport.",
        required=True, static=True)
    password = ConfigText(
        "The password for this transport.",
        required=True, static=True)
    application_id = ConfigText(
        "An application ID required by MTN Nigeria for client authentication.",
        required=True, static=True)
    enquire_link_interval = ConfigInt(
        "The interval (in seconds) between enquire links sent to the server "
        "to check whether the connection is alive and well.",
        default=30, static=True)
    timeout_period = ConfigInt(
        "How long (in seconds) after sending an enquire link request the "
        "client should wait for a response before timing out. NOTE: The "
        "timeout period should not be longer than the enquire link interval",
        default=30, static=True)
    user_termination_response = ConfigText(
        "Response given back to the user if the user terminated the session.",
        default='Session Ended', static=True)
    redis_manager = ConfigDict(
        "Parameters to connect to Redis with",
        default={}, static=True)
    session_timeout_period = ConfigInt(
        "Max length (in seconds) of a USSD session",
        default=600, static=True)


class MtnNigeriaUssdTransport(Transport):
    """
    USSD transport for MTN Nigeria.

    This transport connects as a TCP client and sends messages using a
    custom protocol whose packets consist of binary headers plus an XML body.
    """

    transport_type = 'ussd'

    CONFIG_CLASS = MtnNigeriaUssdTransportConfig

    # The encoding we use internally
    ENCODING = 'UTF-8'

    REQUIRED_METADATA_FIELDS = set(['session_id', 'clientId'])

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        self.user_termination_response = config.user_termination_response

        r_prefix = "vumi.transports.mtn_nigeria:%s" % self.transport_name
        self.session_manager = yield SessionManager.from_redis_config(
            config.redis_manager, r_prefix,
            config.session_timeout_period)

        self.factory = MtnNigeriaUssdClientFactory(
            vumi_transport=self,
            username=config.username,
            password=config.password,
            application_id=config.application_id,
            enquire_link_interval=config.enquire_link_interval,
            timeout_period=config.timeout_period)
        self.client_connector = reactor.connectTCP(
            config.server_hostname, config.server_port, self.factory)
        log.msg('Connecting')

    def teardown_transport(self):
        if self.client_connector is not None:
            self.factory.stopTrying()
            self.client_connector.disconnect()

        return self.session_manager.stop()

    @staticmethod
    def pop_fields(params, *fields):
        return (params.pop(k, None) for k in fields)

    @staticmethod
    def determine_session_event(msg_type, end_of_session):
        if msg_type == '1':
            return TransportUserMessage.SESSION_NEW
        if end_of_session == '0' and msg_type == '4':
            return TransportUserMessage.SESSION_RESUME
        return TransportUserMessage.SESSION_CLOSE

    @inlineCallbacks
    def handle_raw_inbound_message(self, session_id, params):
        # ensure the params are in the encoding we use internally
        params['session_id'] = session_id
        params = dict((k, v.decode(self.ENCODING))
                      for k, v in params.iteritems())

        session_event = self.determine_session_event(
            *self.pop_fields(params, 'msgtype', 'EndofSession'))

        # For the first message of a session, the `user_data` field is the ussd
        # code. For subsequent messages, 'user_data' is the user's content.  We
        # need to keep track of the ussd code we get in in the first session
        # message so we can link the correct `to_addr` to subsequent messages
        if session_event == TransportUserMessage.SESSION_NEW:
            # Set the content to none if this the start of the session.
            # Prevents this inbound message being mistaken as a user message.
            content = None

            to_addr = params.pop('userdata')
            session = yield self.session_manager.create_session(
                session_id, ussd_code=to_addr)
        else:
            session = yield self.session_manager.load_session(session_id)
            to_addr = session['ussd_code']
            content = params.pop('userdata')

        # pop the remaining needed field (the rest is left as metadata)
        [from_addr] = self.pop_fields(params, 'msisdn')

        log.msg('MtnNigeriaUssdTransport receiving inbound message from %s '
                'to %s: %s' % (from_addr, to_addr, content))

        if session_event == TransportUserMessage.SESSION_CLOSE:
            self.factory.client.send_data_response(
                session_id=session_id,
                request_id=params['requestId'],
                star_code=params['starCode'],
                client_id=params['clientId'],
                msisdn=from_addr,
                user_data=self.user_termination_response,
                end_session=True)

        yield self.publish_message(
            content=content,
            to_addr=to_addr,
            from_addr=from_addr,
            provider='mtn_nigeria',
            session_event=session_event,
            transport_type=self.transport_type,
            transport_metadata={'mtn_nigeria_ussd': params})

    def send_response(self, message_id, **client_args):
        try:
            self.factory.client.send_data_response(**client_args)
        except XmlOverTcpError as e:
            return self.publish_nack(message_id, "Response failed: %s" % e)

        return self.publish_ack(user_message_id=message_id,
                                sent_message_id=message_id)

    def validate_outbound_message(self, message):
        metadata = message['transport_metadata']['mtn_nigeria_ussd']
        missing_fields = (self.REQUIRED_METADATA_FIELDS - set(metadata.keys()))
        if missing_fields:
            raise CodedXmlOverTcpError(
                '208',
                "Required message transport metadata fields missing in "
                "outbound message: %s" % list(missing_fields))

    @inlineCallbacks
    def handle_outbound_message(self, message):
        metadata = message['transport_metadata']['mtn_nigeria_ussd']

        try:
            self.validate_outbound_message(message)
        except CodedXmlOverTcpError as e:
            log.msg(e)
            yield self.publish_nack(message['message_id'], "%s" % e)
            yield self.factory.client.send_error_response(
                metadata.get('session_id'),
                message.payload.get('in_reply_to'),
                e.code)
            return

        log.msg(
            'MtnNigeriaUssdTransport sending outbound message: %s' % message)

        end_session = (
            message['session_event'] == TransportUserMessage.SESSION_CLOSE)
        yield self.send_response(
            message_id=message['message_id'],
            session_id=metadata['session_id'],
            request_id=metadata['requestId'],
            star_code=metadata['starCode'],
            client_id=metadata['clientId'],
            msisdn=message['to_addr'],
            user_data=message['content'].encode(self.ENCODING),
            end_session=end_session)


class MtnNigeriaUssdClient(XmlOverTcpClient):
    def __init__(self, vumi_transport, **kwargs):
        XmlOverTcpClient.__init__(self, **kwargs)
        self.vumi_transport = vumi_transport

    def connectionMade(self):
        XmlOverTcpClient.connectionMade(self)
        self.factory.resetDelay()

    def data_request_received(self, session_id, params):
        return self.vumi_transport.handle_raw_inbound_message(
            session_id, params)


class MtnNigeriaUssdClientFactory(ReconnectingClientFactory):
    protocol = MtnNigeriaUssdClient

    def __init__(self, **client_args):
        self.client_args = client_args
        self.client = None

    def buildProtocol(self, addr):
        client = self.protocol(**self.client_args)
        client.factory = self
        self.client = client
        return client
