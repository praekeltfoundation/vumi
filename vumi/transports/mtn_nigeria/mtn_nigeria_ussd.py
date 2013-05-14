from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import ReconnectingClientFactory

from vumi import log
from vumi.transports.base import Transport
from vumi.message import TransportUserMessage
from vumi.config import ConfigText, ConfigInt
from vumi.transports.mtn_nigeria.xml_over_tcp import (
    XmlOverTcpError, CodedXmlOverTcpError, XmlOverTcpClient)


class MtnNigeriaUssdTransportConfig(Transport.CONFIG_CLASS):
    "MTN Nigeria USSD transport configuration."

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
        default=240, static=True)
    timeout_period = ConfigInt(
        "How long (in seconds) after a heartbeat the client should wait "
        "before timing out, should the server not respond to the "
        "heartbeat (enquire link) request.",
        default=120, static=True)
    user_termination_response = ConfigText(
        "Response given back to the user if the user terminated the session.",
        default='Session Ended', static=True)


class MtnNigeriaUssdTransport(Transport):
    transport_type = 'ussd'

    CONFIG_CLASS = MtnNigeriaUssdTransportConfig

    # The encoding we use internally
    ENCODING = 'UTF-8'

    REQUIRED_METADATA_FIELDS = set(['session_id', 'clientId'])

    def setup_transport(self):
        config = self.get_static_config()
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
        self.user_termination_response = config.user_termination_response

    def teardown_transport(self):
        if self.client_connector is not None:
            self.factory.stopTrying()
            self.client_connector.disconnect()

    @staticmethod
    def pop_fields(params, fields):
        return (params.pop(k, None) for k in fields)

    @staticmethod
    def determine_session_event(msg_type, end_of_session):
        if msg_type == '1':
            return TransportUserMessage.SESSION_NEW
        if end_of_session == '0' and msg_type == '4':
            return TransportUserMessage.SESSION_RESUME
        return TransportUserMessage.SESSION_CLOSE

    def handle_raw_inbound_message(self, session_id, params):
        # ensure the params are in the encoding we use internally
        params['session_id'] = session_id
        params = dict(
            (k, str(v).decode(self.ENCODING)) for k, v in params.iteritems())

        # pop needed fields (the rest is left as metadata)
        message_id, from_addr, to_addr, content = self.pop_fields(
            params, ['requestId', 'msisdn', 'starCode', 'userdata'])

        log.msg('MtnNigeriaUssdTransport receiving inbound message from %s '
                'to %s: %s' % (from_addr, to_addr, content))

        session_event = self.determine_session_event(
            *self.pop_fields(params, ['msgtype', 'EndofSession']))
        if session_event == TransportUserMessage.SESSION_CLOSE:
            self.factory.client.send_data_response(
                session_id=session_id,
                request_id=message_id,
                star_code=to_addr,
                client_id=params['clientId'],
                msisdn=from_addr,
                user_data=self.user_termination_response,
                end_session=True)
        return self.publish_message(
            message_id=message_id,
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
            raise CodedXmlOverTcpError('208',
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
            request_id=message['in_reply_to'],
            star_code=message['from_addr'],
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
