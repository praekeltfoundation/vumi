from twisted.internet import reactor
from twisted.internet.protocol import ReconnectingClientFactory

from vumi import log
from vumi.transports.base import Transport
from vumi.message import TransportUserMessage
from vumi.config import ConfigText, ConfigInt
from vumi.transports.mtn_nigeria.xml_over_tcp import XmlOverTcpClient


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
    heartbeat_interval = ConfigInt(
        "The interval (in seconds) between heartbeats sent to the server to "
        "check whether the connection is alive and well.",
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

    REQUIRED_METADATA_FIELDS = set(['session_id', 'clientId'])

    # errors
    RESPONSE_FAILURE_ERROR = "Response to data request failed."
    METADATA_FIELDS_MISSING_ERROR = (
        "Required message transport metadata fields missing in outbound "
        "message: %s")

    def setup_transport(self):
        config = self.get_static_config()
        self.factory = MtnNigeriaUssdClientFactory(
            vumi_transport=self,
            username=config.username,
            password=config.password,
            application_id=config.application_id,
            heartbeat_interval=config.heartbeat_interval,
            timeout_period=config.timeout_period)
        self.client_connector = reactor.connectTCP(
            config.server_hostname, config.server_port, self.factory)
        self.user_termination_response = (
            config.user_termination_response)

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
        params = dict(
            (k, str(v).decode('UTF-8')) for k, v in params.iteritems())

        # pop needed fields (the rest is left as metadata)
        message_id, from_addr, to_addr, content = self.pop_fields(
            params, ['requestId', 'msisdn', 'starCode', 'userdata'])

        log.msg(
            'MtnNigeriaUssdTransport receiving inbound message from %s to %s.'
            % (from_addr, to_addr))

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

        params['session_id'] = session_id.decode('UTF-8')
        return self.publish_message(
            message_id=message_id,
            content=content,
            to_addr=to_addr,
            from_addr=from_addr,
            provider='mtn_nigeria',
            session_event=session_event,
            transport_type=self.transport_type,
            transport_metadata={'mtn_nigeria_ussd': params})

    def validate_outbound_message(self, message):
        metadata = message['transport_metadata']['mtn_nigeria_ussd']
        missing_fields = (
            self.REQUIRED_METADATA_FIELDS - set(metadata.keys()))
        if missing_fields:
            reason = self.METADATA_FIELDS_MISSING_ERROR % list(missing_fields)
            log.msg(reason)
            self.publish_nack(message['message_id'], reason)

            # send an 'Invalid Message' error response
            self.factory.client.send_error_response(
                message['transport_metadata'].get('session_id'),
                message.payload.get('in_reply_to'),
                '208')

            return False

        return True

    def send_response(self, message_id, **client_args):
        if self.factory.client.send_data_response(**client_args):
            return self.publish_ack(user_message_id=message_id,
                                    sent_message_id=message_id)

        log.msg(self.RESPONSE_FAILURE_ERROR)
        return self.publish_nack(message_id, self.RESPONSE_FAILURE_ERROR)

    def handle_outbound_message(self, message):
        if self.validate_outbound_message(message):
            metadata = message['transport_metadata']['mtn_nigeria_ussd']
            session_close_event = TransportUserMessage.SESSION_CLOSE
            return self.send_response(
                message_id=message['message_id'],
                session_id=metadata['session_id'],
                request_id=message['in_reply_to'],
                star_code=message['from_addr'],
                client_id=metadata['clientId'],
                msisdn=message['to_addr'],
                user_data=message['content'].encode('UTF-8'),
                end_session=message['session_event'] == session_close_event)


class MtnNigeriaUssdClient(XmlOverTcpClient):
    def __init__(self, vumi_transport, **kwargs):
        XmlOverTcpClient.__init__(self, **kwargs)
        self.vumi_transport = vumi_transport

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
        self.client = client
        return client
