import uuid
import struct
from xml.etree import ElementTree as ET

from vumi import log
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.internet.protocol import Protocol


class XmlOverTcpClient(Protocol):
    SESSION_ID_HEADER_SIZE = 16
    LENGTH_HEADER_SIZE = 16
    HEADER_SIZE = SESSION_ID_HEADER_SIZE + LENGTH_HEADER_SIZE
    HEADER_FORMAT = '!16s16s'

    PACKET_RECEIVED_HANDLERS = {
        'USSDRequest': 'handle_data_request',
        'AUTHResponse': 'handle_login_response',
        'AUTHError': 'handle_login_error_response',
        'ENQRequest': 'handle_enquire_link_request',
        'ENQResponse': 'handle_enquire_link_response',
        'USSDError': 'handle_error_response',
    }

    # packet types which don't need the client to be authenticated
    IGNORE_AUTH_PACKETS = [
        'AUTHResponse', 'AUTHError', 'AUTHRequest', 'USSDError']

    # received packet fields
    MANDATORY_DATA_REQUEST_FIELDS = set([
        'requestId', 'msisdn', 'clientId', 'starCode', 'msgtype', 'phase',
        'dcs', 'userdata'])
    OTHER_DATA_REQUEST_FIELDS = set(['EndofSession'])
    LOGIN_RESPONSE_FIELDS = set(['requestId', 'authMsg'])
    LOGIN_ERROR_FIELDS = set(['requestId', 'authMsg', 'errorCode'])
    ENQUIRE_LINK_FIELDS = set(['requestId', 'enqCmd'])
    ERROR_FIELDS = set(['requestId', 'errorCode'])

    # Data requests and responses need to include a 'dcs' (data coding scheme)
    # field. '15' is used for ASCII, and is the default. The documentation
    # does not offer any other codes.
    DATA_CODING_SCHEME = '15'

    # Data requests and responses need to include a 'phase' field. The
    # documentation does not provide any information about 'phase', other
    # than mentioning that it is mandatory, and must be set to '2'.
    PHASE = '2'

    ERRORS = {
        '001': 'Invalid User Name Password',
        '002': 'Buffer Overflow',
        '200': 'No free dialogs',
        '201': 'Invalid Destination  (applies for n/w initiated session only)',
        '202': 'Subscriber Not reachable.',
        '203': ('Timer Expiry (session with subscriber terminated due to '
                'TimerExp)'),
        '204': 'Subscriber is Black Listed.',
        '205': ('Service not Configured. (some service is created but but no '
               'menu configured for this)'),
        '206': 'Network Error',
        '207': 'Unknown Error',
        '208': 'Invalid Message',
        '209': 'Subscriber terminated Session (subscriber chose exit option)',
        '210': 'Incomplete Menu',
        '211': 'ER not running',
        '212': 'Timeout waiting for response from ER',
    }

    def __init__(self, username, password, application_id,
                 heartbeat_interval=240, timeout_period=60):
        self.username = username
        self.password = password
        self.application_id = application_id
        self.heartbeat_interval = heartbeat_interval
        self.timeout_period = timeout_period

        self.clock = reactor
        self.authenticated = False
        self.scheduled_timeout = None
        self.heartbeat = LoopingCall(self.send_enquire_link_request)

        self._buffer = ''
        self._current_header = None

    def connectionMade(self):
        self.login()

    def connectionLost(self, reason):
        self.stop_heartbeat()

    def disconnect(self):
        """For easier test stubbing."""
        self.transport.loseConnection()

    def cancel_scheduled_timeout(self):
        scheduled_timeout = self.scheduled_timeout
        if scheduled_timeout is not None and scheduled_timeout.active():
            scheduled_timeout.cancel()

    def reset_scheduled_timeout(self):
        self.cancel_scheduled_timeout()

        # mod the timeout period by the heartbeat interval to prevent skipped
        # heartbeats from going unnoticed.
        delay = self.heartbeat_interval + (
            self.timeout_period % self.heartbeat_interval)

        self.scheduled_timeout = self.clock.callLater(
            delay, self.disconnect)

    def start_heartbeat(self):
        if not self.authenticated:
            log.msg("Heartbeat could not be started, client not "
                    "authenticated")
            return

        self.reset_scheduled_timeout()
        self.heartbeat.clock = self.clock
        d = self.heartbeat.start(self.heartbeat_interval, now=False)
        log.msg("Heartbeat started")

        return d

    def stop_heartbeat(self):
        self.cancel_scheduled_timeout()
        if self.heartbeat.running:
            self.heartbeat.stop()
        log.msg("Heartbeat stopped")

    def dataReceived(self, data):
        self._buffer += data
        if self._current_header is None:
            self.consume_header()
        else:
            self.consume_body()

    def pop_buffer(self, n):
        if n > len(self._buffer):
            return []

        buffer = self._buffer
        self._buffer = buffer[n:]
        return buffer[:n]

    @classmethod
    def deserialize_header(cls, header):
        session_id, length = struct.unpack(cls.HEADER_FORMAT, header)

        # The length field is 16 bytes, but integer types don't reach
        # this length. Therefore, I'm assuming the length field is a 16
        # byte ASCII string that can be converted to an integer type.
        length = int(length) - cls.HEADER_SIZE

        return {
            'session_id': session_id,
            'length': length
        }

    def consume_header(self):
        header = self.pop_buffer(self.HEADER_SIZE)
        if header:
            self._current_header = self.deserialize_header(header)
            self.consume_body()

    @classmethod
    def deserialize_body(cls, body):
        root = ET.fromstring(body)
        return root.tag, dict((el.tag, el.text) for el in root)

    def consume_body(self):
        header = self._current_header
        if header is None:
            return

        body = self.pop_buffer(header['length'])
        if body:
            packet_type, params = self.deserialize_body(body)
            self.packet_received(header['session_id'], packet_type, params)
            self._current_header = None
            self.consume_header()

    def packet_received(self, session_id, packet_type, params):
        # dispatch the packet to the appropriate handler
        handler_name = self.PACKET_RECEIVED_HANDLERS.get(packet_type, None)
        if handler_name is None:
            log.msg("Packet of an unknown type received: %s" % packet_type)
            self.send_error_response(
                session_id, params.get('requestId'), '208')
            return

        if (not self.authenticated and
                packet_type not in self.IGNORE_AUTH_PACKETS):
            log.msg("'%s' packet received before client authentication "
                    "was completed" % packet_type)
            self.send_error_response(
                session_id, params.get('requestId'), '207')
            return

        getattr(self, handler_name)(session_id, params)

    def validate_packet_fields(self, session_id, params, mandatory_fields,
                               other_fields=set()):
        packet_fields = set(params.keys())
        valid = True

        all_fields = mandatory_fields | other_fields
        unexpected_fields = packet_fields - all_fields
        if unexpected_fields:
            log.msg("Unexpected fields in received packet: %s" %
                    list(unexpected_fields))
            valid = False

        missing_mandatory_fields = mandatory_fields - packet_fields
        if missing_mandatory_fields:
            log.msg("Missing mandatory fields in received packet: %s" %
                    list(missing_mandatory_fields))
            valid = False

        if not valid:
            # send an 'Invalid Message' error response
            self.send_error_response(
                session_id, params.get('requestId'), '208')

        return valid

    def handle_login_response(self, session_id, params):
        if self.validate_packet_fields(
                session_id, params, self.LOGIN_RESPONSE_FIELDS):
            log.msg("Client authentication complete.")
            self.authenticated = True
            self.start_heartbeat()
        else:
            self.disconnect()

    def handle_login_error_response(self, session_id, params):
        self.validate_packet_fields(
            session_id, params, self.LOGIN_ERROR_FIELDS)
        self.disconnect()

    def handle_error_response(self, session_id, params):
        if self.validate_packet_fields(
                session_id, params, self.ERROR_FIELDS):
            error_code = params['errorCode']
            error_msg = self.ERRORS.get(error_code)
            if error_msg is not None:
                log.msg("Server sent error message: %s" % error_msg)
            else:
                log.msg("Server sent an error message with an unknown code: %s"
                        % error_code)

    def handle_data_request(self, session_id, params):
        if self.validate_packet_fields(session_id, params,
                self.MANDATORY_DATA_REQUEST_FIELDS,
                self.OTHER_DATA_REQUEST_FIELDS):
            # if EndofSession is not in params, assume the end of session
            params.setdefault('EndofSession', '1')
            self.data_request_received(session_id, params)

    def data_request_received(self, session_id, params):
        raise NotImplementedError("Subclasses should implement.")

    @classmethod
    def serialize_packet(cls, session_id, packet_type, params):
        # construct body
        root = ET.Element(packet_type)
        for param_name, param_value in params:
            ET.SubElement(root, param_name).text = str(param_value).encode()
        body = ET.tostring(root)

        # construct header
        length = len(body) + cls.HEADER_SIZE
        header = struct.pack(
            cls.HEADER_FORMAT,
            session_id.encode(),
            str(length).zfill(cls.LENGTH_HEADER_SIZE))

        return header + body

    def send_packet(self, session_id, packet_type, params):
        if (not self.authenticated and
                packet_type not in self.IGNORE_AUTH_PACKETS):
            log.msg("'%s' packet could not be sent, client not authenticated"
                    % packet_type)
            return False

        packet = self.serialize_packet(session_id, packet_type, params)
        self.transport.write(packet)
        return True

    @classmethod
    def gen_session_id(cls):
        """
        Generates session id. Used for packets needing a dummy session id.
        """
        # XXX: Slicing the generated uuid is probably a bad idea, and will
        # affect collision resistence, but I can't think of a simpler way to
        # generate a unique 16 char alphanumeric.
        return uuid.uuid4().hex[:cls.SESSION_ID_HEADER_SIZE]

    @staticmethod
    def gen_request_id():
        """
        Generates request id. Used for packets needing a dummy request id.
        """
        return uuid.uuid4().hex

    def login(self):
        params = [
            ('requestId', self.gen_request_id()),
            ('userName', self.username),
            ('passWord', self.password),  # plaintext passwords, yay :/
            ('applicationId', self.application_id),
        ]
        self.send_packet(self.gen_session_id(), 'AUTHRequest', params)

    def send_error_response(self, session_id=None, request_id=None,
                            error_code='207'):
        params = [
            ('requestId', request_id or self.gen_request_id()),
            ('errorCode', error_code),
        ]
        return self.send_packet(
            session_id or self.gen_session_id(), 'USSDError', params)

    def send_data_response(self, session_id, request_id, client_id, msisdn,
                           user_data, star_code, end_session=True):
        if end_session:
            msg_type = '6'
            end_of_session = '1'
        else:
            msg_type = '2'
            end_of_session = '0'

        # XXX: delivery reports can be given for the delivery of the last
        # message in a session. However, the documentation does not provide any
        # information on how delivery report packets look, so this is currently
        # disabled ('delvrpt' is set to '0' below).

        packet_params = [
            ('requestId', request_id),
            ('msisdn', msisdn),
            ('starCode', star_code),
            ('clientId', client_id),
            ('phase', self.PHASE),
            ('msgtype', msg_type),
            ('dcs', self.DATA_CODING_SCHEME),
            ('userdata', user_data),
            ('EndofSession', end_of_session),
            ('delvrpt', '0'),
        ]
        return self.send_packet(session_id, 'USSDResponse', packet_params)

    def handle_enquire_link_request(self, session_id, params):
        if self.validate_packet_fields(
                session_id, params, self.ENQUIRE_LINK_FIELDS):
            self.send_enquire_link_response(session_id, params['requestId'])

    def send_enquire_link_request(self):
        self.send_packet(self.gen_session_id(), 'ENQRequest', [
            ('requestId', self.gen_request_id()),
            ('enqCmd', 'ENQUIRELINK')
        ])

    def handle_enquire_link_response(self, session_id, params):
        if self.validate_packet_fields(
                session_id, params, self.ENQUIRE_LINK_FIELDS):
            self.reset_scheduled_timeout()

    def send_enquire_link_response(self, session_id, request_id):
        self.send_packet(session_id, 'ENQResponse', [
            ('requestId', request_id),
            ('enqCmd', 'ENQUIRELINKRSP')
        ])
