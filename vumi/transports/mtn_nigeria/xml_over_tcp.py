import uuid
import struct
from xml.etree import ElementTree as ET

from twisted.python import log
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
        'ENQRequest': 'handle_enquire_link_request',
        'ENQResponse': 'handle_enquire_link_response',
    }

    MANDATORY_DATA_REQUEST_FIELDS = set([
        'requestId', 'msisdn', 'clientId', 'starCode', 'msgtype', 'phase',
        'dcs', 'userdata'])
    OTHER_DATA_REQUEST_FIELDS = set(['EndofSession'])
    LOGIN_RESPONSE_FIELDS = set(['requestId', 'authMsg'])
    ENQUIRE_LINK_FIELDS = set(['requestId', 'enqCmd'])

    # Data requests and responses need to include a 'dcs' (data coding scheme)
    # field. '15' is used for ASCII, and is the default. The documentation
    # does not offer any other codes.
    DATA_CODING_SCHEME = '15'

    # Data requests and responses need to include a 'phase' field. The
    # documentation does not provide any information about 'phase', other
    # than mentioning that it is mandatory, and must be set to '2'.
    PHASE = '2'

    def __init__(self, username, password, application_id,
                 heartbeat_interval=240, timeout_period=120):
        self.username = username
        self.password = password
        self.application_id = application_id
        self.heartbeat_interval = heartbeat_interval
        self.timeout_period = timeout_period

        self.authenticated = False
        self.scheduled_timeout = None
        self.heartbeat = LoopingCall(self.send_enquire_link_request)

        self._buffer = ''
        self._current_header = None

    def connectionMade(self):
        self.login()
        self.start_heartbeat()

    def connectionLost(self, reason):
        self.stop_heartbeat()

    def get_clock(self):
        """For easier test stubbing."""
        return reactor

    def timeout(self):
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

        self.scheduled_timeout = self.get_clock().callLater(
            delay, self.timeout)

    def start_heartbeat(self):
        self.reset_scheduled_timeout()
        self.heartbeat.clock = self.get_clock()
        return self.heartbeat.start(self.heartbeat_interval, now=False)

    def stop_heartbeat(self):
        self.cancel_scheduled_timeout()
        if self.heartbeat.running:
            self.heartbeat.stop()

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
            # TODO send error response
            return
        getattr(self, handler_name)(session_id, params)

    @staticmethod
    def validate_packet_fields(packet_params, mandatory_fields,
                               other_fields=set()):
        packet_fields = set(packet_params.keys())

        all_fields = mandatory_fields | other_fields
        unexpected_fields = packet_fields - all_fields
        if unexpected_fields:
            log.msg("Unexpected fields in received packet: %s" %
                    list(unexpected_fields))
            # TODO send error response
            return False

        missing_mandatory_fields = mandatory_fields - packet_fields
        if missing_mandatory_fields:
            log.msg("Missing mandatory fields in received packet: %s" %
                    list(missing_mandatory_fields))
            # TODO send error response
            return False

        return True

    def handle_login_response(self, session_id, params):
        if self.validate_packet_fields(params, self.LOGIN_RESPONSE_FIELDS):
            self.authenticated = True
            # TODO start heartbeat

    def handle_data_request(self, session_id, params):
        if not self.authenticated:
            log.msg("Data request packet received before client "
                    "authentication was completed.")
            # TODO send error response
            return

        if not self.validate_packet_fields(params,
                self.MANDATORY_DATA_REQUEST_FIELDS,
                self.OTHER_DATA_REQUEST_FIELDS):
            return

        # if EndofSession is not in params, assume this is the end of a session
        params.setdefault('EndofSession', '1')

        self.data_request_received(session_id, params)

    def data_request_received(self, session_id, params):
        raise NotImplementedError("Subclasses should implement.")

    @classmethod
    def serialize_packet(cls, session_id, packet_type, params):
        # construct body
        root = ET.Element(packet_type)
        for param_name, param_value in params:
            ET.SubElement(root, param_name).text = param_value
        body = ET.tostring(root)

        # construct header
        length = len(body) + cls.HEADER_SIZE
        header = struct.pack(
            cls.HEADER_FORMAT,
            session_id,
            str(length).zfill(cls.LENGTH_HEADER_SIZE))

        return header + body

    def send_packet(self, session_id, packet_type, params):
        packet = self.serialize_packet(session_id, packet_type, params)
        self.transport.write(packet)

    @staticmethod
    def gen_id():
        """
        Returns a new uuid's 16 byte string. Used to generate request id's and
        dummy session id's needed for sending certain types of packets,
        including login requests and enquire link requests.
        """
        return uuid.uuid4().bytes

    def login(self):
        params = [
            ('requestId', self.gen_id()),
            ('userName', self.username),
            ('passWord', self.password),  # plaintext passwords, yay :/
            ('applicationId', self.application_id),
        ]
        self.send_packet(self.gen_id(), 'AUTHRequest', params)

    def send_data_response(self, session_id, end_session=True, **params):
        if end_session:
            msgtype = '6'
            end_of_session = '1'
        else:
            msgtype = '2'
            end_of_session = '0'

        packet_params = [
            ('requestId', params['requestId']),
            ('msisdn', params['msisdn']),
            ('starCode', params['starCode']),
            ('clientId', params['clientId']),
            ('phase', params.get('phase', self.PHASE)),
            ('msgtype', params.get('msgtype', msgtype)),
            ('dcs', params.get('dcs', self.DATA_CODING_SCHEME)),
            ('userdata', params['userdata']),
            ('EndofSession', params.get('EndofSession', end_of_session)),
        ]
        self.send_packet(session_id, 'USSDResponse', packet_params)

    def handle_enquire_link_request(self, session_id, params):
        if self.validate_packet_fields(params, self.ENQUIRE_LINK_FIELDS):
            self.send_enquire_link_response(session_id, params['requestId'])

    def send_enquire_link_request(self):
        self.send_packet(self.gen_id(), 'ENQRequest', [
            ('requestId', self.gen_id()),
            ('enqCmd', 'ENQUIRELINK')
        ])

    def handle_enquire_link_response(self, session_id, params):
        if self.validate_packet_fields(params, self.ENQUIRE_LINK_FIELDS):
            # log for verbose logging?
            self.reset_scheduled_timeout()

    def send_enquire_link_response(self, session_id, request_id):
        self.send_packet(session_id, 'ENQResponse', [
            ('requestId', request_id),
            ('enqCmd', 'ENQUIRELINKRSP')
        ])
