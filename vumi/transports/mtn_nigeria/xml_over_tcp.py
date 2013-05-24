import re
import uuid
import struct
from xml.etree import ElementTree as ET

try:
    from xml.etree.ElementTree import ParseError
except ImportError:
    from xml.parsers.expat import ExpatError as ParseError

from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.internet.protocol import Protocol

from vumi import log


class XmlOverTcpError(Exception):
    """
    Raised when an error occurs while interacting with the XmlOverTcp protocol.
    """


class CodedXmlOverTcpError(XmlOverTcpError):
    """
    Raised when an XmlOverTcpError occurs and an error code is available
    """

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

    def __init__(self, code, reason=None):
        self.code = code
        self.message = self.ERRORS.get(code, 'Unknown Code')
        self.reason = reason

    def __str__(self):
        return '(%s) %s%s' % (
            self.code,
            self.message,
            ': %s' % self.reason if self.reason else '')


class XmlOverTcpClient(Protocol):
    SESSION_ID_HEADER_SIZE = 16
    LENGTH_HEADER_SIZE = 16
    HEADER_SIZE = SESSION_ID_HEADER_SIZE + LENGTH_HEADER_SIZE
    HEADER_FORMAT = '!%ss%ss' % (SESSION_ID_HEADER_SIZE, LENGTH_HEADER_SIZE)

    # Used to strip out whitebytes in the length header
    LENGTH_STRIP_RE = re.compile(r'[^0-9]+$')

    # Used to strip out the wierd bytes in the requestId field that make the
    # xml parsing break
    REQUEST_ID_SIZE = 16
    REQUEST_ID_STRIP_RE = re.compile(
        r'(<requestId>\s*.{%s})(.*)'
        r'(\s*</requestId>)' % REQUEST_ID_SIZE)
    REQUEST_ID_STRIP_REPL = r'\g<1>\g<3>'

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
    DATA_REQUEST_FIELDS = set([
        'requestId', 'msisdn', 'clientId', 'starCode', 'msgtype', 'phase',
        'dcs', 'userdata'])
    OTHER_DATA_REQUEST_FIELDS = set(['EndofSession'])
    LOGIN_RESPONSE_FIELDS = set(['requestId', 'authMsg'])
    LOGIN_ERROR_FIELDS = set(['requestId', 'authMsg', 'errorCode'])
    OTHER_LOGIN_ERROR_FIELDS = set(['errorMsg'])
    ENQUIRE_LINK_FIELDS = set(['requestId', 'enqCmd'])
    ERROR_FIELDS = set(['requestId', 'errorCode'])
    OTHER_ERROR_FIELDS = set(['errorMsg'])

    # Data requests and responses need to include a 'dcs' (data coding scheme)
    # field. '15' is used for ASCII, and is the default. The documentation
    # does not offer any other codes.
    DATA_CODING_SCHEME = '15'
    ENCODING = 'ASCII'

    # Data requests and responses need to include a 'phase' field. The
    # documentation does not provide any information about 'phase', but we are
    # assuming this refers to the USSD phase. This should be set to 2 for
    # interactive two-way communication.
    PHASE = '2'

    def __init__(self, username, password, application_id,
                 enquire_link_interval=30, timeout_period=30):
        self.username = username
        self.password = password
        self.application_id = application_id
        self.enquire_link_interval = enquire_link_interval
        self.timeout_period = timeout_period

        self.clock = reactor
        self.authenticated = False
        self.scheduled_timeout = None
        self.periodic_enquire_link = LoopingCall(
            self.send_enquire_link_request)

        self.reset_buffer()

    def connectionMade(self):
        self.login()

    def connectionLost(self, reason):
        log.err("Connection lost")
        self.stop_periodic_enquire_link()
        self.cancel_scheduled_timeout()
        self.reset_buffer()

    def reset_buffer(self):
        self._buffer = ''
        self._current_header = None

    def timeout(self):
        log.err("No enquire link response received after %s seconds, "
                "disconnecting" % self.timeout_period)
        self.disconnect()

    def disconnect(self):
        """For easier test stubbing."""
        self.transport.loseConnection()

    def cancel_scheduled_timeout(self):
        if (self.scheduled_timeout is not None
                and self.scheduled_timeout.active()):
            self.scheduled_timeout.cancel()

    def reset_scheduled_timeout(self):
        self.cancel_scheduled_timeout()

        # cap the timeout period at the enquire link interval to prevent
        # skipped enquire link responses from going unnoticed.
        delay = min(self.timeout_period, self.enquire_link_interval)
        self.scheduled_timeout = self.clock.callLater(delay, self.timeout)

    def start_periodic_enquire_link(self):
        if not self.authenticated:
            log.err("Heartbeat could not be started, client not authenticated")
            return

        self.reset_scheduled_timeout()
        self.periodic_enquire_link.clock = self.clock
        d = self.periodic_enquire_link.start(
            self.enquire_link_interval, now=True)
        log.msg("Heartbeat started")

        return d

    def stop_periodic_enquire_link(self):
        self.cancel_scheduled_timeout()
        if self.periodic_enquire_link.running:
            self.periodic_enquire_link.stop()
        log.msg("Heartbeat stopped")

    def dataReceived(self, data):
        self._buffer += data

        while self._buffer:
            header = self.peak_buffer(self.HEADER_SIZE)

            if not header:
                return

            session_id, length = self.deserialize_header(header)
            packet = self.pop_buffer(length)

            if not packet:
                return

            body = packet[self.HEADER_SIZE:]

            try:
                packet_type, params = self.deserialize_body(body)
            except ParseError as e:
                log.err("Error parsing packet (%s): %s" % (e, packet))
                self.disconnect()
                return

            self.packet_received(session_id, packet_type, params)

    def pop_buffer(self, n):
        if n > len(self._buffer):
            return None

        buffer = self._buffer
        self._buffer = buffer[n:]
        return buffer[:n]

    def peak_buffer(self, n):
        if n > len(self._buffer):
            return None

        return self._buffer[:n]

    @classmethod
    def deserialize_header(cls, header):
        session_id, length = struct.unpack(cls.HEADER_FORMAT, header)

        # The length field is 16 bytes, but integer types don't reach
        # this length. Therefore, I'm assuming the length field is a 16
        # byte ASCII string that can be converted to an integer type.
        length = int(cls.LENGTH_STRIP_RE.sub('', length))

        return session_id, length

    @classmethod
    def deserialize_body(cls, body):
        # remove the wierd bytes in requestId making the xml parser break
        body = cls.REQUEST_ID_STRIP_RE.sub(cls.REQUEST_ID_STRIP_REPL, body)
        root = ET.fromstring(body)

        packet_type = root.tag
        params = dict((el.tag.strip(), el.text.strip()) for el in root)
        return packet_type, params

    def packet_received(self, session_id, packet_type, params):
        log.debug("Packet of type '%s' with session id '%s' received: %s"
                  % (packet_type, session_id, params))

        # dispatch the packet to the appropriate handler
        handler_name = self.PACKET_RECEIVED_HANDLERS.get(packet_type, None)
        if handler_name is None:
            log.err("Packet of an unknown type received: %s" % packet_type)
            return self.send_error_response(
                session_id, params.get('requestId'), '208')

        if (not self.authenticated and
                packet_type not in self.IGNORE_AUTH_PACKETS):
            log.err("'%s' packet received before client authentication "
                    "was completed" % packet_type)
            return self.send_error_response(
                session_id, params.get('requestId'), '207')

        getattr(self, handler_name)(session_id, params)

    def validate_packet_fields(self, params, mandatory_fields,
                               other_fields=set()):
        packet_fields = set(params.keys())

        all_fields = mandatory_fields | other_fields
        unexpected_fields = packet_fields - all_fields
        if unexpected_fields:
            raise CodedXmlOverTcpError(
                '208',
                "Unexpected fields in received packet: %s"
                % list(unexpected_fields))

        missing_mandatory_fields = mandatory_fields - packet_fields
        if missing_mandatory_fields:
            raise CodedXmlOverTcpError(
                '208',
                "Missing mandatory fields in received packet: %s"
                % list(missing_mandatory_fields))

    def handle_error(self, session_id, request_id, e):
        log.err(e)
        self.send_error_response(session_id, request_id, e.code)

    def handle_login_response(self, session_id, params):
        try:
            self.validate_packet_fields(params, self.LOGIN_RESPONSE_FIELDS)
        except CodedXmlOverTcpError as e:
            self.disconnect()
            self.handle_error(session_id, params.get('requestId'), e)
            return

        log.msg("Client authentication complete.")
        self.authenticated = True
        self.start_periodic_enquire_link()

    def handle_login_error_response(self, session_id, params):
        try:
            self.validate_packet_fields(
                params, self.LOGIN_ERROR_FIELDS, self.OTHER_LOGIN_ERROR_FIELDS)
        except CodedXmlOverTcpError as e:
            self.handle_error(session_id, params.get('requestId'), e)
            return

        log.err("Login failed, disconnecting")
        self.disconnect()

    def handle_error_response(self, session_id, params):
        try:
            self.validate_packet_fields(
                params, self.ERROR_FIELDS, self.OTHER_ERROR_FIELDS)
        except CodedXmlOverTcpError as e:
            self.handle_error(session_id, params.get('requestId'), e)
            return

        log.err(
            "Server sent error message: %s" %
            CodedXmlOverTcpError(params['errorCode'], params.get('errorMsg')))

    def handle_data_request(self, session_id, params):

        try:
            self.validate_packet_fields(
                params,
                self.DATA_REQUEST_FIELDS,
                self.OTHER_DATA_REQUEST_FIELDS)
        except CodedXmlOverTcpError as e:
            self.handle_error(session_id, params.get('requestId'), e)
            return

        # if EndofSession is not in params, assume the end of session
        params.setdefault('EndofSession', '1')
        self.data_request_received(session_id, params)

    def data_request_received(self, session_id, params):
        raise NotImplementedError("Subclasses should implement.")

    @classmethod
    def serialize_header(cls, session_id, body):
        length = len(body) + cls.HEADER_SIZE
        return struct.pack(
            cls.HEADER_FORMAT,
            session_id.encode(cls.ENCODING),
            str(length).zfill(cls.LENGTH_HEADER_SIZE))

    @classmethod
    def serialize_body(cls, packet_type, params):
        root = ET.Element(packet_type)
        for param_name, param_value in params:
            param_value = str(param_value).encode(cls.ENCODING)
            ET.SubElement(root, param_name).text = param_value
        return ET.tostring(root)

    @classmethod
    def serialize_packet(cls, session_id, packet_type, params):
        body = cls.serialize_body(packet_type, params)
        return cls.serialize_header(session_id, body) + body

    def send_packet(self, session_id, packet_type, params):
        if (not self.authenticated
                and packet_type not in self.IGNORE_AUTH_PACKETS):
            raise XmlOverTcpError(
                "'%s' packet could not be sent, client not authenticated"
                % packet_type)

        packet = self.serialize_packet(session_id, packet_type, params)
        log.debug("Sending packet: %s" % packet)
        self.transport.write(packet)

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
        log.msg('Logging in')

    def send_error_response(self, session_id=None, request_id=None,
                            code='207'):
        params = [
            ('requestId', request_id or self.gen_request_id()),
            ('errorCode', code),
        ]
        self.send_packet(
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

        self.send_packet(session_id, 'USSDResponse', packet_params)

    def handle_enquire_link_request(self, session_id, params):
        try:
            self.validate_packet_fields(params, self.ENQUIRE_LINK_FIELDS)
        except CodedXmlOverTcpError as e:
            self.handle_error(session_id, params.get('requestId'), e)
            return

        log.debug("Enquire link request received, sending response")
        self.send_enquire_link_response(session_id, params['requestId'])

    def send_enquire_link_request(self):
        log.debug("Sending enquire link request")
        self.send_packet(self.gen_session_id(), 'ENQRequest', [
            ('requestId', self.gen_request_id()),
            ('enqCmd', 'ENQUIRELINK')
        ])

    def handle_enquire_link_response(self, session_id, params):
        try:
            self.validate_packet_fields(params, self.ENQUIRE_LINK_FIELDS)
        except CodedXmlOverTcpError as e:
            self.handle_error(session_id, params.get('requestId'), e)
            return

        log.debug("Enquire link response received, sending next request in %s "
                  "seconds" % self.enquire_link_interval)
        self.reset_scheduled_timeout()

    def send_enquire_link_response(self, session_id, request_id):
        self.send_packet(session_id, 'ENQResponse', [
            ('requestId', request_id),
            ('enqCmd', 'ENQUIRELINKRSP')
        ])
