import struct
from xml.etree import ElementTree as ET

from twisted.internet.protocol import Protocol


class XmlOverTcpClient(Protocol):
    SESSION_ID_HEADER_SIZE = 16
    LENGTH_HEADER_SIZE = 16
    HEADER_SIZE = SESSION_ID_HEADER_SIZE + LENGTH_HEADER_SIZE
    HEADER_FORMAT = '!16s16s'

    PACKET_RECEIVED_HANDLERS = {
        'USSDRequest': 'data_request_packet_received',
    }

    def __init__(self):
        self._buffer = ''
        self._current_header = None
        self._state = 'INITIALISED'

    def connectionMade(self):
        # TODO login (on successful login, start heartbeat)
        self._state = 'CONNECTED'

    def connectionLost(self):
        pass
        # TODO stop heartbeat

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
            # TODO
            return
        getattr(self, handler_name)(session_id, params)

    def data_request_packet_received(self, session_id, params):
        # TODO error handling
        self.data_request_received(session_id, params)

    def data_request_received(self, session_id, params):
        raise NotImplementedError("Subclasses should implement.")

    @classmethod
    def serialize_packet(cls, session_id, packet_type, params):
        # construct body
        root = ET.Element(packet_type)
        for param_name, param_value in params.iteritems():
            ET.SubElement(root, param_name).text = param_value
        body = ET.dump(root)

        # construct header
        length = len(body) + cls.HEADER_SIZE
        header = struct.pack(
            cls.protocol.HEADER_FORMAT,
            session_id,
            str(length).zfill(cls.protocol.LENGTH_HEADER_SIZE))

        return header + body
