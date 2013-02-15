import struct

from twisted.trial import unittest
from twisted.test import proto_helpers

from vumi.transports.mtn_nigeria.xml_over_tcp import XmlOverTcpClient


class ToyXmlOverTcpClient(XmlOverTcpClient):
    def __init__(self):
        XmlOverTcpClient.__init__(self)
        self.received_data_requests = []

    def data_request_received(self, session_id, params):
        self.received_data_requests.append([session_id, params])


class XmlOverTcpClientTestCase(unittest.TestCase):
    def setUp(self):
        t = proto_helpers.StringTransport()
        self.client = ToyXmlOverTcpClient()
        self.client.makeConnection(t)

    def mk_serialized_packet(self, session_id, body):
        length = len(body) + self.client.HEADER_SIZE
        header = struct.pack(
            self.client.HEADER_FORMAT,
            session_id,
            str(length).zfill(self.client.LENGTH_HEADER_SIZE))
        return header + body

    def test_data_request_received(self):
        session_id = "1234" * 4
        body = (
            "<USSDRequest>"
                "<requestId>13434</requestId>"
            "</USSDRequest>"
        )
        packet = self.mk_serialized_packet(session_id, body)
        for b in packet:
            self.client.dataReceived(b)
        self.assertEqual(
            self.client.received_data_requests,
            [['1234123412341234', {'requestId': '13434'}]])
