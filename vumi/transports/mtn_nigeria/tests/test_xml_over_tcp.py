import struct

from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.protocol import Protocol, Factory, ClientFactory

from vumi.transports.mtn_nigeria.xml_over_tcp import XmlOverTcpClient


class WaitForDataMixin(object):
    waiting_for_data = False
    deferred_data = Deferred()

    def wait_for_data(self):
        self.waiting_for_data = True
        self.deferred_data = Deferred()
        return self.deferred_data

    def callback_deferred_data(self, data):
        if self.waiting_for_data and not self.deferred_data.called:
            self.deferred_data.callback(data)
            self.waiting_for_data = False


class MockXmlOverTcpServer(Protocol, WaitForDataMixin):
    def __init__(self):
        self.responses = {}

    def send_data(self, data):
        self.transport.write(data)

    def connectionMade(self):
        self.factory.deferred_server.callback(self)

    def dataReceived(self, data):
        response = self.responses.get(data)
        if response is not None:
            self.transport.write(response)
        self.callback_deferred_data(data)


class ToyXmlOverTcpClient(XmlOverTcpClient, WaitForDataMixin):
    _PACKET_RECEIVED_HANDLERS = {
        'DummyPacket': 'dummy_packet_received'
    }

    def __init__(self):
        XmlOverTcpClient.__init__(self, 'root', 'toor', '1029384756')
        self.PACKET_RECEIVED_HANDLERS.update(self._PACKET_RECEIVED_HANDLERS)
        self.received_dummy_packets = []
        self.received_data_request_packets = []
        self.next_id = 0

    def connectionMade(self):
        self.factory.deferred_client.callback(self)

    def dataReceived(self, data):
        XmlOverTcpClient.dataReceived(self, data)
        self.callback_deferred_data(data)

    def dummy_packet_received(self, session_id, params):
        self.received_dummy_packets.append((session_id, params))

    def data_request_received(self, session_id, params):
        self.received_data_request_packets.append((session_id, params))

    def gen_id(self):
        id = str(self.next_id).zfill(16)
        self.next_id += 1
        return id


class ClientServerMixin(object):
    @inlineCallbacks
    def start_protocols(self):
        server_factory = Factory()
        server_factory.protocol = MockXmlOverTcpServer
        server_factory.deferred_server = Deferred()
        self.server_port = reactor.listenTCP(0, server_factory)

        client_factory = ClientFactory()
        client_factory.protocol = ToyXmlOverTcpClient
        client_factory.deferred_client = Deferred()
        self.client_connector = reactor.connectTCP(
            '127.0.0.1', self.server_port.getHost().port, client_factory)

        self.client = yield client_factory.deferred_client
        self.server = yield server_factory.deferred_server

    def stop_protocols(self):
        self.client_connector.disconnect()
        self.server_port.loseConnection()


class XmlOverTcpClientTestCase(unittest.TestCase, ClientServerMixin):
    @inlineCallbacks
    def setUp(self):
        yield self.start_protocols()

    def tearDown(self):
        self.stop_protocols()

    def mk_serialized_packet(self, session_id, body):
        length = len(body) + self.client.HEADER_SIZE
        header = struct.pack(
            self.client.HEADER_FORMAT,
            session_id,
            str(length).zfill(self.client.LENGTH_HEADER_SIZE))
        return header + body

    @inlineCallbacks
    def test_contiguous_packets_received(self):
        session_id_a = "1" * 16
        body_a = "<DummyPacket><someParam>123</someParam></DummyPacket>"
        session_id_b = "2" * 16
        body_b = "<DummyPacket><someParam>456</someParam></DummyPacket>"
        data = self.mk_serialized_packet(session_id_a, body_a)
        data += self.mk_serialized_packet(session_id_b, body_b)
        self.server.send_data(data)

        yield self.client.wait_for_data()
        self.assertEqual(
            self.client.received_dummy_packets, [
                (session_id_a, {'someParam': '123'}),
                (session_id_b, {'someParam': '456'}),
            ])

    @inlineCallbacks
    def test_partial_data_received(self):
        session_id_a = "1" * 16
        body_a = "<DummyPacket><someParam>123</someParam></DummyPacket>"
        session_id_b = "2" * 16
        body_b = "<DummyPacket><someParam>456</someParam></DummyPacket>"

        # add a full first packet, then concatenate a sliced version of a
        # second packet
        data = self.mk_serialized_packet(session_id_a, body_a)
        data += self.mk_serialized_packet(session_id_b, body_b)[:12]
        self.server.send_data(data)

        yield self.client.wait_for_data()
        self.assertEqual(
            self.client.received_dummy_packets,
            [(session_id_a, {'someParam': '123'})])

    @inlineCallbacks
    def test_authentication(self):
        request_id = str("0").zfill(16)
        session_id = str("1").zfill(16)

        request_body = (
            "<AUTHRequest>"
                "<requestId>%s</requestId>"
                "<userName>root</userName>"
                "<passWord>toor</passWord>"
                "<applicationId>1029384756</applicationId>"
            "</AUTHRequest>"
        % request_id)
        expected_request_packet = self.mk_serialized_packet(
            session_id, request_body)

        response_body = (
            "<AUTHResponse>"
                "<requestId>%s</requestId>"
                "<authMsg>SUCCESS</authMsg>"
            "</AUTHResponse>"
        % request_id)
        response_packet = self.mk_serialized_packet(session_id, response_body)
        self.server.responses[expected_request_packet] = response_packet

        self.client.login()
        yield self.client.wait_for_data()
        self.assertTrue(self.client.authenticated)

    @inlineCallbacks
    def test_data_request_received(self):
        session_id = "1" * 16
        body = (
            "<USSDRequest>"
                "<requestId>1291850641</requestId>"
                "<msisdn>27845335367</msisdn>"
                "<starCode>123</starCode>"
                "<clientId>123</clientId>"
                "<phase>2</phase>"
                "<dcs>15</dcs>"
                "<userdata>*123#</userdata>"
                "<msgtype>1</msgtype>"
                "<EndofSession>0</EndofSession>"
            "</USSDRequest>"
        )
        packet = self.mk_serialized_packet(session_id, body)
        self.client.authenticated = True
        self.server.send_data(packet)

        yield self.client.wait_for_data()
        expected_params = {
            'requestId': '1291850641',
            'msisdn': '27845335367',
            'starCode': '123',
            'clientId': '123',
            'phase': '2',
            'dcs': '15',
            'userdata': '*123#',
            'msgtype': '1',
            'EndofSession': '0',
        }
        self.assertEqual(
            self.client.received_data_request_packets,
            [(session_id, expected_params)])

    def test_validate_packet_fields(self):
        self.assertTrue(self.client.validate_packet_fields(
            {'a': 1, 'b': 2}, set(['a', 'b']), set(['b', 'c'])))

        self.assertFalse(self.client.validate_packet_fields(
            {'a': 1, 'b': 2}, set(['a', 'b', 'c'])))

        self.assertFalse(self.client.validate_packet_fields(
            {'a': 1, 'b': 2, 'c': 2, 'd': 3}, set(['a', 'b']), set(['c'])))

    @inlineCallbacks
    def test_send_data_response_for_continue_session(self):
        session_id = "1" * 16
        body = (
            "<USSDResponse>"
                "<requestId>1291850641</requestId>"
                "<msisdn>27845335367</msisdn>"
                "<starCode>123</starCode>"
                "<clientId>123</clientId>"
                "<phase>2</phase>"
                "<msgtype>2</msgtype>"
                "<dcs>15</dcs>"
                "<userdata>*123#</userdata>"
                "<EndofSession>0</EndofSession>"
            "</USSDResponse>"
        )
        expected_packet = self.mk_serialized_packet(session_id, body)

        self.client.send_data_response(
            session_id,
            end_session=False,
            requestId='1291850641',
            msisdn='27845335367',
            starCode='123',
            clientId='123',
            userdata='*123#',
        )

        received_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_packet, received_packet)

    @inlineCallbacks
    def test_send_data_response_for_end_session(self):
        session_id = "1" * 16
        body = (
            "<USSDResponse>"
                "<requestId>1291850641</requestId>"
                "<msisdn>27845335367</msisdn>"
                "<starCode>123</starCode>"
                "<clientId>123</clientId>"
                "<phase>2</phase>"
                "<msgtype>6</msgtype>"
                "<dcs>15</dcs>"
                "<userdata>*123#</userdata>"
                "<EndofSession>1</EndofSession>"
            "</USSDResponse>"
        )
        expected_packet = self.mk_serialized_packet(session_id, body)

        self.client.send_data_response(
            session_id,
            end_session=True,
            requestId='1291850641',
            msisdn='27845335367',
            starCode='123',
            clientId='123',
            userdata='*123#',
        )

        received_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_packet, received_packet)
