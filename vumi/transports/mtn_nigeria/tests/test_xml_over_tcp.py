import struct

from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.task import Clock
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.protocol import Protocol, Factory, ClientFactory

from vumi import log
from vumi.transports.mtn_nigeria.xml_over_tcp import XmlOverTcpClient


class WaitForDataMixin(object):
    waiting_for_data = False
    deferred_data = Deferred()

    def wait_for_data(self):
        d = Deferred()
        self.deferred_data = d
        self.waiting_for_data = True
        return d

    def callback_deferred_data(self, data):
        if self.waiting_for_data and not self.deferred_data.called:
            self.waiting_for_data = False
            self.deferred_data.callback(data)


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
    _PACKET_RECEIVED_HANDLERS = {'DummyPacket': 'dummy_packet_received'}
    _ERRORS = {'000': 'Dummy error occured'}

    def __init__(self):
        XmlOverTcpClient.__init__(self, 'root', 'toor', '1029384756')
        self.PACKET_RECEIVED_HANDLERS.update(self._PACKET_RECEIVED_HANDLERS)
        self.ERRORS.update(self._ERRORS)

        self.received_dummy_packets = []
        self.received_data_request_packets = []
        self.next_id = 0
        self.clock = reactor
        self.disconnected = False

    def connectionMade(self):
        self.factory.deferred_client.callback(self)

    def dataReceived(self, data):
        XmlOverTcpClient.dataReceived(self, data)
        self.callback_deferred_data(data)

    def get_clock(self):
        return self.clock

    def dummy_packet_received(self, session_id, params):
        self.received_dummy_packets.append((session_id, params))

    def data_request_received(self, session_id, params):
        self.received_data_request_packets.append((session_id, params))

    def gen_id(self):
        id = str(self.next_id).zfill(16)
        self.next_id += 1
        return id

    def disconnect(self):
        self.disconnected = True


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
        # stub logger
        self.log = []
        self.patch(log, 'msg', lambda msg: self.log.append(msg))

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

    def stub_client_heartbeat(self, heartbeat_interval=120, timeout_period=20):
        self.client.heartbeat_interval = 120
        self.client.timeout_period = 20
        self.client.clock = Clock()

    @inlineCallbacks
    def test_contiguous_packets_received(self):
        session_id_a = '1' * 16
        body_a = "<DummyPacket><someParam>123</someParam></DummyPacket>"
        session_id_b = '2' * 16
        body_b = "<DummyPacket><someParam>456</someParam></DummyPacket>"
        data = self.mk_serialized_packet(session_id_a, body_a)
        data += self.mk_serialized_packet(session_id_b, body_b)
        self.client.authenticated = True
        self.server.send_data(data)

        yield self.client.wait_for_data()
        self.assertEqual(
            self.client.received_dummy_packets, [
                (session_id_a, {'someParam': '123'}),
                (session_id_b, {'someParam': '456'}),
            ])

    @inlineCallbacks
    def test_partial_data_received(self):
        session_id_a = '1' * 16
        body_a = "<DummyPacket><someParam>123</someParam></DummyPacket>"
        session_id_b = '2' * 16
        body_b = "<DummyPacket><someParam>456</someParam></DummyPacket>"

        # add a full first packet, then concatenate a sliced version of a
        # second packet
        data = self.mk_serialized_packet(session_id_a, body_a)
        data += self.mk_serialized_packet(session_id_b, body_b)[:12]
        self.client.authenticated = True
        self.server.send_data(data)

        yield self.client.wait_for_data()
        self.assertEqual(
            self.client.received_dummy_packets,
            [(session_id_a, {'someParam': '123'})])

    @inlineCallbacks
    def test_authentication(self):
        request_id = str('0').zfill(16)
        session_id = str('1').zfill(16)

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
    def test_authentication_error_handling(self):
        request_id = str('0').zfill(16)
        session_id = str('1').zfill(16)

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
            "<AUTHError>"
                "<requestId>%s</requestId>"
                "<authMsg>FAILURE</authMsg>"
                "<errorCode>001</errorCode>"
            "</AUTHError>"
        % request_id)
        response_packet = self.mk_serialized_packet(session_id, response_body)
        self.server.responses[expected_request_packet] = response_packet

        self.client.login()
        yield self.client.wait_for_data()
        self.assertFalse(self.client.authenticated)
        self.assertTrue(self.client.disconnected)

    @inlineCallbacks
    def test_unknown_packet_handling(self):
        session_id = '0' * 16
        request_id = '1' * 16

        request_body = (
            "<UnknownPacket>"
                "<requestId>%s</requestId>"
            "</UnknownPacket>"
        % request_id)
        request_packet = self.mk_serialized_packet(session_id, request_body)

        response_body = (
            "<USSDError>"
                "<requestId>%s</requestId>"
                "<errorCode>208</errorCode>"
            "</USSDError>"
        % request_id)
        expected_response_packet = self.mk_serialized_packet(
            session_id, response_body)

        self.server.send_data(request_packet)
        yield self.client.wait_for_data()
        self.assertTrue('UnknownPacket' in self.log.pop())

        response_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_response_packet, response_packet)

    @inlineCallbacks
    def test_packet_received_before_auth(self):
        session_id = '0' * 16
        request_id = '1' * 16

        request_body = (
            "<DummyPacket>"
                "<requestId>%s</requestId>"
            "</DummyPacket>"
        % request_id)
        request_packet = self.mk_serialized_packet(session_id, request_body)

        response_body = (
            "<USSDError>"
                "<requestId>%s</requestId>"
                "<errorCode>207</errorCode>"
            "</USSDError>"
        % request_id)
        expected_response_packet = self.mk_serialized_packet(
            session_id, response_body)

        self.server.send_data(request_packet)
        yield self.client.wait_for_data()
        self.assertTrue('DummyPacket' in self.log.pop())

        response_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_response_packet, response_packet)

    def test_packet_send_before_auth(self):
        self.client.send_packet('0' * 16, 'DummyPacket', [])
        self.assertTrue('DummyPacket' in self.log.pop())

    @inlineCallbacks
    def test_data_request_handling(self):
        session_id = '1' * 16
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

    def test_field_validation_for_valid_cases(self):
        self.assertTrue(self.client.validate_packet_fields(
            '0', {'a': '1', 'b': '2'}, set(['a', 'b']), set(['b', 'c'])))

        self.assertTrue(self.client.validate_packet_fields(
            '0', {'a': '1', 'b': '2'}, set(['a', 'b'])))

    @inlineCallbacks
    def test_field_validation_for_missing_mandatory_fields(self):
        session_id = '0' * 16
        body = (
            "<USSDError>"
                "<requestId>1291850641</requestId>"
                "<errorCode>208</errorCode>"
            "</USSDError>"
        )
        expected_packet = self.mk_serialized_packet(session_id, body)

        params = {
            'requestId': '1291850641',
            'a': '1',
            'b': '2'
        }
        self.assertFalse(self.client.validate_packet_fields(
            session_id, params, set(['requestId', 'a', 'b', 'c'])))
        self.assertTrue(str(['c']) in self.log.pop())

        received_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_packet, received_packet)

    @inlineCallbacks
    def test_field_validation_for_unexpected_fields(self):
        session_id = '0' * 16
        body = (
            "<USSDError>"
                "<requestId>1291850641</requestId>"
                "<errorCode>208</errorCode>"
            "</USSDError>"
        )
        expected_packet = self.mk_serialized_packet(session_id, body)

        params = {
            'requestId': '1291850641',
            'a': '1',
            'b': '2',
            'd': '3'
        }
        self.assertFalse(self.client.validate_packet_fields(
            session_id, params, set(['requestId', 'a', 'b'])))
        self.assertTrue(str(['d']) in self.log.pop())

        received_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_packet, received_packet)

    @inlineCallbacks
    def test_continuing_session_data_response(self):
        session_id = '1' * 16
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

        self.client.authenticated = True
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
    def test_ending_session_data_response(self):
        session_id = '1' * 16
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

        self.client.authenticated = True
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

    @inlineCallbacks
    def test_periodic_client_enquire_link(self):
        session_id_1 = str('0').zfill(16)
        request_id_1 = str('1').zfill(16)
        request_body_1 = (
            "<ENQRequest>"
                "<requestId>%s</requestId>"
                "<enqCmd>ENQUIRELINK</enqCmd>"
            "</ENQRequest>"
        % request_id_1)
        expected_request_packet_1 = self.mk_serialized_packet(
            session_id_1, request_body_1)
        response_body_1 = (
            "<ENQResponse>"
                "<requestId>%s</requestId>"
                "<enqCmd>ENQUIRELINKRSP</enqCmd>"
            "</ENQResponse>"
        % request_id_1)
        response_packet_1 = self.mk_serialized_packet(
            session_id_1, response_body_1)
        self.server.responses[expected_request_packet_1] = response_packet_1

        session_id_2 = str('2').zfill(16)
        request_id_2 = str('3').zfill(16)
        request_body_2 = (
            "<ENQRequest>"
                "<requestId>%s</requestId>"
                "<enqCmd>ENQUIRELINK</enqCmd>"
            "</ENQRequest>"
        % request_id_2)
        expected_request_packet_2 = self.mk_serialized_packet(
            session_id_2, request_body_2)
        response_body_2 = (
            "<ENQResponse>"
                "<requestId>%s</requestId>"
                "<enqCmd>ENQUIRELINKRSP</enqCmd>"
            "</ENQResponse>"
        % request_id_2)
        response_packet_2 = self.mk_serialized_packet(
            session_id_2, response_body_2)
        self.server.responses[expected_request_packet_2] = response_packet_2

        self.stub_client_heartbeat()
        self.client.authenticated = True
        self.client.start_heartbeat()
        timeout_t0 = self.client.scheduled_timeout.getTime()

        # advance to just after the first heartbeat is sent
        self.client.clock.advance(120.1)
        yield self.client.wait_for_data()
        timeout_t1 = self.client.scheduled_timeout.getTime()
        self.assertTrue(timeout_t1 > timeout_t0)

        # advance to just after the second heartbeat is sent
        self.client.clock.advance(120)
        yield self.client.wait_for_data()
        timeout_t2 = self.client.scheduled_timeout.getTime()
        self.assertTrue(timeout_t2 > timeout_t1)

    @inlineCallbacks
    def test_timeout(self):
        session_id = str('0').zfill(16)
        request_id = str('1').zfill(16)

        request_body = (
            "<ENQRequest>"
                "<requestId>%s</requestId>"
                "<enqCmd>ENQUIRELINK</enqCmd>"
            "</ENQRequest>"
        % request_id)
        expected_request_packet = self.mk_serialized_packet(
            session_id, request_body)

        self.stub_client_heartbeat()
        self.client.authenticated = True
        self.client.start_heartbeat()

        # advance to just after the first heartbeat is sent
        self.client.clock.advance(120.1)
        received_request_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_request_packet, received_request_packet)

        # advance to just after the timeout occured
        self.client.clock.advance(20)
        self.assertTrue(self.client.disconnected)

    @inlineCallbacks
    def test_server_enquire_link(self):
        session_id = str('0').zfill(16)
        request_id = str('1').zfill(16)

        request_body = (
            "<ENQRequest>"
                "<requestId>%s</requestId>"
                "<enqCmd>ENQUIRELINK</enqCmd>"
            "</ENQRequest>"
        % request_id)
        request_packet = self.mk_serialized_packet(session_id, request_body)

        response_body = (
            "<ENQResponse>"
                "<requestId>%s</requestId>"
                "<enqCmd>ENQUIRELINKRSP</enqCmd>"
            "</ENQResponse>"
        % request_id)
        expected_response_packet = self.mk_serialized_packet(
            session_id, response_body)

        self.client.authenticated = True
        self.server.send_data(request_packet)
        response_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_response_packet, response_packet)

    @inlineCallbacks
    def test_error_response_handling_for_known_codes(self):
        session_id = '0' * 16
        body = (
            "<USSDError>"
                "<requestId>1</requestId>"
                "<errorCode>000</errorCode>"
            "</USSDError>"
        )
        error_packet = self.mk_serialized_packet(session_id, body)

        self.server.send_data(error_packet)
        yield self.client.wait_for_data()
        self.assertTrue('Dummy error occured' in self.log.pop())

    @inlineCallbacks
    def test_error_response_handling_for_unknown_codes(self):
        session_id = '0' * 16
        body = (
            "<USSDError>"
                "<requestId>1</requestId>"
                "<errorCode>1337</errorCode>"
            "</USSDError>"
        )
        error_packet = self.mk_serialized_packet(session_id, body)

        self.server.send_data(error_packet)
        yield self.client.wait_for_data()
        [error_msg] = self.log
        self.assertTrue('1337' in error_msg)
