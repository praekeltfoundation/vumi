from twisted.trial import unittest
from twisted.internet.task import Clock
from twisted.internet.defer import inlineCallbacks

from vumi import log
from vumi.transports.mtn_nigeria.xml_over_tcp import XmlOverTcpClient
from vumi.transports.mtn_nigeria.tests import utils


class ToyXmlOverTcpClient(XmlOverTcpClient, utils.WaitForDataMixin):
    _PACKET_RECEIVED_HANDLERS = {'DummyPacket': 'dummy_packet_received'}
    _ERRORS = {'000': 'Dummy error occured'}

    def __init__(self):
        XmlOverTcpClient.__init__(self, 'root', 'toor', '1029384756')
        self.PACKET_RECEIVED_HANDLERS.update(self._PACKET_RECEIVED_HANDLERS)
        self.ERRORS.update(self._ERRORS)

        self.received_dummy_packets = []
        self.received_data_request_packets = []
        self.next_id = 0
        self.disconnected = False

    def connectionMade(self):
        pass

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

    def disconnect(self):
        self.disconnected = True


class XmlOverTcpClientServerMixin(utils.MockClientServerMixin):
    client_protocol = ToyXmlOverTcpClient
    server_protocol = utils.MockXmlOverTcpServer


class XmlOverTcpClientTestCase(unittest.TestCase, XmlOverTcpClientServerMixin):
    @inlineCallbacks
    def setUp(self):
        # stub logger
        self.log = []
        self.patch(log, 'msg', lambda msg: self.log.append(msg))

        yield self.start_protocols()

    def tearDown(self):
        self.stop_protocols()

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
        data = utils.mk_packet(session_id_a, body_a)
        data += utils.mk_packet(session_id_b, body_b)
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
        data = utils.mk_packet(session_id_a, body_a)
        data += utils.mk_packet(session_id_b, body_b)[:12]
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
        expected_request_packet = utils.mk_packet(
            session_id, request_body)

        response_body = (
            "<AUTHResponse>"
                "<requestId>%s</requestId>"
                "<authMsg>SUCCESS</authMsg>"
            "</AUTHResponse>"
        % request_id)
        response_packet = utils.mk_packet(session_id, response_body)
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
        expected_request_packet = utils.mk_packet(
            session_id, request_body)

        response_body = (
            "<AUTHError>"
                "<requestId>%s</requestId>"
                "<authMsg>FAILURE</authMsg>"
                "<errorCode>001</errorCode>"
            "</AUTHError>"
        % request_id)
        response_packet = utils.mk_packet(session_id, response_body)
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
        request_packet = utils.mk_packet(session_id, request_body)

        response_body = (
            "<USSDError>"
                "<requestId>%s</requestId>"
                "<errorCode>208</errorCode>"
            "</USSDError>"
        % request_id)
        expected_response_packet = utils.mk_packet(
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
        request_packet = utils.mk_packet(session_id, request_body)

        response_body = (
            "<USSDError>"
                "<requestId>%s</requestId>"
                "<errorCode>207</errorCode>"
            "</USSDError>"
        % request_id)
        expected_response_packet = utils.mk_packet(
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
        packet = utils.mk_packet(session_id, body)
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
        expected_packet = utils.mk_packet(session_id, body)

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
        expected_packet = utils.mk_packet(session_id, body)

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
        expected_packet = utils.mk_packet(session_id, body)

        self.client.authenticated = True
        self.client.send_data_response(
            session_id=session_id,
            request_id='1291850641',
            star_code='123',
            client_id='123',
            msisdn='27845335367',
            user_data='*123#',
            end_session=False)

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
        expected_packet = utils.mk_packet(session_id, body)

        self.client.authenticated = True
        self.client.send_data_response(
            session_id=session_id,
            request_id='1291850641',
            star_code='123',
            client_id='123',
            msisdn='27845335367',
            user_data='*123#',
            end_session=True)

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
        expected_request_packet_1 = utils.mk_packet(
            session_id_1, request_body_1)
        response_body_1 = (
            "<ENQResponse>"
                "<requestId>%s</requestId>"
                "<enqCmd>ENQUIRELINKRSP</enqCmd>"
            "</ENQResponse>"
        % request_id_1)
        response_packet_1 = utils.mk_packet(
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
        expected_request_packet_2 = utils.mk_packet(
            session_id_2, request_body_2)
        response_body_2 = (
            "<ENQResponse>"
                "<requestId>%s</requestId>"
                "<enqCmd>ENQUIRELINKRSP</enqCmd>"
            "</ENQResponse>"
        % request_id_2)
        response_packet_2 = utils.mk_packet(
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
        expected_request_packet = utils.mk_packet(
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
        request_packet = utils.mk_packet(session_id, request_body)

        response_body = (
            "<ENQResponse>"
                "<requestId>%s</requestId>"
                "<enqCmd>ENQUIRELINKRSP</enqCmd>"
            "</ENQResponse>"
        % request_id)
        expected_response_packet = utils.mk_packet(
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
        error_packet = utils.mk_packet(session_id, body)

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
        error_packet = utils.mk_packet(session_id, body)

        self.server.send_data(error_packet)
        yield self.client.wait_for_data()
        [error_msg] = self.log
        self.assertTrue('1337' in error_msg)
