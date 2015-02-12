# -*- test-case-name: vumi.transports.mtn_nigeria.tests.test_xml_over_tcp -*-
# -*- coding: utf-8 -*-

import struct
from itertools import count

from twisted.internet.task import Clock
from twisted.internet.defer import inlineCallbacks

from vumi import log
from vumi.transports.mtn_nigeria.xml_over_tcp import (
    XmlOverTcpError, CodedXmlOverTcpError, XmlOverTcpClient)
from vumi.transports.mtn_nigeria.tests import utils
from vumi.tests.helpers import VumiTestCase


class ToyXmlOverTcpClient(XmlOverTcpClient, utils.WaitForDataMixin):
    _PACKET_RECEIVED_HANDLERS = {'DummyPacket': 'dummy_packet_received'}

    def __init__(self):
        XmlOverTcpClient.__init__(self, 'root', 'toor', '1029384756')
        self.PACKET_RECEIVED_HANDLERS.update(self._PACKET_RECEIVED_HANDLERS)

        self.received_dummy_packets = []
        self.received_data_request_packets = []
        self.disconnected = False

        self.session_id_counter = count()
        self.generated_session_ids = []

        self.request_id_counter = count()
        self.generated_request_ids = []

    def connectionMade(self):
        pass

    def dataReceived(self, data):
        XmlOverTcpClient.dataReceived(self, data)
        self.callback_deferred_data(data)

    def dummy_packet_received(self, session_id, params):
        self.received_dummy_packets.append((session_id, params))

    def data_request_received(self, session_id, params):
        self.received_data_request_packets.append((session_id, params))

    def disconnect(self):
        self.disconnected = True

    @classmethod
    def session_id_from_nr(cls, nr):
        return cls.serialize_header_field(nr, cls.SESSION_ID_HEADER_SIZE)

    def gen_session_id(self):
        return self.session_id_from_nr(next(self.session_id_counter))

    def gen_request_id(self):
        return str(next(self.request_id_counter))


class XmlOverTcpClientServerMixin(utils.MockClientServerMixin):
    client_protocol = ToyXmlOverTcpClient
    server_protocol = utils.MockXmlOverTcpServer


class TestXmlOverTcpClient(VumiTestCase, XmlOverTcpClientServerMixin):
    def setUp(self):
        errors = dict(CodedXmlOverTcpError.ERRORS)
        errors['000'] = 'Dummy error occured'
        self.patch(CodedXmlOverTcpError, 'ERRORS', errors)

        self.logs = {'msg': [], 'err': [], 'debug': []}
        self.patch(log, 'msg', lambda *a: self.append_to_log('msg', *a))
        self.patch(log, 'err', lambda *a: self.append_to_log('err', *a))
        self.patch(log, 'debug', lambda *a: self.append_to_log('debug', *a))

        self.add_cleanup(self.stop_protocols)
        return self.start_protocols()

    def append_to_log(self, log_name, *args):
        self.logs[log_name].append(' '.join(str(a) for a in args))

    def assert_in_log(self, log_name, substr):
        log = self.logs[log_name]
        if not any(substr in m for m in log):
            self.fail("'%s' not in %s log" % (substr, log_name))

    @staticmethod
    def mk_raw_packet(session_id, length_header, body):
        header = struct.pack(
            XmlOverTcpClient.HEADER_FORMAT, session_id, length_header)
        return header + body

    @inlineCallbacks
    def test_packet_parsing_for_packets_with_wierd_bodies(self):
        data = utils.mk_packet('0', "<BadPacket>")
        self.client.authenticated = True
        self.server.send_data(data)

        yield self.client.wait_for_data()
        err_msg = self.logs['err'][0]
        self.assertTrue("Error parsing packet" in err_msg)
        self.assertTrue(('%r' % (data,)) in err_msg)
        self.assertTrue(self.client.disconnected)

    def test_packet_header_serializing(self):
        self.assertEqual(
            XmlOverTcpClient.serialize_header('23', 'abcdef'),
            '23\0\0\0\0\0\0\0\0\0\0\0\0\0\0'
            '38\0\0\0\0\0\0\0\0\0\0\0\0\0\0')

    def test_packet_header_deserializing(self):
        session_id, length = XmlOverTcpClient.deserialize_header(
            '23\0\0\0\0\0\0\0\0\0\0\0\0\0\0'
            '38\0\0\0\0\0\0\0\0\0\0\0\0\0\0')

        self.assertEqual(session_id, '23')
        self.assertEqual(length, 38)

    def test_packet_body_serializing(self):
        body = XmlOverTcpClient.serialize_body(
            'DummyPacket',
            [('requestId', '123456789abcdefg')])
        expected_body = (
            "<DummyPacket>"
            "<requestId>123456789abcdefg</requestId>"
            "</DummyPacket>")
        self.assertEqual(body, expected_body)

    def test_packet_body_serializing_for_non_latin1_chars(self):
        body = XmlOverTcpClient.serialize_body(
            'DummyPacket',
            [('requestId', '123456789abcdefg'),
             ('userdata', u'Erdős')])
        expected_body = (
            "<DummyPacket>"
            "<requestId>123456789abcdefg</requestId>"
            "<userdata>Erd&#337;s</userdata>"
            "</DummyPacket>")
        self.assertEqual(body, expected_body)

    def test_packet_body_deserializing(self):
        body = '\n'.join([
            "<USSDRequest>",
            "\t<requestId>",
            "\t\t123456789abcdefg",
            "\t</requestId>",
            "\t<msisdn>",
            "\t\t2347067123456",
            "\t</msisdn>",
            "\t<starCode>",
            "\t\t759",
            "\t</starCode>",
            "\t<clientId>",
            "\t\t441",
            "\t</clientId>",
            "\t<phase>",
            "\t\t2",
            "\t</phase>",
            "\t<dcs>",
            "\t\t15",
            "\t</dcs>",
            "\t<userdata>",
            "\t\t\xa4",
            "\t</userdata>",
            "\t<msgtype>",
            "\t\t4",
            "\t</msgtype>",
            "\t<EndofSession>",
            "\t\t0",
            "\t</EndofSession>",
            "</USSDRequest>\n"
        ])
        packet_type, params = XmlOverTcpClient.deserialize_body(body)

        self.assertEqual(packet_type, 'USSDRequest')
        self.assertEqual(params, {
            'requestId': '123456789abcdefg',
            'msisdn': '2347067123456',
            'userdata': u'\xa4',
            'clientId': '441',
            'dcs': '15',
            'msgtype': '4',
            'phase': '2',
            'starCode': '759',
            'EndofSession': '0',
        })

    def test_packet_body_deserializing_for_invalid_xml_chars(self):
        body = '\n'.join([
            '<USSDRequest>'
            '\t<requestId>'
            '\t\t123456789abcdefg'
            '\t</requestId>'
            '\t<msisdn>'
            '\t\t2341234567890',
            '\t</msisdn>',
            '\t<starCode>',
            '\t\t759',
            '\t</starCode>',
            '\t<clientId>',
            '\t\t441',
            '\t</clientId>',
            '\t<phase>',
            '\t\t2',
            '\t</phase>',
            '\t<dcs>',
            '\t\t229',
            '\t</dcs>',
            '\t<userdata>',
            '\t\t\x18',
            '\t</userdata>',
            '\t<msgtype>',
            '\t\t4',
            '\t</msgtype>',
            '\t<EndofSession>',
            '\t\t0',
            '\t</EndofSession>',
            '</USSDRequest>',
        ])
        packet_type, params = XmlOverTcpClient.deserialize_body(body)

        self.assertEqual(packet_type, 'USSDRequest')
        self.assertEqual(params, {
            'EndofSession': '0',
            'clientId': '441',
            'dcs': '229',
            'msgtype': '4',
            'msisdn': '2341234567890',
            'phase': '2',
            'requestId': '123456789abcdefg',
            'starCode': '759',
            'userdata': u'\x18',
        })

    def test_packet_body_deserializing_for_entity_references(self):
        body = '\n'.join([
            '<USSDRequest>',
            '\t<requestId>',
            '\t\t123456789abcdefg',
            '\t</requestId>',
            '\t<msisdn>',
            '\t\t2341234567890',
            '\t</msisdn>',
            '\t<starCode>',
            '\t\t759',
            '\t</starCode>',
            '\t<clientId>',
            '\t\t441',
            '\t</clientId>',
            '\t<phase>',
            '\t\t2',
            '\t</phase>',
            '\t<dcs>',
            '\t\t15',
            '\t</dcs>',
            '\t<userdata>',
            '\t\tTeam&apos;s rank',
            '\t</userdata>',
            '\t<msgtype>\n\t\t4',
            '\t</msgtype>',
            '\t<EndofSession>',
            '\t\t0',
            '\t</EndofSession>',
            '</USSDRequest>',
        ])
        packet_type, params = XmlOverTcpClient.deserialize_body(body)

        self.assertEqual(packet_type, 'USSDRequest')
        self.assertEqual(params, {
            'EndofSession': u'0',
            'clientId': u'441',
            'dcs': u'15',
            'msgtype': u'4',
            'msisdn': u'2341234567890',
            'phase': u'2',
            'requestId': u'123456789abcdefg',
            'starCode': u'759',
            'userdata': u"Team's rank"
        })

    @inlineCallbacks
    def test_contiguous_packets_received(self):
        body_a = "<DummyPacket><someParam>123</someParam></DummyPacket>"
        body_b = "<DummyPacket><someParam>456</someParam></DummyPacket>"

        data = utils.mk_packet('0', body_a)
        data += utils.mk_packet('1', body_b)
        self.client.authenticated = True
        self.server.send_data(data)

        yield self.client.wait_for_data()
        self.assertEqual(
            self.client.received_dummy_packets, [
                ('0', {'someParam': '123'}),
                ('1', {'someParam': '456'}),
            ])

    @inlineCallbacks
    def test_packets_split_over_socket_reads(self):
        body = "<DummyPacket><someParam>123</someParam></DummyPacket>"
        data = utils.mk_packet('0', body)
        split_position = int(len(data) / 2)

        self.client.authenticated = True

        self.server.send_data(data[:split_position])
        yield self.client.wait_for_data()

        self.server.send_data(data[split_position:])
        yield self.client.wait_for_data()

        self.assertEqual(
            self.client.received_dummy_packets,
            [('0', {'someParam': '123'})])

    @inlineCallbacks
    def test_partial_data_received(self):
        body_a = "<DummyPacket><someParam>123</someParam></DummyPacket>"
        body_b = "<DummyPacket><someParam>456</someParam></DummyPacket>"

        # add a full first packet, then concatenate a sliced version of a
        # second packet
        data = utils.mk_packet('0', body_a)
        data += utils.mk_packet('1', body_b)[:12]
        self.client.authenticated = True
        self.server.send_data(data)

        yield self.client.wait_for_data()
        self.assertEqual(
            self.client.received_dummy_packets,
            [('0', {'someParam': '123'})])

    @inlineCallbacks
    def test_authentication(self):
        request_body = (
            "<AUTHRequest>"
            "<requestId>0</requestId>"
            "<userName>root</userName>"
            "<passWord>toor</passWord>"
            "<applicationId>1029384756</applicationId>"
            "</AUTHRequest>")
        expected_request_packet = utils.mk_packet('0', request_body)

        response_body = (
            "<AUTHResponse>"
            "<requestId>0</requestId>"
            "<authMsg>SUCCESS</authMsg>"
            "</AUTHResponse>")
        response_packet = utils.mk_packet('a', response_body)
        self.server.responses[expected_request_packet] = response_packet

        self.client.login()
        yield self.client.wait_for_data()
        self.assertTrue(self.client.authenticated)

    @inlineCallbacks
    def test_authentication_error_handling(self):
        request_body = (
            "<AUTHRequest>"
            "<requestId>0</requestId>"
            "<userName>root</userName>"
            "<passWord>toor</passWord>"
            "<applicationId>1029384756</applicationId>"
            "</AUTHRequest>")
        expected_request_packet = utils.mk_packet('0', request_body)

        response_body = (
            "<AUTHError>"
            "<requestId>0</requestId>"
            "<authMsg>FAILURE</authMsg>"
            "<errorCode>001</errorCode>"
            "</AUTHError>")
        response_packet = utils.mk_packet('0', response_body)
        self.server.responses[expected_request_packet] = response_packet

        self.client.login()
        yield self.client.wait_for_data()
        self.assertFalse(self.client.authenticated)
        self.assertTrue(self.client.disconnected)
        self.assert_in_log('err', 'Login failed, disconnecting')

    @inlineCallbacks
    def test_unknown_packet_handling(self):
        request_body = (
            "<UnknownPacket><requestId>0</requestId></UnknownPacket>")
        request_packet = utils.mk_packet('0', request_body)

        response_body = (
            "<USSDError>"
            "<requestId>0</requestId>"
            "<errorCode>208</errorCode>"
            "</USSDError>")
        expected_response_packet = utils.mk_packet('0', response_body)

        self.server.send_data(request_packet)
        yield self.client.wait_for_data()

        response_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_response_packet, response_packet)
        self.assert_in_log(
            'err', "Packet of an unknown type received: UnknownPacket")

    @inlineCallbacks
    def test_packet_received_before_auth(self):
        request_body = (
            "<DummyPacket>"
            "<requestId>0</requestId>"
            "</DummyPacket>")
        request_packet = utils.mk_packet('0', request_body)

        response_body = (
            "<USSDError>"
            "<requestId>0</requestId>"
            "<errorCode>207</errorCode>"
            "</USSDError>")
        expected_response_packet = utils.mk_packet('0', response_body)

        self.server.send_data(request_packet)
        yield self.client.wait_for_data()

        response_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_response_packet, response_packet)
        self.assert_in_log(
            'err',
            "'DummyPacket' packet received before client authentication was "
            "completed")

    def test_packet_send_before_auth(self):
        self.assertRaises(
            XmlOverTcpError,
            self.client.send_packet, '0', 'DummyPacket', [])

    @inlineCallbacks
    def test_data_request_handling(self):
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
        packet = utils.mk_packet('0', body)
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
            [('0', expected_params)])

    @inlineCallbacks
    def test_data_response_handling(self):
        body = (
            "<USSDResponse>"
            "<requestId>1291850641</requestId>"
            "<msisdn>27845335367</msisdn>"
            "<starCode>123</starCode>"
            "<clientId>123</clientId>"
            "<phase>2</phase>"
            "<dcs>15</dcs>"
            "<userdata>Session closed due to cause 0</userdata>"
            "<msgtype>1</msgtype>"
            "<EndofSession>1</EndofSession>"
            "</USSDResponse>"
        )
        packet = utils.mk_packet('0', body)
        self.client.authenticated = True
        self.server.send_data(packet)

        yield self.client.wait_for_data()
        self.assert_in_log(
            'msg', "Received spurious USSDResponse message, ignoring.")

    def test_field_validation_for_valid_cases(self):
        self.client.validate_packet_fields(
            {'a': '1', 'b': '2'},
            set(['a', 'b']),
            set(['b', 'c']))

        self.client.validate_packet_fields(
            {'a': '1', 'b': '2'},
            set(['a', 'b']))

    def test_field_validation_for_missing_mandatory_fields(self):
        self.assertRaises(
            XmlOverTcpError,
            self.client.validate_packet_fields,
            {'requestId': '1291850641', 'a': '1', 'b': '2'},
            set(['requestId', 'a', 'b', 'c']))

    def test_field_validation_for_unexpected_fields(self):
        self.assertRaises(
            XmlOverTcpError,
            self.client.validate_packet_fields,
            {'requestId': '1291850641', 'a': '1', 'b': '2', 'd': '3'},
            set(['requestId', 'a', 'b']))

    @inlineCallbacks
    def test_login_response_validation(self):
        body = "<AUTHResponse><requestId>0</requestId></AUTHResponse>"
        bad_packet = utils.mk_packet('0', body)

        self.server.send_data(bad_packet)
        yield self.client.wait_for_data()
        self.assert_in_log(
            'err',
            "(208) Invalid Message: Missing mandatory fields in received "
            "packet: %s" % ['authMsg'])

        received_packet = yield self.server.wait_for_data()
        self.assertEqual(received_packet, utils.mk_packet(
            '0',
            "<USSDError>"
            "<requestId>0</requestId>"
            "<errorCode>208</errorCode>"
            "</USSDError>"))

    @inlineCallbacks
    def test_login_error_response_validation(self):
        bad_packet = utils.mk_packet(
            '0', "<AUTHError><requestId>0</requestId></AUTHError>")

        self.server.send_data(bad_packet)
        yield self.client.wait_for_data()
        self.assert_in_log(
            'err',
            "(208) Invalid Message: Missing mandatory fields in received "
            "packet: %s" % ['authMsg', 'errorCode'])

        received_packet = yield self.server.wait_for_data()
        self.assertEqual(received_packet, utils.mk_packet(
            '0',
            "<USSDError>"
            "<requestId>0</requestId>"
            "<errorCode>208</errorCode>"
            "</USSDError>"))

    @inlineCallbacks
    def test_error_response_validation(self):
        bad_packet = utils.mk_packet(
            '0', "<USSDError><requestId>0</requestId></USSDError>")

        self.server.send_data(bad_packet)
        yield self.client.wait_for_data()
        self.assert_in_log(
            'err',
            "(208) Invalid Message: Missing mandatory fields in received "
            "packet: %s" % ['errorCode'])

        received_packet = yield self.server.wait_for_data()
        self.assertEqual(received_packet, utils.mk_packet(
            '0',
            "<USSDError>"
            "<requestId>0</requestId>"
            "<errorCode>208</errorCode>"
            "</USSDError>"))

    @inlineCallbacks
    def test_data_request_validation(self):
        bad_packet = utils.mk_packet(
            '0', "<USSDRequest><requestId>0</requestId></USSDRequest>")

        self.client.authenticated = True
        self.server.send_data(bad_packet)
        yield self.client.wait_for_data()

        missing_fields = ['userdata', 'msisdn', 'clientId', 'starCode',
                          'msgtype', 'phase', 'dcs']
        self.assert_in_log(
            'err',
            "(208) Invalid Message: Missing mandatory fields in received "
            "packet: %s" % sorted(missing_fields))

        received_packet = yield self.server.wait_for_data()
        self.assertEqual(received_packet, utils.mk_packet(
            '0',
            "<USSDError>"
            "<requestId>0</requestId>"
            "<errorCode>208</errorCode>"
            "</USSDError>"))

    @inlineCallbacks
    def test_enquire_link_request_validation(self):
        bad_packet = utils.mk_packet(
            '0', "<ENQRequest><requestId>0</requestId></ENQRequest>")

        self.client.authenticated = True
        self.server.send_data(bad_packet)
        yield self.client.wait_for_data()
        self.assert_in_log(
            'err',
            "(208) Invalid Message: Missing mandatory fields in received "
            "packet: %s" % ['enqCmd'])

        received_packet = yield self.server.wait_for_data()
        self.assertEqual(received_packet, utils.mk_packet(
            '0',
            "<USSDError>"
            "<requestId>0</requestId>"
            "<errorCode>208</errorCode>"
            "</USSDError>"))

    @inlineCallbacks
    def test_enquire_link_response_validation(self):
        bad_packet = utils.mk_packet(
            '0', "<ENQResponse><requestId>0</requestId></ENQResponse>")

        self.client.authenticated = True
        self.server.send_data(bad_packet)
        yield self.client.wait_for_data()
        self.assert_in_log(
            'err',
            "(208) Invalid Message: Missing mandatory fields in received "
            "packet: %s" % ['enqCmd'])

        received_packet = yield self.server.wait_for_data()
        self.assertEqual(received_packet, utils.mk_packet(
            '0',
            "<USSDError>"
            "<requestId>0</requestId>"
            "<errorCode>208</errorCode>"
            "</USSDError>"))

    @inlineCallbacks
    def test_continuing_session_data_response(self):
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
            "<delvrpt>0</delvrpt>"
            "</USSDResponse>"
        )
        expected_packet = utils.mk_packet('0', body)

        self.client.authenticated = True
        self.client.send_data_response(
            session_id='0',
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
            "<delvrpt>0</delvrpt>"
            "</USSDResponse>"
        )
        expected_packet = utils.mk_packet('0', body)

        self.client.authenticated = True
        self.client.send_data_response(
            session_id='0',
            request_id='1291850641',
            star_code='123',
            client_id='123',
            msisdn='27845335367',
            user_data='*123#',
            end_session=True)

        received_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_packet, received_packet)

    def assert_next_timeout(self, t):
        return self.assertAlmostEqual(
            self.client.scheduled_timeout.getTime(), t, 1)

    def assert_timeout_cancelled(self):
        self.assertFalse(self.client.scheduled_timeout.active())

    @inlineCallbacks
    def test_periodic_client_enquire_link(self):
        request_body_a = (
            "<ENQRequest>"
            "<requestId>0</requestId>"
            "<enqCmd>ENQUIRELINK</enqCmd>"
            "</ENQRequest>")
        expected_request_packet_a = utils.mk_packet('0', request_body_a)

        response_body_a = (
            "<ENQResponse>"
            "<requestId>0</requestId>"
            "<enqCmd>ENQUIRELINKRSP</enqCmd>"
            "</ENQResponse>")
        response_packet_a = utils.mk_packet('0', response_body_a)
        self.server.responses[expected_request_packet_a] = response_packet_a

        request_body_b = (
            "<ENQRequest>"
            "<requestId>1</requestId>"
            "<enqCmd>ENQUIRELINK</enqCmd>"
            "</ENQRequest>")
        expected_request_packet_b = utils.mk_packet('1', request_body_b)

        response_body_b = (
            "<ENQResponse>"
            "<requestId>1</requestId>"
            "<enqCmd>ENQUIRELINKRSP</enqCmd>"
            "</ENQResponse>")
        response_packet_b = utils.mk_packet('1', response_body_b)
        self.server.responses[expected_request_packet_b] = response_packet_b

        clock = Clock()
        t0 = clock.seconds()
        self.client.clock = clock
        self.client.enquire_link_interval = 120
        self.client.timeout_period = 20
        self.client.authenticated = True
        self.client.start_periodic_enquire_link()

        # advance to just after the first enquire link request
        clock.advance(0.01)
        self.assert_next_timeout(t0 + 20)

        # wait for the first enquire link response
        yield self.client.wait_for_data()
        self.assert_timeout_cancelled()

        # advance to just after the second enquire link request
        clock.advance(120.01)
        self.assert_next_timeout(t0 + 140)

        # wait for the second enquire link response
        yield self.client.wait_for_data()
        self.assert_timeout_cancelled()

    @inlineCallbacks
    def test_timeout(self):
        request_body = (
            "<ENQRequest>"
            "<requestId>0</requestId>"
            "<enqCmd>ENQUIRELINK</enqCmd>"
            "</ENQRequest>")
        expected_request_packet = utils.mk_packet('0', request_body)

        clock = Clock()
        self.client.clock = clock
        self.client.enquire_link_interval = 120
        self.client.timeout_period = 20
        self.client.authenticated = True
        self.client.start_periodic_enquire_link()

        # wait for the first enquire link request
        received_request_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_request_packet, received_request_packet)

        # advance to just before the timeout should occur
        clock.advance(19.9)
        self.assertFalse(self.client.disconnected)

        # advance to just after the timeout should occur
        clock.advance(0.1)
        self.assertTrue(self.client.disconnected)
        self.assert_in_log(
            'msg',
            "No enquire link response received after 20 seconds, "
            "disconnecting")

    @inlineCallbacks
    def test_server_enquire_link(self):
        request_body = (
            "<ENQRequest>"
            "<requestId>0</requestId>"
            "<enqCmd>ENQUIRELINK</enqCmd>"
            "</ENQRequest>")
        request_packet = utils.mk_packet('0', request_body)

        response_body = (
            "<ENQResponse>"
            "<requestId>0</requestId>"
            "<enqCmd>ENQUIRELINKRSP</enqCmd>"
            "</ENQResponse>")
        expected_response_packet = utils.mk_packet('0', response_body)

        self.client.authenticated = True
        self.server.send_data(request_packet)
        response_packet = yield self.server.wait_for_data()
        self.assertEqual(expected_response_packet, response_packet)

    @inlineCallbacks
    def test_error_response_handling_for_known_codes(self):
        body = (
            "<USSDError>"
            "<requestId>0</requestId>"
            "<errorCode>000</errorCode>"
            "<errorMsg>Some Reason</errorMsg>"
            "</USSDError>"
        )
        error_packet = utils.mk_packet('0', body)

        self.server.send_data(error_packet)
        yield self.client.wait_for_data()
        self.assert_in_log(
            'err',
            "Server sent error message: (000) Dummy error occured: "
            "Some Reason")

    @inlineCallbacks
    def test_error_response_handling_for_unknown_codes(self):
        body = (
            "<USSDError>"
            "<requestId>0</requestId>"
            "<errorCode>1337</errorCode>"
            "<errorMsg>Some Reason</errorMsg>"
            "</USSDError>"
        )
        error_packet = utils.mk_packet('0', body)

        self.server.send_data(error_packet)
        yield self.client.wait_for_data()
        self.assert_in_log(
            'err',
            "Server sent error message: (1337) Unknown Code: "
            "Some Reason")
