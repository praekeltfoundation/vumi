# -*- test-case-name: vumi.transports.mtn_nigeria.tests.test_mtn_nigeria -*-

from twisted.internet.defer import Deferred, inlineCallbacks

from vumi.message import TransportUserMessage
from vumi.transports.mtn_nigeria.tests import utils
from vumi.transports.tests.utils import TransportTestCase
from vumi.transports.mtn_nigeria import MtnNigeriaUssdTransport
from vumi.transports.mtn_nigeria.tests.utils import MockXmlOverTcpServerMixin
from vumi.transports.mtn_nigeria.xml_over_tcp import (
    XmlOverTcpError, CodedXmlOverTcpError, XmlOverTcpClient)


class TestMtnNigeriaUssdTransportTestCase(TransportTestCase,
                                          MockXmlOverTcpServerMixin):

    transport_class = MtnNigeriaUssdTransport
    transport_name = 'test_mtn_nigeria_ussd_transport'

    _session_id_header_size = XmlOverTcpClient.SESSION_ID_HEADER_SIZE
    _params = {
        'request_id': '1291850641',
        'msisdn': '27845335367',
        'star_code': '123',
        'client_id': '0123',
        'phase': '2',
        'dcs': '15',
        'user_data': '*123#',
        'msg_type': '1',
        'end_of_session': '0',
    }
    _request_body = (
        "<USSDRequest>"
            "<requestId>%(request_id)s</requestId>"
            "<msisdn>%(msisdn)s</msisdn>"
            "<starCode>%(star_code)s</starCode>"
            "<clientId>%(client_id)s</clientId>"
            "<phase>%(phase)s</phase>"
            "<msgtype>%(msg_type)s</msgtype>"
            "<dcs>%(dcs)s</dcs>"
            "<userdata>%(user_data)s</userdata>"
            "<EndofSession>%(end_of_session)s</EndofSession>"
        "</USSDRequest>"
    )
    _response_body = (
        "<USSDResponse>"
            "<requestId>%(request_id)s</requestId>"
            "<msisdn>%(msisdn)s</msisdn>"
            "<starCode>%(star_code)s</starCode>"
            "<clientId>%(client_id)s</clientId>"
            "<phase>%(phase)s</phase>"
            "<msgtype>%(msg_type)s</msgtype>"
            "<dcs>%(dcs)s</dcs>"
            "<userdata>%(user_data)s</userdata>"
            "<EndofSession>%(end_of_session)s</EndofSession>"
            "<delvrpt>%(delivery_report)s</delvrpt>"
        "</USSDResponse>"
    )

    _expected_outbound_payload = {
        'message_id': _params['request_id'],
        'content': _params['user_data'],
        'from_addr': _params['msisdn'],
        'to_addr': _params['star_code'],
        'session_event': TransportUserMessage.SESSION_RESUME,
        'transport_name': transport_name,
        'transport_type': 'ussd',
        'transport_metadata': {
            'mtn_nigeria_ussd': {
                'session_id': ("0" * _session_id_header_size),
                'clientId': _params['client_id'],
                'phase': _params['phase'],
                'dcs': _params['dcs'],
            },
        }
    }

    @inlineCallbacks
    def setUp(self):
        super(TestMtnNigeriaUssdTransportTestCase, self).setUp()

        deferred_server = self.start_server()
        config = {
            'transport_name': self.transport_name,
            'server_hostname': '127.0.0.1',
            'server_port': self.get_server_port(),
            'username': 'root',
            'password': 'toor',
            'application_id': '1029384756',
            'enquire_link_interval': 240,
            'timeout_period': 120,
            'user_termination_response': 'Bye',
        }
        self.transport = yield self.get_transport(config)
        yield deferred_server

        yield self.fake_login()
        self.client = self.transport.factory.client

    @inlineCallbacks
    def tearDown(self):
        yield self.transport.teardown_transport()
        yield self.stop_server()
        yield super(TestMtnNigeriaUssdTransportTestCase, self).tearDown()

    def fake_login(self):
        d = Deferred()

        def stubbed_login(self):
            self.authenticated = True
            d.callback(None)
        self.patch(self.transport.factory.protocol, 'login', stubbed_login)
        return d

    def mk_data_request_packet(self, session_id, **params):
        _params = self._params.copy()
        _params.update(params)
        return utils.mk_packet(session_id, self._request_body % _params)

    def mk_data_response_packet(self, session_id, **params):
        params.setdefault('msg_type', '2')
        params.setdefault('delivery_report', '0')
        _params = self._params.copy()
        _params.update(params)
        return utils.mk_packet(session_id, self._response_body % _params)

    def mk_error_response_packet(self, session_id, request_id, error_code):
        body = (
            "<USSDError>"
                "<requestId>%s</requestId>"
                "<errorCode>%s</errorCode>"
            "</USSDError>"
        % (request_id, error_code))
        return utils.mk_packet(session_id, body)

    def mk_session_id(self, nr):
        return str(nr).zfill(self._session_id_header_size)

    def send_request(self, session_id, **params):
        packet = self.mk_data_request_packet(session_id, **params)
        self.server.send_data(packet)

    def mk_msg(self, **fields):
        payload = self._expected_outbound_payload.copy()
        payload.update(fields)
        return TransportUserMessage(**payload)

    def mk_reply(self, request_msg, reply_content, continue_session=True):
        request_msg = TransportUserMessage(**request_msg.payload)
        return request_msg.reply(reply_content, continue_session)

    def assert_inbound_message(self, msg, **field_values):
        expected_payload = self._expected_outbound_payload.copy()
        expected_payload.update(field_values)

        for field, expected_value in expected_payload.iteritems():
            self.assertEqual(msg[field], expected_value)

    def assert_ack(self, ack, reply):
        self.assertEqual(ack.payload['event_type'], 'ack')
        self.assertEqual(ack.payload['user_message_id'], reply['message_id'])
        self.assertEqual(ack.payload['sent_message_id'], reply['message_id'])

    def assert_nack(self, nack, reply, reason):
        self.assertEqual(nack.payload['event_type'], 'nack')
        self.assertEqual(nack.payload['user_message_id'], reply['message_id'])
        self.assertEqual(nack.payload['nack_reason'], reason)

    @inlineCallbacks
    def test_inbound_begin(self):
        session_id = self.mk_session_id(0)
        request_content = "Who are you?"
        self.send_request(session_id, user_data=request_content)
        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assert_inbound_message(msg,
            session_event=TransportUserMessage.SESSION_NEW,
            content=request_content)

        reply_content = "We are the Knights Who Say ... Ni!"
        reply = self.mk_reply(msg, reply_content)
        self.dispatch(reply)
        response_packet = yield self.server.wait_for_data()
        self.assertEqual(
            response_packet,
            self.mk_data_response_packet(session_id, user_data=reply_content))

        [ack] = yield self.wait_for_dispatched_events(1)
        self.assert_ack(ack, reply)

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_end(self):
        session_id = self.mk_session_id(0)
        request_content = "Well, what is it you want?"
        self.send_request(
            session_id,
            user_data=request_content,
            msg_type=4,
            end_of_session=0)
        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assert_inbound_message(msg,
            session_event=TransportUserMessage.SESSION_RESUME,
            content=request_content)

        reply_content = "We want ... a shrubbery!"
        reply = self.mk_reply(msg, reply_content, continue_session=False)
        self.dispatch(reply)

        response_packet = yield self.server.wait_for_data()
        expected_response_packet = self.mk_data_response_packet(
            session_id,
            user_data=reply_content,
            msg_type=6,
            end_of_session=1)
        self.assertEqual(response_packet, expected_response_packet)

        [ack] = yield self.wait_for_dispatched_events(1)
        self.assert_ack(ack, reply)

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_resume(self):
        session_id = self.mk_session_id(0)
        request_content = "Well, what is it you want?"
        self.send_request(
            session_id,
            user_data=request_content,
            msg_type=4,
            end_of_session=0)
        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assert_inbound_message(msg,
            session_event=TransportUserMessage.SESSION_RESUME,
            content=request_content)

        reply_content = "We want ... a shrubbery!"
        reply = self.mk_reply(msg, reply_content, continue_session=True)
        self.dispatch(reply)

        response_packet = yield self.server.wait_for_data()
        expected_response_packet = self.mk_data_response_packet(
            session_id,
            user_data=reply_content,
            msg_type=2,
            end_of_session=0)
        self.assertEqual(response_packet, expected_response_packet)

        [ack] = yield self.wait_for_dispatched_events(1)
        self.assert_ack(ack, reply)

    @inlineCallbacks
    def test_user_terminated_session(self):
        session_id = self.mk_session_id(0)
        request_content = "I'm leaving now"
        self.send_request(
            session_id,
            user_data=request_content,
            msg_type=4,
            end_of_session=1)
        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assert_inbound_message(msg,
            session_event=TransportUserMessage.SESSION_CLOSE,
            content=request_content)

        response_packet = yield self.server.wait_for_data()
        expected_response_packet = self.mk_data_response_packet(
            session_id,
            user_data='Bye',
            msg_type=6,
            end_of_session=1)
        self.assertEqual(response_packet, expected_response_packet)

    @inlineCallbacks
    def test_outbound_response_failure(self):
        # stub the client to fake a response failure
        def stubbed_send_data_response(*a, **kw):
            raise XmlOverTcpError("Something bad happened")

        self.patch(self.client, 'send_data_response',
                   stubbed_send_data_response)

        reply = self.mk_reply(self.mk_msg(), "It's a trap!")
        self.dispatch(reply)

        [nack] = yield self.wait_for_dispatched_events(1)
        self.assert_nack(
            nack, reply, "Response failed: Something bad happened")

    @inlineCallbacks
    def test_outbound_metadata_fields_missing(self):
        reply = self.mk_reply(self.mk_msg(), "It's a trap!").copy()
        reply_metadata = reply['transport_metadata']['mtn_nigeria_ussd']
        del reply_metadata['clientId']
        self.dispatch(reply)

        response_packet = yield self.server.wait_for_data()
        expected_response_packet = self.mk_error_response_packet(
            reply_metadata['session_id'],
            reply['in_reply_to'],
            '208')
        self.assertEqual(response_packet, expected_response_packet)

        reason = "%s" % CodedXmlOverTcpError('208',
            "Required message transport metadata fields missing in "
            "outbound message: %s" % ['clientId'])

        [nack] = yield self.wait_for_dispatched_events(1)
        self.assert_nack(nack, reply, reason)
