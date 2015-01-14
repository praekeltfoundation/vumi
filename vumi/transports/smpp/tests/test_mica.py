from twisted.internet.defer import inlineCallbacks

from smpp.pdu_builder import DeliverSM

from vumi.message import TransportUserMessage

from vumi.transports.smpp.pdu_utils import (
    command_id, seq_no, pdu_tlv)
from vumi.transports.smpp.tests.test_smpp_transport import (
    SmppTransportTestCase)
from vumi.transports.smpp.smpp_transport import (
    SmppTransceiverTransport)
from vumi.transports.smpp.processors.mica import make_vumi_session_identifier


class MicaProcessorTestCase(SmppTransportTestCase):

    transport_class = SmppTransceiverTransport

    def setUp(self):
        super(MicaProcessorTestCase, self).setUp()
        self.default_config = {
            'transport_name': self.tx_helper.transport_name,
            'twisted_endpoint': 'tcp:host=127.0.0.1:port=0',
            'deliver_short_message_processor': (
                'vumi.transports.smpp.processors.mica.'
                'DeliverShortMessageProcessor'),
            'submit_short_message_processor': (
                'vumi.transports.smpp.processors.mica.'
                'SubmitShortMessageProcessor'),
            'system_id': 'foo',
            'password': 'bar',
            'deliver_short_message_processor_config': {
                'data_coding_overrides': {
                    0: 'utf-8',
                }
            }
        }

    @inlineCallbacks
    def test_submit_and_deliver_ussd_new(self):
        session_identifier = 12345
        smpp_helper = yield self.get_smpp_helper()

        # Server delivers a USSD message to the Client
        pdu = DeliverSM(1, short_message="*123#")
        pdu.add_optional_parameter('ussd_service_op', '01')
        pdu.add_optional_parameter('user_message_reference',
                                   session_identifier)

        yield smpp_helper.handle_pdu(pdu)

        [mess] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(mess['content'], None)
        self.assertEqual(mess['to_addr'], '*123#')
        self.assertEqual(mess['transport_type'], "ussd")
        self.assertEqual(mess['session_event'],
                         TransportUserMessage.SESSION_NEW)
        self.assertEqual(
            mess['transport_metadata'],
            {
                'session_info': {
                    'session_identifier': 12345,
                    'ussd_service_op': '01',
                }
            })

    @inlineCallbacks
    def test_deliver_sm_op_codes_new(self):
        session_identifier = 12345
        smpp_helper = yield self.get_smpp_helper()
        pdu = DeliverSM(1, short_message="*123#")
        pdu.add_optional_parameter('ussd_service_op', '01')
        pdu.add_optional_parameter('user_message_reference',
                                   session_identifier)
        yield smpp_helper.handle_pdu(pdu)
        [start] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(start['session_event'],
                         TransportUserMessage.SESSION_NEW)

    @inlineCallbacks
    def test_deliver_sm_op_codes_resume(self):
        source_addr = 'msisdn'
        session_identifier = 12345
        vumi_session_identifier = make_vumi_session_identifier(
            source_addr, session_identifier)

        smpp_helper = yield self.get_smpp_helper()
        deliver_sm_processor = smpp_helper.transport.deliver_sm_processor
        session_manager = deliver_sm_processor.session_manager

        yield session_manager.create_session(
            vumi_session_identifier, ussd_code='*123#')

        pdu = DeliverSM(1, short_message="", source_addr=source_addr)
        pdu.add_optional_parameter('ussd_service_op', '12')
        pdu.add_optional_parameter('user_message_reference',
                                   session_identifier)
        yield smpp_helper.handle_pdu(pdu)
        [resume] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(resume['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_deliver_sm_op_codes_end(self):
        source_addr = 'msisdn'
        session_identifier = 12345
        vumi_session_identifier = make_vumi_session_identifier(
            source_addr, session_identifier)

        smpp_helper = yield self.get_smpp_helper()
        deliver_sm_processor = smpp_helper.transport.deliver_sm_processor
        session_manager = deliver_sm_processor.session_manager

        yield session_manager.create_session(
            vumi_session_identifier, ussd_code='*123#')

        pdu = DeliverSM(1, short_message="", source_addr=source_addr)
        pdu.add_optional_parameter('ussd_service_op', '81')
        pdu.add_optional_parameter('user_message_reference',
                                   session_identifier)
        yield smpp_helper.handle_pdu(pdu)
        [end] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(end['session_event'],
                         TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def test_deliver_sm_unknown_op_code(self):
        session_identifier = 12345
        smpp_helper = yield self.get_smpp_helper()

        pdu = DeliverSM(1, short_message="*123#")
        pdu.add_optional_parameter('ussd_service_op', '01')
        pdu.add_optional_parameter('user_message_reference',
                                   session_identifier)

        yield smpp_helper.handle_pdu(pdu)

        pdu = DeliverSM(1, short_message="*123#")
        pdu.add_optional_parameter('ussd_service_op', '99')
        pdu.add_optional_parameter('user_message_reference',
                                   session_identifier)

        yield smpp_helper.handle_pdu(pdu)
        [start, unknown] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(unknown['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_submit_sm_op_codes_resume(self):
        user_msisdn = 'msisdn'
        session_identifier = 12345
        smpp_helper = yield self.get_smpp_helper()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world",
            transport_type="ussd",
            session_event=TransportUserMessage.SESSION_RESUME,
            transport_metadata={
                'session_info': {
                    'session_identifier': session_identifier
                }
            }, to_addr=user_msisdn)
        [resume] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(pdu_tlv(resume, 'ussd_service_op'), '02')

    @inlineCallbacks
    def test_submit_sm_op_codes_close(self):
        user_msisdn = 'msisdn'
        session_identifier = 12345
        smpp_helper = yield self.get_smpp_helper()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world",
            transport_type="ussd",
            session_event=TransportUserMessage.SESSION_CLOSE,
            transport_metadata={
                'session_info': {
                    'session_identifier': session_identifier
                }
            }, to_addr=user_msisdn)

        [close] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(pdu_tlv(close, 'ussd_service_op'), '17')

    @inlineCallbacks
    def test_submit_and_deliver_ussd_continue(self):
        user_msisdn = 'msisdn'
        session_identifier = 12345
        vumi_session_identifier = make_vumi_session_identifier(
            user_msisdn, session_identifier)
        smpp_helper = yield self.get_smpp_helper()

        deliver_sm_processor = smpp_helper.transport.deliver_sm_processor
        session_manager = deliver_sm_processor.session_manager
        yield session_manager.create_session(
            vumi_session_identifier, ussd_code='*123#')

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", transport_type="ussd", transport_metadata={
                'session_info': {
                    'session_identifier': session_identifier
                }
            }, to_addr=user_msisdn)

        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(command_id(submit_sm_pdu), 'submit_sm')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'ussd_service_op'), '02')
        self.assertEqual(
            pdu_tlv(submit_sm_pdu, 'user_message_reference'),
            session_identifier)

        # Server delivers a USSD message to the Client
        pdu = DeliverSM(seq_no(submit_sm_pdu) + 1, short_message="reply!",
                        source_addr=user_msisdn)
        # 0x12 is 'continue'
        pdu.add_optional_parameter('ussd_service_op', '12')
        pdu.add_optional_parameter('user_message_reference',
                                   session_identifier)

        yield smpp_helper.handle_pdu(pdu)

        [mess] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(mess['content'], "reply!")
        self.assertEqual(mess['transport_type'], "ussd")
        self.assertEqual(mess['to_addr'], '*123#')
        self.assertEqual(mess['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_submit_and_deliver_ussd_close(self):
        smpp_helper = yield self.get_smpp_helper()
        session_identifier = 12345

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", transport_type="ussd",
            session_event=TransportUserMessage.SESSION_CLOSE,
            transport_metadata={
                'session_info': {
                    'session_identifier': session_identifier
                }
            })

        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(command_id(submit_sm_pdu), 'submit_sm')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'ussd_service_op'), '17')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'user_message_reference'),
                         session_identifier)

    @inlineCallbacks
    def test_submit_sm_null_message(self):
        """
        We can successfully send a message with null content.
        """
        user_msisdn = 'msisdn'
        session_identifier = 12345
        smpp_helper = yield self.get_smpp_helper()

        yield self.tx_helper.make_dispatch_outbound(
            None,
            transport_type="ussd",
            session_event=TransportUserMessage.SESSION_RESUME,
            transport_metadata={
                'session_info': {
                    'session_identifier': session_identifier
                }
            }, to_addr=user_msisdn)
        [resume] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(pdu_tlv(resume, 'ussd_service_op'), '02')
