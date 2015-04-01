from twisted.internet.defer import inlineCallbacks

from smpp.pdu_builder import DeliverSM

from vumi.message import TransportUserMessage

from vumi.transports.smpp.pdu_utils import (
    command_id, seq_no, pdu_tlv, short_message)
from vumi.transports.smpp.tests.test_smpp_transport import (
    SmppTransportTestCase)
from vumi.transports.smpp.smpp_transport import (
    SmppTransceiverTransport)
from vumi.transports.smpp.processors.sixdee import make_vumi_session_identifier


class SessionInfo(object):
    """Helper for holding session ids."""

    def __init__(self, session_id=5678, addr='1234', continue_session=True):
        # all 6D session IDs are even by construction
        assert session_id % 2 == 0
        self.session_id = session_id
        self.addr = addr
        self.continue_session = continue_session

    @property
    def its_info(self):
        return "%04x" % (self.session_id | int(not self.continue_session))

    @property
    def vumi_id(self):
        return make_vumi_session_identifier(self.addr, self.sixdee_id)

    @property
    def sixdee_id(self):
        return "%04x" % self.session_id


class SixDeeProcessorTestCase(SmppTransportTestCase):

    transport_class = SmppTransceiverTransport

    def setUp(self):
        super(SixDeeProcessorTestCase, self).setUp()
        self.default_config = {
            'transport_name': self.tx_helper.transport_name,
            'twisted_endpoint': 'tcp:host=127.0.0.1:port=0',
            'deliver_short_message_processor': (
                'vumi.transports.smpp.processors.sixdee.'
                'DeliverShortMessageProcessor'),
            'submit_short_message_processor': (
                'vumi.transports.smpp.processors.sixdee.'
                'SubmitShortMessageProcessor'),
            'system_id': 'foo',
            'password': 'bar',
            'deliver_short_message_processor_config': {
                'data_coding_overrides': {
                    0: 'utf-8',
                }
            },
            'submit_short_message_processor_config': {
                'submit_sm_encoding': 'utf-16be',
                'submit_sm_data_coding': 8,
                'send_multipart_udh': True,
            }
        }

    def assert_udh_parts(self, pdus, texts, encoding):
        pdu_header = lambda pdu: short_message(pdu)[:6]
        pdu_text = lambda pdu: short_message(pdu)[6:].decode(encoding)
        udh_header = lambda i: '\x05\x00\x03\x03\x07' + chr(i)
        self.assertEqual(
            [(pdu_header(pdu), pdu_text(pdu)) for pdu in pdus],
            [(udh_header(i + 1), text) for i, text in enumerate(texts)])

    @inlineCallbacks
    def test_submit_sm_multipart_udh_ucs2(self):
        message = (
            "A cup is a small, open container used for carrying and "
            "drinking drinks. It may be made of wood, plastic, glass, "
            "clay, metal, stone, china or other materials, and may have "
            "a stem, handles or other adornments. Cups are used for "
            "drinking across a wide range of cultures and social classes, "
            "and different styles of cups may be used for different liquids "
            "or in different situations. Cups have been used for thousands "
            "of years for the ...Reply 1 for more")

        smpp_helper = yield self.get_smpp_helper()
        yield self.tx_helper.make_dispatch_outbound(message, to_addr='msisdn')
        pdus = yield smpp_helper.wait_for_pdus(7)
        self.assert_udh_parts(pdus, [
            ("A cup is a small, open container used"
             " for carrying and drinking d"),
            ("rinks. It may be made of wood, plastic,"
             " glass, clay, metal, stone"),
            (", china or other materials, and may have"
             " a stem, handles or other"),
            (" adornments. Cups are used for drinking"
             " across a wide range of cu"),
            ("ltures and social classes, and different"
             " styles of cups may be us"),
            ("ed for different liquids or in different"
             " situations. Cups have be"),
            ("en used for thousands of years for the ...Reply 1 for more"),
        ], encoding='utf-16be')  # utf-16be is close enough to UCS2
        for pdu in pdus:
            self.assertTrue(len(short_message(pdu)) < 140)

    @inlineCallbacks
    def test_submit_and_deliver_ussd_new(self):
        session = SessionInfo()
        smpp_helper = yield self.get_smpp_helper()

        # Server delivers a USSD message to the Client
        pdu = DeliverSM(1, short_message="*123#")
        pdu.add_optional_parameter('ussd_service_op', '01')
        pdu.add_optional_parameter('its_session_info', session.its_info)

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
                    'session_identifier': session.sixdee_id,
                    'ussd_service_op': '01',
                }
            })

    @inlineCallbacks
    def test_deliver_sm_op_codes_new(self):
        session = SessionInfo()
        smpp_helper = yield self.get_smpp_helper()
        pdu = DeliverSM(1, short_message="*123#")
        pdu.add_optional_parameter('ussd_service_op', '01')
        pdu.add_optional_parameter('its_session_info', session.its_info)
        yield smpp_helper.handle_pdu(pdu)
        [start] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(start['session_event'],
                         TransportUserMessage.SESSION_NEW)

    @inlineCallbacks
    def test_deliver_sm_op_codes_resume(self):
        session = SessionInfo()
        smpp_helper = yield self.get_smpp_helper()
        deliver_sm_processor = smpp_helper.transport.deliver_sm_processor
        session_manager = deliver_sm_processor.session_manager

        yield session_manager.create_session(
            session.vumi_id, ussd_code='*123#')

        pdu = DeliverSM(1, short_message="", source_addr=session.addr)
        pdu.add_optional_parameter('ussd_service_op', '12')
        pdu.add_optional_parameter('its_session_info', session.its_info)
        yield smpp_helper.handle_pdu(pdu)
        [resume] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(resume['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_deliver_sm_op_codes_end(self):
        session = SessionInfo()
        smpp_helper = yield self.get_smpp_helper()
        deliver_sm_processor = smpp_helper.transport.deliver_sm_processor
        session_manager = deliver_sm_processor.session_manager

        yield session_manager.create_session(
            session.vumi_id, ussd_code='*123#')

        pdu = DeliverSM(1, short_message="", source_addr=session.addr)
        pdu.add_optional_parameter('ussd_service_op', '81')
        pdu.add_optional_parameter('its_session_info', session.its_info)
        yield smpp_helper.handle_pdu(pdu)
        [end] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(end['session_event'],
                         TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def test_deliver_sm_unknown_op_code(self):
        session = SessionInfo()
        smpp_helper = yield self.get_smpp_helper()

        pdu = DeliverSM(1, short_message="*123#")
        pdu.add_optional_parameter('ussd_service_op', '01')
        pdu.add_optional_parameter('its_session_info', session.its_info)

        yield smpp_helper.handle_pdu(pdu)

        pdu = DeliverSM(1, short_message="*123#")
        pdu.add_optional_parameter('ussd_service_op', '99')
        pdu.add_optional_parameter('its_session_info', session.its_info)

        yield smpp_helper.handle_pdu(pdu)
        [start, unknown] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(unknown['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_submit_sm_op_codes_resume(self):
        session = SessionInfo()
        smpp_helper = yield self.get_smpp_helper()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world",
            transport_type="ussd",
            session_event=TransportUserMessage.SESSION_RESUME,
            transport_metadata={
                'session_info': {
                    'session_identifier': session.sixdee_id,
                }
            }, to_addr=session.addr)
        [resume] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(pdu_tlv(resume, 'ussd_service_op'), '02')
        self.assertEqual(pdu_tlv(resume, 'its_session_info'), session.its_info)

    @inlineCallbacks
    def test_submit_sm_op_codes_close(self):
        session = SessionInfo(continue_session=False)
        smpp_helper = yield self.get_smpp_helper()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world",
            transport_type="ussd",
            session_event=TransportUserMessage.SESSION_CLOSE,
            transport_metadata={
                'session_info': {
                    'session_identifier': session.sixdee_id,
                }
            }, to_addr=session.addr)

        [close] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(pdu_tlv(close, 'ussd_service_op'), '17')
        self.assertEqual(pdu_tlv(close, 'its_session_info'), session.its_info)

    @inlineCallbacks
    def test_submit_and_deliver_ussd_continue(self):
        session = SessionInfo()
        smpp_helper = yield self.get_smpp_helper()

        deliver_sm_processor = smpp_helper.transport.deliver_sm_processor
        session_manager = deliver_sm_processor.session_manager
        yield session_manager.create_session(
            session.vumi_id, ussd_code='*123#')

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", transport_type="ussd", transport_metadata={
                'session_info': {
                    'session_identifier': session.sixdee_id,
                }
            }, to_addr=session.addr)

        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(command_id(submit_sm_pdu), 'submit_sm')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'ussd_service_op'), '02')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'its_session_info'),
                         session.its_info)

        # Server delivers a USSD message to the Client
        pdu = DeliverSM(seq_no(submit_sm_pdu) + 1, short_message="reply!",
                        source_addr=session.addr)
        # 0x12 is 'continue'
        pdu.add_optional_parameter('ussd_service_op', '12')
        pdu.add_optional_parameter('its_session_info', session.its_info)

        yield smpp_helper.handle_pdu(pdu)

        [mess] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(mess['content'], "reply!")
        self.assertEqual(mess['transport_type'], "ussd")
        self.assertEqual(mess['to_addr'], '*123#')
        self.assertEqual(mess['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_submit_and_deliver_ussd_close(self):
        session = SessionInfo(continue_session=False)
        smpp_helper = yield self.get_smpp_helper()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", transport_type="ussd",
            session_event=TransportUserMessage.SESSION_CLOSE,
            transport_metadata={
                'session_info': {
                    'session_identifier': session.sixdee_id,
                }
            })

        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(command_id(submit_sm_pdu), 'submit_sm')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'ussd_service_op'), '17')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'its_session_info'),
                         session.its_info)

    @inlineCallbacks
    def test_submit_sm_null_message(self):
        """
        We can successfully send a message with null content.
        """
        session = SessionInfo()
        smpp_helper = yield self.get_smpp_helper()

        yield self.tx_helper.make_dispatch_outbound(
            None,
            transport_type="ussd",
            session_event=TransportUserMessage.SESSION_RESUME,
            transport_metadata={
                'session_info': {
                    'session_identifier': session.sixdee_id,
                }
            }, to_addr=session.addr)
        [resume] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(pdu_tlv(resume, 'ussd_service_op'), '02')
        self.assertEqual(pdu_tlv(resume, 'its_session_info'), session.its_info)
