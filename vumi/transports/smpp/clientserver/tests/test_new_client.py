from twisted.test import proto_helpers
from twisted.internet.defer import succeed
from twisted.internet.error import ConnectionDone
from twisted.internet.task import Clock

from vumi.tests.helpers import VumiTestCase, PersistenceHelper
from vumi.transports.smpp.transport import SmppTransport
from vumi.transports.smpp.clientserver.new_client import (
    EsmeTransceiver, EsmeTransceiverFactory, seq_no, command_status)

from smpp.pdu import unpack_pdu
from smpp.pdu_builder import BindTransceiverResp, Unbind, UnbindResp


def sequence_generator():
    counter = 0
    while True:
        yield succeed(counter)
        counter = counter + 1


class EsmeTestCase(VumiTestCase):

    PROTOCOL_CLASS = EsmeTransceiver

    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.redis = self.persistence_helper.get_redis_manager()
        self.clock = Clock()
        self.patch(EsmeTransceiver, 'clock', self.clock)

    def get_protocol(self, config={}, sm_processor=None, dr_processor=None):

        default_config = {
            'transport_name': 'sphex_transport',
            'twisted_endpoint': 'tcp:host=localhost:port=0',
            'system_id': 'system_id',
            'password': 'password',
            'smpp_bind_timeout': 30,
        }
        default_config.update(config)
        cfg = SmppTransport.CONFIG_CLASS(default_config, static=True)
        if sm_processor is None:
            sm_processor = cfg.short_message_processor(
                self.redis, None, cfg.short_message_processor_config)
        if dr_processor is None:
            dr_processor = cfg.delivery_report_processor(
                self.redis, None, cfg.delivery_report_processor_config)

        factory = EsmeTransceiverFactory(
            cfg, sm_processor, dr_processor, sequence_generator())
        proto = factory.buildProtocol(('127.0.0.1', 0))
        self.add_cleanup(proto.connectionLost, reason=ConnectionDone)
        return proto

    def connect_transport(self, protocol):
        transport = proto_helpers.StringTransport()
        protocol.makeConnection(transport)
        return transport

    def assertCommand(self, pdu, command_id, sequence_number=None,
                      status=None):
        self.assertEqual(pdu['header']['command_id'], command_id)
        if sequence_number is not None:
            self.assertEqual(seq_no(pdu), sequence_number)
        if status is not None:
            self.assertEqual(command_status(pdu), status)

    def bind_protocol(self, transport, protocol):
        bind_pdu = unpack_pdu(transport.value())
        transport.clear()
        protocol.dataReceived(
            BindTransceiverResp(seq_no(bind_pdu)).get_bin())
        return bind_pdu

    def setup_bind(self, clear=True):
        protocol = self.get_protocol()
        transport = self.connect_transport(protocol)
        self.bind_protocol(transport, protocol)
        if clear:
            transport.clear()
        return transport, protocol

    def test_on_connection_made(self):
        protocol = self.get_protocol()
        self.assertEqual(protocol.state, EsmeTransceiver.CLOSED_STATE)
        transport = self.connect_transport(protocol)
        self.assertEqual(protocol.state, EsmeTransceiver.OPEN_STATE)
        bind_pdu = unpack_pdu(transport.value())
        self.assertEqual(
            bind_pdu['body']['mandatory_parameters'],
            {
                'addr_npi': 'unknown',
                'interface_version': '34',
                'addr_ton': 'unknown',
                'address_range': '',
                'system_id': 'system_id',
                'system_type': '',
                'password': 'password',
            })
        self.assertEqual(
            bind_pdu['header'], {
                'command_status': 'ESME_ROK',
                'command_length': 40,
                'sequence_number': 0,
                'command_id': 'bind_transceiver',
            })

    def test_drop_link(self):
        protocol = self.get_protocol()
        transport = self.connect_transport(protocol)
        self.assertFalse(protocol.isBound())
        self.assertEqual(protocol.state, EsmeTransceiver.OPEN_STATE)
        self.assertFalse(transport.disconnecting)
        self.clock.advance(protocol.config.smpp_bind_timeout + 1)
        self.assertTrue(transport.disconnecting)

    def test_on_smpp_bind(self):
        protocol = self.get_protocol()
        transport = self.connect_transport(protocol)
        bind_pdu = self.bind_protocol(transport, protocol)
        self.assertEqual(protocol.state, EsmeTransceiver.BOUND_STATE_TRX)
        self.assertTrue(protocol.isBound())
        self.assertTrue(protocol.enquire_link_call.running)
        enquire_link_pdu = unpack_pdu(transport.value())
        self.assertCommand(enquire_link_pdu,
                           command_id='enquire_link', sequence_number=1,
                           status='ESME_ROK')

    def test_unbind(self):
        transport, protocol = self.setup_bind()
        protocol.dataReceived(Unbind(sequence_number=0).get_bin())
        pdu = unpack_pdu(transport.value())
        self.assertCommand(
            pdu, command_id='unbind_resp', status='ESME_ROK',
            sequence_number=0)
