# -*- coding: utf-8 -*-
from twisted.test import proto_helpers
from twisted.internet import reactor
from twisted.internet.defer import (
    inlineCallbacks, gatherResults, Deferred, returnValue, succeed)
from twisted.internet.error import ConnectionDone
from twisted.internet.task import Clock


from vumi.tests.helpers import VumiTestCase, PersistenceHelper
from vumi.transports.smpp.smpp_transport import SmppTransceiverTransport
from vumi.transports.smpp.protocol import (
    EsmeTransceiver, EsmeTransceiverFactory,
    EsmeTransmitterFactory, EsmeReceiverFactory)
from vumi.transports.smpp.processors import DeliverShortMessageProcessor
from vumi.transports.smpp.pdu_utils import (
    seq_no, command_status, command_id, chop_pdu_stream, short_message)
from vumi.transports.smpp.smpp_utils import unpacked_pdu_opts
from vumi.transports.smpp.sequence import RedisSequence
from vumi.transports.tests.helpers import TransportHelper

from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (
    Unbind, UnbindResp,
    BindTransceiver, BindTransceiverResp,
    BindTransmitter, BindTransmitterResp,
    BindReceiver, BindReceiverResp,
    SubmitSMResp,
    DeliverSM,
    EnquireLink, EnquireLinkResp)


def connect_transport(protocol, system_id='', password='', system_type=''):
    transport = proto_helpers.StringTransport()
    protocol.makeConnection(transport)
    d = protocol.bind(system_id=system_id, password=password,
                      system_type=system_type)
    d.addCallback(lambda _: transport)
    return d


@inlineCallbacks
def bind_protocol(transport, protocol, clear=True, bind_pdu=None):
    if bind_pdu is None:
        [bind_pdu] = yield wait_for_pdus(transport, 1)
    resp_pdu_class = {
        BindTransceiver: BindTransceiverResp,
        BindReceiver: BindReceiverResp,
        BindTransmitter: BindTransmitterResp,
    }.get(protocol.bind_pdu)
    protocol.dataReceived(
        resp_pdu_class(seq_no(bind_pdu)).get_bin())
    [enquire_link] = yield wait_for_pdus(transport, 1)
    protocol.dataReceived(
        EnquireLinkResp(seq_no(enquire_link)).get_bin())
    if clear:
        transport.clear()
    returnValue(bind_pdu)


def wait_for_pdus(transport, count):
    d = Deferred()

    def cb(pdus):
        data_stream = transport.value()
        pdu_found = chop_pdu_stream(data_stream)
        if pdu_found is not None:
            pdu_data, remainder = pdu_found
            pdu = unpack_pdu(pdu_data)
            pdus.append(pdu)
            transport.clear()
            transport.write(remainder)

        if len(pdus) == count:
            d.callback(pdus)
        else:
            reactor.callLater(0, cb, pdus)

    cb([])

    return d


class ForwardableRedisSequence(RedisSequence):

    def advance(self, seq_nr):
        d = self.redis.set('smpp_last_sequence_number', seq_nr)
        d.addCallback(lambda _: seq_nr)
        return d


class DummySmppTransport(SmppTransceiverTransport):

    sequence_class = ForwardableRedisSequence


class EsmeTestCase(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(DummySmppTransport))
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.redis = yield self.persistence_helper.get_redis_manager()
        self.clock = Clock()
        self.patch(EsmeTransceiver, 'clock', self.clock)

    @inlineCallbacks
    def get_protocol(self, config={},
                     deliver_sm_processor=None, dr_processor=None,
                     factory_class=None):

        factory_class = factory_class or EsmeTransceiverFactory

        default_config = {
            'transport_name': 'sphex_transport',
            'twisted_endpoint': 'tcp:host=127.0.0.1:port=0',
            'system_id': 'system_id',
            'password': 'password',
            'smpp_bind_timeout': 30,
        }

        if deliver_sm_processor:
            default_config['deliver_short_message_processor'] = (
                deliver_sm_processor)

        if dr_processor:
            default_config['delivery_report_processor'] = (
                dr_processor)

        default_config.update(config)

        smpp_transport = yield self.tx_helper.get_transport(default_config)

        factory = factory_class(smpp_transport)
        proto = factory.buildProtocol(('127.0.0.1', 0))
        self.add_cleanup(proto.connectionLost, reason=ConnectionDone)
        returnValue(proto)

    def assertCommand(self, pdu, cmd_id, sequence_number=None,
                      status=None, params={}):
        self.assertEqual(command_id(pdu), cmd_id)
        if sequence_number is not None:
            self.assertEqual(seq_no(pdu), sequence_number)
        if status is not None:
            self.assertEqual(command_status(pdu), status)

        pdu_params = {}
        if params:
            if 'body' not in pdu:
                raise Exception('Body does not have parameters.')

            mandatory_parameters = pdu['body']['mandatory_parameters']
            for key in params:
                if key in mandatory_parameters:
                    pdu_params[key] = mandatory_parameters[key]

            self.assertEqual(params, pdu_params)

    @inlineCallbacks
    def setup_bind(self, config={}, clear=True, factory_class=None):
        protocol = yield self.get_protocol(config, factory_class=factory_class)
        transport = yield connect_transport(protocol)
        yield bind_protocol(transport, protocol, clear=clear)
        returnValue((transport, protocol))

    def lookup_message_ids(self, protocol, seq_nums):
        message_stash = protocol.vumi_transport.message_stash
        lookup_func = message_stash.get_sequence_number_message_id
        return gatherResults([lookup_func(seq_num) for seq_num in seq_nums])

    @inlineCallbacks
    def test_on_connection_made(self):
        protocol = yield self.get_protocol()
        self.assertEqual(protocol.state, EsmeTransceiver.CLOSED_STATE)
        transport = yield connect_transport(
            protocol, system_id='system_id', password='password')
        self.assertEqual(protocol.state, EsmeTransceiver.OPEN_STATE)
        [bind_pdu] = yield wait_for_pdus(transport, 1)
        self.assertCommand(
            bind_pdu,
            'bind_transceiver',
            sequence_number=1,
            params={
                'system_id': 'system_id',
                'password': 'password',
            })

    @inlineCallbacks
    def test_drop_link(self):
        protocol = yield self.get_protocol()
        transport = yield connect_transport(protocol)
        [bind_pdu] = yield wait_for_pdus(transport, 1)
        self.assertCommand(bind_pdu, 'bind_transceiver')
        self.assertFalse(protocol.is_bound())
        self.assertEqual(protocol.state, EsmeTransceiver.OPEN_STATE)
        self.assertFalse(transport.disconnecting)
        self.clock.advance(protocol.config.smpp_bind_timeout + 1)
        [unbind_pdu] = yield wait_for_pdus(transport, 1)
        self.assertCommand(unbind_pdu, 'unbind')
        unbind_resp_pdu = UnbindResp(sequence_number=seq_no(unbind_pdu))
        yield protocol.on_pdu(unpack_pdu(unbind_resp_pdu.get_bin()))
        self.assertTrue(transport.disconnecting)

    @inlineCallbacks
    def test_on_smpp_bind(self):
        protocol = yield self.get_protocol()
        transport = yield connect_transport(protocol)
        yield bind_protocol(transport, protocol)
        self.assertEqual(protocol.state, EsmeTransceiver.BOUND_STATE_TRX)
        self.assertTrue(protocol.is_bound())
        self.assertTrue(protocol.enquire_link_call.running)

    @inlineCallbacks
    def test_handle_unbind(self):
        transport, protocol = yield self.setup_bind()
        protocol.dataReceived(Unbind(sequence_number=0).get_bin())
        [pdu] = yield wait_for_pdus(transport, 1)
        self.assertCommand(pdu, 'unbind_resp',
                           sequence_number=0, status='ESME_ROK')

    @inlineCallbacks
    def test_on_submit_sm_resp(self):
        calls = []
        self.patch(EsmeTransceiver, 'on_submit_sm_resp',
                   lambda p, *a: calls.append(a))
        transport, protocol = yield self.setup_bind()
        pdu = SubmitSMResp(sequence_number=0, message_id='foo')
        protocol.dataReceived(pdu.get_bin())
        self.assertEqual(calls, [(0, 'foo', 'ESME_ROK')])

    @inlineCallbacks
    def test_deliver_sm(self):
        calls = []
        self.patch(EsmeTransceiver, 'handle_deliver_sm',
                   lambda p, pdu: succeed(calls.append(pdu)))
        transport, protocol = yield self.setup_bind()
        pdu = DeliverSM(
            sequence_number=0, message_id='foo', short_message='bar')
        protocol.dataReceived(pdu.get_bin())
        [deliver_sm] = calls
        self.assertCommand(deliver_sm, 'deliver_sm', sequence_number=0)

    @inlineCallbacks
    def test_deliver_sm_fail(self):
        transport, protocol = yield self.setup_bind()
        pdu = DeliverSM(
            sequence_number=0, message_id='foo', data_coding=4,
            short_message='string with unknown data coding')
        protocol.dataReceived(pdu.get_bin())
        [deliver_sm_resp] = yield wait_for_pdus(transport, 1)
        self.assertCommand(
            deliver_sm_resp, 'deliver_sm_resp', sequence_number=0,
            status='ESME_RDELIVERYFAILURE')

    @inlineCallbacks
    def test_deliver_sm_fail_with_custom_error(self):
        transport, protocol = yield self.setup_bind(config={
            "deliver_sm_decoding_error": "ESME_RSYSERR"
        })
        pdu = DeliverSM(
            sequence_number=0, message_id='foo', data_coding=4,
            short_message='string with unknown data coding')
        protocol.dataReceived(pdu.get_bin())
        [deliver_sm_resp] = yield wait_for_pdus(transport, 1)
        self.assertCommand(
            deliver_sm_resp, 'deliver_sm_resp', sequence_number=0,
            status='ESME_RSYSERR')

    @inlineCallbacks
    def test_on_enquire_link(self):
        transport, protocol = yield self.setup_bind()
        pdu = EnquireLink(sequence_number=0)
        protocol.dataReceived(pdu.get_bin())
        [enquire_link_resp] = yield wait_for_pdus(transport, 1)
        self.assertCommand(
            enquire_link_resp, 'enquire_link_resp', sequence_number=0,
            status='ESME_ROK')

    @inlineCallbacks
    def test_on_enquire_link_resp(self):
        calls = []
        self.patch(EsmeTransceiver, 'handle_enquire_link_resp',
                   lambda p, pdu: calls.append(pdu))
        transport, protocol = yield self.setup_bind()
        [pdu] = calls
        # bind_transceiver is sequence_number 1
        self.assertEqual(seq_no(pdu), 2)
        self.assertEqual(command_id(pdu), 'enquire_link_resp')

    @inlineCallbacks
    def test_enquire_link_no_response(self):
        transport, protocol = yield self.setup_bind(clear=False)
        protocol.clock.advance(protocol.idle_timeout)
        [unbind_pdu] = yield wait_for_pdus(transport, 1)
        self.assertCommand(unbind_pdu, 'unbind')
        self.clock.advance(protocol.unbind_timeout)
        self.assertTrue(transport.disconnecting)

    @inlineCallbacks
    def test_enquire_link_looping(self):
        transport, protocol = yield self.setup_bind(clear=False)
        enquire_link_resp = EnquireLinkResp(1)

        protocol.clock.advance(protocol.idle_timeout - 1)
        protocol.dataReceived(enquire_link_resp.get_bin())

        protocol.clock.advance(protocol.idle_timeout - 1)
        self.assertFalse(transport.disconnecting)
        protocol.clock.advance(1)

        [unbind_pdu] = yield wait_for_pdus(transport, 1)
        self.assertCommand(unbind_pdu, 'unbind')
        unbind_resp_pdu = UnbindResp(sequence_number=seq_no(unbind_pdu))
        yield protocol.on_pdu(unpack_pdu(unbind_resp_pdu.get_bin()))
        self.assertTrue(transport.disconnecting)

    @inlineCallbacks
    def test_submit_sm(self):
        transport, protocol = yield self.setup_bind()
        seq_nums = yield protocol.submit_sm(
            'abc123', 'dest_addr', short_message='foo')
        [submit_sm] = yield wait_for_pdus(transport, 1)
        self.assertCommand(submit_sm, 'submit_sm', params={
            'short_message': 'foo',
        })
        stored_ids = yield self.lookup_message_ids(protocol, seq_nums)
        self.assertEqual(['abc123'], stored_ids)

    @inlineCallbacks
    def test_submit_sm_configured_parameters(self):
        transport, protocol = yield self.setup_bind({
            'service_type': 'stype',
            'source_addr_ton': 2,
            'source_addr_npi': 2,
            'dest_addr_ton': 2,
            'dest_addr_npi': 2,
            'registered_delivery': 0,
        })
        seq_nums = yield protocol.submit_sm(
            'abc123', 'dest_addr', short_message='foo')
        [submit_sm] = yield wait_for_pdus(transport, 1)
        self.assertCommand(submit_sm, 'submit_sm', params={
            'short_message': 'foo',
            'service_type': 'stype',
            'source_addr_ton': 'national',  # replaced by unpack_pdu()
            'source_addr_npi': 2,
            'dest_addr_ton': 'national',  # replaced by unpack_pdu()
            'dest_addr_npi': 2,
            'registered_delivery': 0,
        })
        stored_ids = yield self.lookup_message_ids(protocol, seq_nums)
        self.assertEqual(['abc123'], stored_ids)

    @inlineCallbacks
    def test_submit_sm_long(self):
        transport, protocol = yield self.setup_bind()
        long_message = 'This is a long message.' * 20
        seq_nums = yield protocol.submit_sm_long(
            'abc123', 'dest_addr', long_message)
        [submit_sm] = yield wait_for_pdus(transport, 1)
        pdu_opts = unpacked_pdu_opts(submit_sm)

        self.assertEqual('submit_sm', submit_sm['header']['command_id'])
        self.assertEqual(
            None, submit_sm['body']['mandatory_parameters']['short_message'])
        self.assertEqual(''.join('%02x' % ord(c) for c in long_message),
                         pdu_opts['message_payload'])
        stored_ids = yield self.lookup_message_ids(protocol, seq_nums)
        self.assertEqual(['abc123'], stored_ids)

    @inlineCallbacks
    def test_submit_sm_multipart_udh(self):
        transport, protocol = yield self.setup_bind(config={
            'send_multipart_udh': True,
        })
        long_message = 'This is a long message.' * 20
        seq_numbers = yield protocol.submit_csm_udh(
            'abc123', 'dest_addr', short_message=long_message)
        pdus = yield wait_for_pdus(transport, 4)
        self.assertEqual(len(seq_numbers), 4)

        msg_parts = []
        msg_refs = []

        for i, sm in enumerate(pdus):
            mandatory_parameters = sm['body']['mandatory_parameters']
            self.assertEqual('submit_sm', sm['header']['command_id'])
            msg = mandatory_parameters['short_message']

            udh_hlen, udh_tag, udh_len, udh_ref, udh_tot, udh_seq = [
                ord(octet) for octet in msg[:6]]
            self.assertEqual(5, udh_hlen)
            self.assertEqual(0, udh_tag)
            self.assertEqual(3, udh_len)
            msg_refs.append(udh_ref)
            self.assertEqual(4, udh_tot)
            self.assertEqual(i + 1, udh_seq)
            self.assertTrue(len(msg) <= 136)
            msg_parts.append(msg[6:])
            self.assertEqual(0x40, mandatory_parameters['esm_class'])

        self.assertEqual(long_message, ''.join(msg_parts))
        self.assertEqual(1, len(set(msg_refs)))

        stored_ids = yield self.lookup_message_ids(protocol, seq_numbers)
        self.assertEqual(['abc123'] * len(seq_numbers), stored_ids)

    @inlineCallbacks
    def test_udh_ref_num_limit(self):
        transport, protocol = yield self.setup_bind(config={
            'send_multipart_udh': True,
        })

        # forward until we go past 0xFF
        yield protocol.sequence_generator.advance(0xFF)

        long_message = 'This is a long message.' * 20
        seq_numbers = yield protocol.submit_csm_udh(
            'abc123', 'dest_addr', short_message=long_message)
        pdus = yield wait_for_pdus(transport, 4)

        self.assertEqual(len(seq_numbers), 4)
        self.assertTrue(all([sn > 0xFF for sn in seq_numbers]))

        msg_refs = []

        for pdu in pdus:
            msg = short_message(pdu)
            _, _, _, udh_ref, _, _ = [ord(octet) for octet in msg[:6]]
            msg_refs.append(udh_ref)

        self.assertEqual(1, len(set(msg_refs)))
        self.assertTrue(all([msg_ref < 0xFF for msg_ref in msg_refs]))

    @inlineCallbacks
    def test_submit_sm_multipart_sar(self):
        transport, protocol = yield self.setup_bind(config={
            'send_multipart_sar': True,
        })
        long_message = 'This is a long message.' * 20
        seq_nums = yield protocol.submit_csm_sar(
            'abc123', 'dest_addr', short_message=long_message)
        pdus = yield wait_for_pdus(transport, 4)
        # seq no 1 == bind_transceiver, 2 == enquire_link, 3 == sar_msg_ref_num
        self.assertEqual([4, 5, 6, 7], seq_nums)
        msg_parts = []
        msg_refs = []

        for i, sm in enumerate(pdus):
            pdu_opts = unpacked_pdu_opts(sm)
            mandatory_parameters = sm['body']['mandatory_parameters']

            self.assertEqual('submit_sm', sm['header']['command_id'])
            msg_parts.append(mandatory_parameters['short_message'])
            self.assertTrue(len(mandatory_parameters['short_message']) <= 130)
            msg_refs.append(pdu_opts['sar_msg_ref_num'])
            self.assertEqual(i + 1, pdu_opts['sar_segment_seqnum'])
            self.assertEqual(4, pdu_opts['sar_total_segments'])

        self.assertEqual(long_message, ''.join(msg_parts))
        self.assertEqual([3, 3, 3, 3], msg_refs)

        stored_ids = yield self.lookup_message_ids(protocol, seq_nums)
        self.assertEqual(['abc123'] * len(seq_nums), stored_ids)

    @inlineCallbacks
    def test_sar_ref_num_limit(self):
        transport, protocol = yield self.setup_bind(config={
            'send_multipart_udh': True,
        })

        # forward until we go past 0xFFFF
        yield protocol.sequence_generator.advance(0xFFFF)

        long_message = 'This is a long message.' * 20
        seq_numbers = yield protocol.submit_csm_udh(
            'abc123', 'dest_addr', short_message=long_message)
        pdus = yield wait_for_pdus(transport, 4)

        self.assertEqual(len(seq_numbers), 4)
        self.assertTrue(all([sn > 0xFF for sn in seq_numbers]))

        msg_refs = []

        for pdu in pdus:
            msg = short_message(pdu)
            _, _, _, udh_ref, _, _ = [ord(octet) for octet in msg[:6]]
            msg_refs.append(udh_ref)

        self.assertEqual(1, len(set(msg_refs)))
        self.assertTrue(all([msg_ref < 0xFFFF for msg_ref in msg_refs]))

    @inlineCallbacks
    def test_query_sm(self):
        transport, protocol = yield self.setup_bind()
        yield protocol.query_sm('foo', source_addr='bar')
        [query_sm] = yield wait_for_pdus(transport, 1)
        self.assertCommand(query_sm, 'query_sm', params={
            'message_id': 'foo',
            'source_addr': 'bar',
        })

    @inlineCallbacks
    def test_unbind(self):
        calls = []
        self.patch(EsmeTransceiver, 'handle_unbind_resp',
                   lambda p, pdu: calls.append(pdu))
        transport, protocol = yield self.setup_bind()
        yield protocol.unbind()
        [unbind_pdu] = yield wait_for_pdus(transport, 1)
        protocol.dataReceived(UnbindResp(seq_no(unbind_pdu)).get_bin())
        [unbind_resp_pdu] = calls
        self.assertEqual(seq_no(unbind_resp_pdu), seq_no(unbind_pdu))

    @inlineCallbacks
    def test_bind_transmitter(self):
        transport, protocol = yield self.setup_bind(
            factory_class=EsmeTransmitterFactory)
        self.assertTrue(protocol.is_bound())
        self.assertEqual(protocol.state, protocol.BOUND_STATE_TX)

    @inlineCallbacks
    def test_bind_receiver(self):
        transport, protocol = yield self.setup_bind(
            factory_class=EsmeReceiverFactory)
        self.assertTrue(protocol.is_bound())
        self.assertEqual(protocol.state, protocol.BOUND_STATE_RX)

    @inlineCallbacks
    def test_partial_pdu_data_received(self):
        calls = []
        self.patch(EsmeTransceiver, 'handle_deliver_sm',
                   lambda p, pdu: calls.append(pdu))
        transport, protocol = yield self.setup_bind()
        deliver_sm = DeliverSM(sequence_number=1, short_message='foo')
        pdu = deliver_sm.get_bin()
        half = len(pdu) / 2
        pdu_part1, pdu_part2 = pdu[:half], pdu[half:]
        protocol.dataReceived(pdu_part1)
        self.assertEqual([], calls)
        protocol.dataReceived(pdu_part2)
        [handled_pdu] = calls
        self.assertEqual(command_id(handled_pdu), 'deliver_sm')
        self.assertEqual(seq_no(handled_pdu), 1)
        self.assertEqual(short_message(handled_pdu), 'foo')

    @inlineCallbacks
    def test_unsupported_command_id(self):
        calls = []
        self.patch(EsmeTransceiver, 'on_unsupported_command_id',
                   lambda p, pdu: calls.append(pdu))
        invalid_pdu = {
            'header': {
                'command_id': 'foo',
            }
        }
        transport, protocol = yield self.setup_bind()
        protocol.on_pdu(invalid_pdu)
        self.assertEqual(calls, [invalid_pdu])

    @inlineCallbacks
    def test_csm_split_message(self):
        protocol = yield self.get_protocol()

        def split(msg):
            return protocol.csm_split_message(msg.encode('utf-8'))

        # these are fine because they're in the 7-bit character set
        self.assertEqual(1, len(split(u'&' * 140)))
        self.assertEqual(1, len(split(u'&' * 160)))
        # ± is not in the 7-bit character set so it should utf-8 encode it
        # which bumps it over the 140 bytes
        self.assertEqual(2, len(split(u'±' + u'1' * 139)))
