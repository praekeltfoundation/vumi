# -*- coding: utf-8 -*-
from twisted.internet.defer import inlineCallbacks, gatherResults, succeed
from twisted.internet.task import Clock

from smpp.pdu_builder import (
    Unbind, UnbindResp, SubmitSMResp, DeliverSM, EnquireLink)
from vumi.tests.helpers import VumiTestCase, PersistenceHelper
from vumi.transports.smpp.smpp_transport import (
    SmppTransceiverTransport, SmppMessageDataStash)
from vumi.transports.smpp.protocol import (
    EsmeProtocol, EsmeProtocolFactory)
from vumi.transports.smpp.pdu_utils import (
    seq_no, command_status, command_id, short_message)
from vumi.transports.smpp.smpp_utils import unpacked_pdu_opts
from vumi.transports.smpp.sequence import RedisSequence
from vumi.transports.smpp.tests.fake_smsc import FakeSMSC


class DummySmppService(object):
    def __init__(self, clock, redis, config):
        self.clock = clock
        self.redis = redis
        self._config = config
        self._static_config = SmppTransceiverTransport.CONFIG_CLASS(
            self._config, static=True)

        config = self.get_config()
        self.dr_processor = config.delivery_report_processor(
            self, config.delivery_report_processor_config)
        self.deliver_sm_processor = config.deliver_short_message_processor(
            self, config.deliver_short_message_processor_config)
        self.submit_sm_processor = config.submit_short_message_processor(
            self, config.submit_short_message_processor_config)
        self.sequence_generator = RedisSequence(self.redis)
        self.message_stash = SmppMessageDataStash(self.redis, config)

        self.paused = True

    def get_static_config(self):
        return self._static_config

    def get_config(self):
        return self._static_config

    def on_connection_lost(self):
        self.paused = True

    def on_smpp_bind(self):
        self.paused = False


class TestEsmeProtocol(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.clock = Clock()
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.redis = yield self.persistence_helper.get_redis_manager()
        self.fake_smsc = FakeSMSC(auto_accept=False)

    def get_protocol(self, config={}, bind_type='TRX', accept_connection=True):
        cfg = {
            'transport_name': 'sphex_transport',
            'twisted_endpoint': 'tcp:host=127.0.0.1:port=0',
            'system_id': 'system_id',
            'password': 'password',
            'smpp_bind_timeout': 30,
        }
        cfg.update(config)
        dummy_service = DummySmppService(self.clock, self.redis, cfg)

        factory = EsmeProtocolFactory(dummy_service, bind_type)
        proto_d = self.fake_smsc.endpoint.connect(factory)
        if accept_connection:
            self.fake_smsc.accept_connection()
        return proto_d

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

    def set_sequence_number(self, protocol, seq_nr):
        return protocol.sequence_generator.redis.set(
            'smpp_last_sequence_number', seq_nr)

    def lookup_message_ids(self, protocol, seq_nums):
        message_stash = protocol.service.message_stash
        lookup_func = message_stash.get_sequence_number_message_id
        return gatherResults([lookup_func(seq_num) for seq_num in seq_nums])

    @inlineCallbacks
    def test_on_connection_made(self):
        connect_d = self.get_protocol(accept_connection=False)
        protocol = yield self.fake_smsc.await_connecting()
        self.assertEqual(protocol.state, EsmeProtocol.CLOSED_STATE)
        self.fake_smsc.accept_connection()
        protocol = yield connect_d  # Same protocol.
        self.assertEqual(protocol.state, EsmeProtocol.OPEN_STATE)

        bind_pdu = yield self.fake_smsc.await_pdu()
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
        bind_pdu = yield self.fake_smsc.await_pdu()
        self.assertCommand(bind_pdu, 'bind_transceiver')
        self.assertFalse(protocol.is_bound())
        self.assertEqual(protocol.state, EsmeProtocol.OPEN_STATE)
        self.clock.advance(protocol.config.smpp_bind_timeout + 1)
        unbind_pdu = yield self.fake_smsc.await_pdu()
        self.assertCommand(unbind_pdu, 'unbind')
        yield self.fake_smsc.send_pdu(UnbindResp(seq_no(unbind_pdu)))
        yield self.fake_smsc.await_disconnect()

    @inlineCallbacks
    def test_on_smpp_bind(self):
        protocol = yield self.get_protocol()
        yield self.fake_smsc.bind()
        self.assertEqual(protocol.state, EsmeProtocol.BOUND_STATE_TRX)
        self.assertTrue(protocol.is_bound())
        self.assertTrue(protocol.enquire_link_call.running)

    @inlineCallbacks
    def test_handle_unbind(self):
        protocol = yield self.get_protocol()
        yield self.fake_smsc.bind()
        self.assertEqual(protocol.state, EsmeProtocol.BOUND_STATE_TRX)
        self.fake_smsc.send_pdu(Unbind(0))
        pdu = yield self.fake_smsc.await_pdu()
        self.assertCommand(
            pdu, 'unbind_resp', sequence_number=0, status='ESME_ROK')
        # We don't change state here.
        self.assertEqual(protocol.state, EsmeProtocol.BOUND_STATE_TRX)

    @inlineCallbacks
    def test_on_submit_sm_resp(self):
        protocol = yield self.get_protocol()
        yield self.fake_smsc.bind()
        calls = []
        protocol.on_submit_sm_resp = lambda *a: calls.append(a)
        yield self.fake_smsc.send_pdu(SubmitSMResp(0, message_id='foo'))
        self.assertEqual(calls, [(0, 'foo', 'ESME_ROK')])

    @inlineCallbacks
    def test_deliver_sm(self):
        calls = []
        protocol = yield self.get_protocol()
        protocol.handle_deliver_sm = lambda pdu: succeed(calls.append(pdu))
        yield self.fake_smsc.bind()
        yield self.fake_smsc.send_pdu(
            DeliverSM(0, message_id='foo', short_message='bar'))
        [deliver_sm] = calls
        self.assertCommand(deliver_sm, 'deliver_sm', sequence_number=0)

    @inlineCallbacks
    def test_deliver_sm_fail(self):
        yield self.get_protocol()
        yield self.fake_smsc.send_pdu(DeliverSM(
            sequence_number=0, message_id='foo', data_coding=4,
            short_message='string with unknown data coding'))
        deliver_sm_resp = yield self.fake_smsc.await_pdu()
        self.assertCommand(
            deliver_sm_resp, 'deliver_sm_resp', sequence_number=0,
            status='ESME_RDELIVERYFAILURE')

    @inlineCallbacks
    def test_deliver_sm_fail_with_custom_error(self):
        yield self.get_protocol({
            "deliver_sm_decoding_error": "ESME_RSYSERR"
        })
        yield self.fake_smsc.send_pdu(DeliverSM(
            sequence_number=0, message_id='foo', data_coding=4,
            short_message='string with unknown data coding'))
        deliver_sm_resp = yield self.fake_smsc.await_pdu()
        self.assertCommand(
            deliver_sm_resp, 'deliver_sm_resp', sequence_number=0,
            status='ESME_RSYSERR')

    @inlineCallbacks
    def test_on_enquire_link(self):
        protocol = yield self.get_protocol()
        yield self.fake_smsc.bind()
        pdu = EnquireLink(0)
        protocol.dataReceived(pdu.get_bin())
        enquire_link_resp = yield self.fake_smsc.await_pdu()
        self.assertCommand(
            enquire_link_resp, 'enquire_link_resp', sequence_number=0,
            status='ESME_ROK')

    @inlineCallbacks
    def test_on_enquire_link_resp(self):
        protocol = yield self.get_protocol()
        calls = []
        protocol.handle_enquire_link_resp = calls.append
        yield self.fake_smsc.bind()
        [pdu] = calls
        # bind_transceiver is sequence_number 1
        self.assertEqual(seq_no(pdu), 2)
        self.assertEqual(command_id(pdu), 'enquire_link_resp')

    @inlineCallbacks
    def test_enquire_link_no_response(self):
        self.fake_smsc.auto_unbind = False
        protocol = yield self.get_protocol()
        yield self.fake_smsc.bind()
        self.assertEqual(self.fake_smsc.connected, True)
        self.clock.advance(protocol.idle_timeout)
        [enquire_link_pdu, unbind_pdu] = yield self.fake_smsc.await_pdus(2)
        self.assertCommand(enquire_link_pdu, 'enquire_link')
        self.assertCommand(unbind_pdu, 'unbind')
        self.assertEqual(self.fake_smsc.connected, True)
        self.clock.advance(protocol.unbind_timeout)
        yield self.fake_smsc.await_disconnect()

    @inlineCallbacks
    def test_enquire_link_looping(self):
        self.fake_smsc.auto_unbind = False
        protocol = yield self.get_protocol()
        yield self.fake_smsc.bind()
        self.assertEqual(self.fake_smsc.connected, True)

        # Respond to a few enquire_link cycles.
        for i in range(5):
            self.clock.advance(protocol.idle_timeout - 1)
            pdu = yield self.fake_smsc.await_pdu()
            self.assertCommand(pdu, 'enquire_link')
            yield self.fake_smsc.respond_to_enquire_link(pdu)

        # Fail to respond, so we disconnect.
        self.clock.advance(protocol.idle_timeout - 1)
        pdu = yield self.fake_smsc.await_pdu()
        self.assertCommand(pdu, 'enquire_link')
        self.clock.advance(1)
        unbind_pdu = yield self.fake_smsc.await_pdu()
        self.assertCommand(unbind_pdu, 'unbind')
        yield self.fake_smsc.send_pdu(
            UnbindResp(seq_no(unbind_pdu)))
        yield self.fake_smsc.await_disconnect()

    @inlineCallbacks
    def test_submit_sm(self):
        protocol = yield self.get_protocol()
        yield self.fake_smsc.bind()
        seq_nums = yield protocol.submit_sm(
            'abc123', 'dest_addr', short_message='foo')
        submit_sm = yield self.fake_smsc.await_pdu()
        self.assertCommand(submit_sm, 'submit_sm', params={
            'short_message': 'foo',
        })
        stored_ids = yield self.lookup_message_ids(protocol, seq_nums)
        self.assertEqual(['abc123'], stored_ids)

    @inlineCallbacks
    def test_submit_sm_configured_parameters(self):
        protocol = yield self.get_protocol({
            'service_type': 'stype',
            'source_addr_ton': 2,
            'source_addr_npi': 2,
            'dest_addr_ton': 2,
            'dest_addr_npi': 2,
            'registered_delivery': 0,
        })
        yield self.fake_smsc.bind()
        seq_nums = yield protocol.submit_sm(
            'abc123', 'dest_addr', short_message='foo')
        submit_sm = yield self.fake_smsc.await_pdu()
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
        protocol = yield self.get_protocol()
        yield self.fake_smsc.bind()
        long_message = 'This is a long message.' * 20
        seq_nums = yield protocol.submit_sm_long(
            'abc123', 'dest_addr', long_message)
        submit_sm = yield self.fake_smsc.await_pdu()
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
        protocol = yield self.get_protocol({'send_multipart_udh': True})
        yield self.fake_smsc.bind()
        long_message = 'This is a long message.' * 20
        seq_numbers = yield protocol.submit_csm_udh(
            'abc123', 'dest_addr', short_message=long_message)
        pdus = yield self.fake_smsc.await_pdus(4)
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
        protocol = yield self.get_protocol({'send_multipart_udh': True})
        yield self.fake_smsc.bind()

        # forward until we go past 0xFF
        yield self.set_sequence_number(protocol, 0xFF)

        long_message = 'This is a long message.' * 20
        seq_numbers = yield protocol.submit_csm_udh(
            'abc123', 'dest_addr', short_message=long_message)
        pdus = yield self.fake_smsc.await_pdus(4)

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
        protocol = yield self.get_protocol({'send_multipart_sar': True})
        yield self.fake_smsc.bind()
        long_message = 'This is a long message.' * 20
        seq_nums = yield protocol.submit_csm_sar(
            'abc123', 'dest_addr', short_message=long_message)
        pdus = yield self.fake_smsc.await_pdus(4)
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
        protocol = yield self.get_protocol({'send_multipart_udh': True})
        yield self.fake_smsc.bind()

        # forward until we go past 0xFFFF
        yield self.set_sequence_number(protocol, 0xFFFF)

        long_message = 'This is a long message.' * 20
        seq_numbers = yield protocol.submit_csm_udh(
            'abc123', 'dest_addr', short_message=long_message)
        pdus = yield self.fake_smsc.await_pdus(4)

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
        protocol = yield self.get_protocol()
        yield self.fake_smsc.bind()
        yield protocol.query_sm('foo', source_addr='bar')
        query_sm = yield self.fake_smsc.await_pdu()
        self.assertCommand(query_sm, 'query_sm', params={
            'message_id': 'foo',
            'source_addr': 'bar',
        })

    @inlineCallbacks
    def test_unbind(self):
        protocol = yield self.get_protocol()
        calls = []
        protocol.handle_unbind_resp = calls.append
        yield self.fake_smsc.bind()
        yield protocol.unbind()
        unbind_pdu = yield self.fake_smsc.await_pdu()
        protocol.dataReceived(UnbindResp(seq_no(unbind_pdu)).get_bin())
        [unbind_resp_pdu] = calls
        self.assertEqual(seq_no(unbind_resp_pdu), seq_no(unbind_pdu))

    @inlineCallbacks
    def test_bind_transmitter(self):
        protocol = yield self.get_protocol(bind_type='TX')
        yield self.fake_smsc.bind()
        self.assertTrue(protocol.is_bound())
        self.assertEqual(protocol.state, protocol.BOUND_STATE_TX)

    @inlineCallbacks
    def test_bind_receiver(self):
        protocol = yield self.get_protocol(bind_type='RX')
        yield self.fake_smsc.bind()
        self.assertTrue(protocol.is_bound())
        self.assertEqual(protocol.state, protocol.BOUND_STATE_RX)

    @inlineCallbacks
    def test_partial_pdu_data_received(self):
        protocol = yield self.get_protocol()
        calls = []
        protocol.handle_deliver_sm = calls.append
        yield self.fake_smsc.bind()
        deliver_sm = DeliverSM(1, short_message='foo')
        pdu = deliver_sm.get_bin()
        half = len(pdu) / 2
        pdu_part1, pdu_part2 = pdu[:half], pdu[half:]
        yield self.fake_smsc.send_bytes(pdu_part1)
        self.assertEqual([], calls)
        yield self.fake_smsc.send_bytes(pdu_part2)
        [handled_pdu] = calls
        self.assertEqual(command_id(handled_pdu), 'deliver_sm')
        self.assertEqual(seq_no(handled_pdu), 1)
        self.assertEqual(short_message(handled_pdu), 'foo')

    @inlineCallbacks
    def test_unsupported_command_id(self):
        protocol = yield self.get_protocol()
        calls = []
        protocol.on_unsupported_command_id = calls.append
        invalid_pdu = {
            'header': {
                'command_id': 'foo',
            }
        }
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
