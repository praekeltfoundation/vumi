# -*- coding: utf-8 -*-
from twisted.internet.defer import inlineCallbacks, succeed, gatherResults
from twisted.internet.task import Clock

from smpp.pdu_builder import Unbind, SubmitSM
from vumi.log import WrappingLogger
from vumi.tests.helpers import VumiTestCase, PersistenceHelper, skiptest
from vumi.transports.smpp.smpp_transport import (
    SmppTransceiverTransport, SmppMessageDataStash, pdu_key)
from vumi.transports.smpp.protocol import EsmeProtocol, EsmeProtocolError
from vumi.transports.smpp.smpp_service import SmppService
from vumi.transports.smpp.pdu_utils import (
    command_id, unpacked_pdu_opts, short_message)
from vumi.transports.smpp.sequence import RedisSequence
from vumi.transports.smpp.tests.fake_smsc import FakeSMSC


class DummySmppTransport(object):
    def __init__(self, clock, redis, config):
        self.log = WrappingLogger(system=config.get('worker_name'))
        self.clock = clock
        self.redis = redis
        self._config = config
        self._static_config = SmppTransceiverTransport.CONFIG_CLASS(
            self._config, static=True)

        config = self.get_static_config()
        self.transport_name = config.transport_name

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

    def pause_connectors(self):
        self.paused = True

    def unpause_connectors(self):
        self.paused = False

    def on_smpp_binding(self):
        pass

    def on_smpp_unbinding(self):
        pass

    def on_smpp_bind(self):
        pass

    def on_smpp_bind_timeout(self):
        pass

    def on_throttled(self):
        pass

    def on_throttled_end(self):
        pass


class TestSmppService(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.clock = Clock()
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.redis = yield self.persistence_helper.get_redis_manager()
        self.fake_smsc = FakeSMSC(auto_accept=False)
        self.default_config = {
            'transport_name': 'sphex_transport',
            'twisted_endpoint': self.fake_smsc.endpoint,
            'system_id': 'system_id',
            'password': 'password',
        }

    def get_service(self, config={}, bind_type='TRX', start=True):
        """
        Create and optionally start a new service object.
        """
        cfg = self.default_config.copy()
        cfg.update(config)
        dummy_transport = DummySmppTransport(self.clock, self.redis, cfg)
        service = SmppService(
            self.fake_smsc.endpoint, bind_type, dummy_transport)
        service.clock = self.clock

        d = succeed(service)
        if start:
            d.addCallback(self.start_service)
        return d

    def start_service(self, service, accept_connection=True):
        """
        Start the given service.
        """
        service.startService()
        self.clock.advance(0)
        d = self.fake_smsc.await_connecting()
        if accept_connection:
            d.addCallback(lambda _: self.fake_smsc.accept_connection())
        return d.addCallback(lambda _: service)

    def lookup_message_ids(self, service, seq_nums):
        """
        Find vumi message ids associated with SMPP sequence numbers.
        """
        lookup_func = service.message_stash.get_sequence_number_message_id
        return gatherResults([lookup_func(seq_num) for seq_num in seq_nums])

    def set_sequence_number(self, service, seq_nr):
        return service.sequence_generator.redis.set(
            'smpp_last_sequence_number', seq_nr)

    @inlineCallbacks
    def test_start_sequence(self):
        """
        The service goes through several states while starting.
        """
        # New service, never started.
        service = yield self.get_service(start=False)
        self.assertEqual(service.running, False)
        self.assertEqual(service.get_bind_state(), EsmeProtocol.CLOSED_STATE)

        # Start, but don't connect.
        yield self.start_service(service, accept_connection=False)
        self.assertEqual(service.running, True)
        self.assertEqual(service.get_bind_state(), EsmeProtocol.CLOSED_STATE)

        # Connect, but don't bind.
        yield self.fake_smsc.accept_connection()
        self.assertEqual(service.running, True)
        self.assertEqual(service.get_bind_state(), EsmeProtocol.OPEN_STATE)
        bind_pdu = yield self.fake_smsc.await_pdu()
        self.assertEqual(command_id(bind_pdu), 'bind_transceiver')

        # Bind.
        yield self.fake_smsc.bind(bind_pdu)
        self.assertEqual(service.running, True)
        self.assertEqual(
            service.get_bind_state(), EsmeProtocol.BOUND_STATE_TRX)

    @inlineCallbacks
    def test_connect_retries(self):
        """
        If we fail to connect, we retry.
        """
        service = yield self.get_service(start=False)
        self.assertEqual(self.fake_smsc.has_pending_connection(), False)

        # Start, but don't connect.
        yield self.start_service(service, accept_connection=False)
        self.assertEqual(self.fake_smsc.has_pending_connection(), True)
        self.assertEqual(service._protocol, None)
        self.assertEqual(service.retries, 1)

        # Reject the connection.
        yield self.fake_smsc.reject_connection()
        self.assertEqual(service._protocol, None)
        self.assertEqual(service.retries, 2)

        # Advance to the next connection attempt.
        self.clock.advance(service.delay)
        self.assertEqual(self.fake_smsc.has_pending_connection(), True)
        self.assertEqual(service._protocol, None)
        self.assertEqual(service.retries, 2)

        # Accept the connection.
        yield self.fake_smsc.accept_connection()
        self.assertEqual(service.running, True)
        self.assertNotEqual(service._protocol, None)

    @inlineCallbacks
    def test_submit_sm(self):
        """
        When bound, we can send a message.
        """
        service = yield self.get_service()
        yield self.fake_smsc.bind()

        seq_nums = yield service.submit_sm(
            'abc123', 'dest_addr', short_message='foo')
        submit_sm = yield self.fake_smsc.await_pdu()
        self.assertEqual(command_id(submit_sm), 'submit_sm')
        stored_ids = yield self.lookup_message_ids(service, seq_nums)
        self.assertEqual(['abc123'], stored_ids)

    @inlineCallbacks
    def test_submit_sm_unbound(self):
        """
        When unbound, we can't send a message.
        """
        service = yield self.get_service()

        self.assertRaises(
            EsmeProtocolError,
            service.submit_sm, 'abc123', 'dest_addr', short_message='foo')

    @inlineCallbacks
    def test_submit_sm_not_connected(self):
        """
        When not connected, we can't send a message.
        """
        service = yield self.get_service(start=False)
        yield self.start_service(service, accept_connection=False)

        self.assertRaises(
            EsmeProtocolError,
            service.submit_sm, 'abc123', 'dest_addr', short_message='foo')

    @skiptest("FIXME: We don't actually unbind and disconnect yet.")
    @inlineCallbacks
    def test_handle_unbind(self):
        """
        If the SMSC sends an unbind command, we respond and disconnect.
        """
        service = yield self.get_service()
        yield self.fake_smsc.bind()

        self.assertEqual(service.is_bound(), True)
        self.fake_smsc.send_pdu(Unbind(7))
        unbind_resp_pdu = yield self.fake_smsc.await_pdu()
        self.assertEqual(command_id(unbind_resp_pdu), 'unbind_resp')
        self.assertEqual(service.is_bound(), False)

    @inlineCallbacks
    def test_csm_split_message(self):
        """
        A multipart message is split into chunks such that the smallest number
        of message parts are required.
        """
        service = yield self.get_service()

        split = lambda msg: service.csm_split_message(msg.encode('utf-8'))

        # these are fine because they're in the 7-bit character set
        self.assertEqual(1, len(split(u'&' * 140)))
        self.assertEqual(1, len(split(u'&' * 160)))
        # ± is not in the 7-bit character set so it should utf-8 encode it
        # which bumps it over the 140 bytes
        self.assertEqual(2, len(split(u'±' + u'1' * 139)))

    @inlineCallbacks
    def test_submit_sm_long(self):
        """
        A long message can be sent in a single PDU using the optional
        `message_payload` PDU field.
        """
        service = yield self.get_service()
        yield self.fake_smsc.bind()

        long_message = 'This is a long message.' * 20
        seq_nums = yield service.submit_sm_long(
            'abc123', 'dest_addr', long_message)
        submit_sm = yield self.fake_smsc.await_pdu()
        pdu_opts = unpacked_pdu_opts(submit_sm)

        self.assertEqual('submit_sm', submit_sm['header']['command_id'])
        self.assertEqual(
            None, submit_sm['body']['mandatory_parameters']['short_message'])
        self.assertEqual(''.join('%02x' % ord(c) for c in long_message),
                         pdu_opts['message_payload'])
        stored_ids = yield self.lookup_message_ids(service, seq_nums)
        self.assertEqual(['abc123'], stored_ids)

    @inlineCallbacks
    def test_submit_csm_sar(self):
        """
        A long message can be sent in multiple PDUs with SAR fields set to
        instruct the SMSC to build user data headers.
        """
        service = yield self.get_service({'send_multipart_sar': True})
        yield self.fake_smsc.bind()

        long_message = 'This is a long message.' * 20
        seq_nums = yield service.submit_csm_sar(
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

        stored_ids = yield self.lookup_message_ids(service, seq_nums)
        self.assertEqual(['abc123'] * len(seq_nums), stored_ids)

    @inlineCallbacks
    def test_submit_csm_sar_ref_num_limit(self):
        """
        The SAR reference number is set correctly when the generated reference
        number is larger than 0xFFFF.
        """
        service = yield self.get_service({'send_multipart_sar': True})
        yield self.fake_smsc.bind()
        # forward until we go past 0xFFFF
        yield self.set_sequence_number(service, 0x10000)

        long_message = 'This is a long message.' * 20
        seq_nums = yield service.submit_csm_sar(
            'abc123', 'dest_addr', short_message=long_message)
        pdus = yield self.fake_smsc.await_pdus(4)
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
        self.assertEqual([2, 2, 2, 2], msg_refs)

        stored_ids = yield self.lookup_message_ids(service, seq_nums)
        self.assertEqual(['abc123'] * len(seq_nums), stored_ids)

    @inlineCallbacks
    def test_submit_csm_sar_single_part(self):
        """
        If the content fits in a single message, all the multipart madness is
        avoided.
        """
        service = yield self.get_service({'send_multipart_sar': True})
        yield self.fake_smsc.bind()

        content = 'a' * 160
        seq_numbers = yield service.submit_csm_sar(
            'abc123', 'dest_addr', short_message=content)
        self.assertEqual(len(seq_numbers), 1)
        submit_sm_pdu = yield self.fake_smsc.await_pdu()

        self.assertEqual(command_id(submit_sm_pdu), 'submit_sm')
        self.assertEqual(short_message(submit_sm_pdu), content)
        self.assertEqual(unpacked_pdu_opts(submit_sm_pdu), {})

    @inlineCallbacks
    def test_submit_csm_udh(self):
        """
        A long message can be sent in multiple PDUs with carefully handcrafted
        user data headers.
        """
        service = yield self.get_service({'send_multipart_udh': True})
        yield self.fake_smsc.bind()

        long_message = 'This is a long message.' * 20
        seq_numbers = yield service.submit_csm_udh(
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

        stored_ids = yield self.lookup_message_ids(service, seq_numbers)
        self.assertEqual(['abc123'] * len(seq_numbers), stored_ids)

    @inlineCallbacks
    def test_submit_csm_udh_ref_num_limit(self):
        """
        User data headers are crafted correctly when the generated reference
        number is larger than 0xFF.
        """
        service = yield self.get_service({'send_multipart_udh': True})
        yield self.fake_smsc.bind()
        # forward until we go past 0xFF
        yield self.set_sequence_number(service, 0x100)

        long_message = 'This is a long message.' * 20
        seq_numbers = yield service.submit_csm_udh(
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

        stored_ids = yield self.lookup_message_ids(service, seq_numbers)
        self.assertEqual(['abc123'] * len(seq_numbers), stored_ids)

    @inlineCallbacks
    def test_submit_csm_udh_single_part(self):
        """
        If the content fits in a single message, all the multipart madness is
        avoided.
        """
        service = yield self.get_service({'send_multipart_udh': True})
        yield self.fake_smsc.bind()

        content = 'a' * 160
        seq_numbers = yield service.submit_csm_udh(
            'abc123', 'dest_addr', short_message=content)
        self.assertEqual(len(seq_numbers), 1)
        submit_sm_pdu = yield self.fake_smsc.await_pdu()

        self.assertEqual(command_id(submit_sm_pdu), 'submit_sm')
        self.assertEqual(short_message(submit_sm_pdu), content)
        self.assertEqual(
            submit_sm_pdu['body']['mandatory_parameters']['esm_class'], 0)

    @inlineCallbacks
    def test_pdu_cache_persistence(self):
        """
        A cached PDU has an appropriate TTL and can be deleted.
        """
        service = yield self.get_service()

        message_stash = service.message_stash
        config = service.get_config()

        pdu = SubmitSM(1337, short_message="foo")
        yield message_stash.cache_pdu("vumi0", pdu)

        ttl = yield message_stash.redis.ttl(pdu_key(1337))
        self.assertTrue(0 < ttl <= config.submit_sm_expiry)

        pdu_data = yield message_stash.get_cached_pdu(1337)
        self.assertEqual(pdu_data.vumi_message_id, "vumi0")
        self.assertEqual(pdu_data.pdu.get_hex(), pdu.get_hex())

        yield message_stash.delete_cached_pdu(1337)
        deleted_pdu_data = yield message_stash.get_cached_pdu(1337)
        self.assertEqual(deleted_pdu_data, None)
