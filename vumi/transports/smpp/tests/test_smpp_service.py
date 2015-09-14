# -*- coding: utf-8 -*-
from twisted.internet.defer import inlineCallbacks, succeed, gatherResults
from twisted.internet.task import Clock

from smpp.pdu_builder import Unbind
from vumi.tests.helpers import VumiTestCase, PersistenceHelper, skiptest
from vumi.transports.smpp.smpp_transport import (
    SmppTransceiverTransport, SmppMessageDataStash)
from vumi.transports.smpp.protocol import EsmeProtocol, EsmeProtocolError
from vumi.transports.smpp.smpp_service import SmppService
from vumi.transports.smpp.pdu_utils import command_id
from vumi.transports.smpp.sequence import RedisSequence
from vumi.transports.smpp.tests.fake_smsc import FakeSMSC


class DummySmppTransport(object):
    def __init__(self, clock, redis, config):
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
