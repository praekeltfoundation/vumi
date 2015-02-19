# -*- coding: utf-8 -*-

import logging

from twisted.test import proto_helpers
from twisted.internet.defer import (
    inlineCallbacks, returnValue, Deferred, succeed)
from twisted.internet.error import ConnectionDone
from twisted.internet.task import Clock
from twisted.application.service import Service

from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import LogCatcher

from vumi.transports.tests.helpers import TransportHelper
from vumi.transports.smpp.smpp_transport import (
    SmppTransceiverTransport,
    SmppTransceiverTransportWithOldConfig,
    SmppTransmitterTransport, SmppReceiverTransport,
    message_key, remote_message_key, SmppService)
from vumi.transports.smpp.pdu_utils import (
    pdu_ok, short_message, command_id, seq_no, pdu_tlv, unpacked_pdu_opts)
from vumi.transports.smpp.tests.test_protocol import (
    bind_protocol, wait_for_pdus)
from vumi.transports.smpp.processors import SubmitShortMessageProcessor

from vumi.message import TransportUserMessage
from vumi.config import ConfigError

from smpp.pdu_builder import DeliverSM, SubmitSMResp


class DummyService(Service):

    def __init__(self, endpoint, factory):
        self.factory = factory
        self.protocol = None
        self.wait_on_protocol_deferreds = []

    def startService(self):
        self.protocol = self.factory.buildProtocol(('120.0.0.1', 0))
        while self.wait_on_protocol_deferreds:
            deferred = self.wait_on_protocol_deferreds.pop()
            deferred.callback(self.protocol)

    def stopService(self):
        if self.protocol and self.protocol.transport:
            self.protocol.transport.loseConnection()
            self.protocol.connectionLost(reason=ConnectionDone)

    def get_protocol(self):
        if self.protocol is not None:
            return succeed(self.protocol)
        else:
            d = Deferred()
            self.wait_on_protocol_deferreds.append(d)
            return d

    def is_bound(self):
        if self.protocol is not None:
            return self.protocol.is_bound()
        return False


class SMPPHelper(object):
    def __init__(self, string_transport, transport, protocol):
        self.string_transport = string_transport
        self.transport = transport
        self.protocol = protocol

    def send_pdu(self, pdu):
        """put it on the wire and don't wait for a response"""
        self.protocol.dataReceived(pdu.get_bin())

    def handle_pdu(self, pdu):
        """short circuit the wire so we get a deferred so we know
        when it's been handled, also allows us to test PDUs that are invalid
        because we're skipping the encode/decode step."""
        return self.protocol.on_pdu(pdu.obj)

    def send_mo(self, sequence_number, short_message, data_coding=1, **kwargs):
        return self.send_pdu(
            DeliverSM(sequence_number, short_message=short_message,
                      data_coding=data_coding, **kwargs))

    def wait_for_pdus(self, count):
        return wait_for_pdus(self.string_transport, count)

    def no_pdus(self):
        return self.string_transport.value() == ''


class SmppTransportTestCase(VumiTestCase):

    DR_TEMPLATE = ("id:%s sub:... dlvrd:... submit date:200101010030"
                   " done date:200101020030 stat:DELIVRD err:... text:Meep")
    DR_MINIMAL_TEMPLATE = "id:%s stat:DELIVRD text:Meep"
    transport_class = None

    def setUp(self):

        self.clock = Clock()
        self.patch(self.transport_class, 'service_class', DummyService)
        self.patch(self.transport_class, 'clock', self.clock)

        self.string_transport = proto_helpers.StringTransport()

        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.default_config = {
            'transport_name': self.tx_helper.transport_name,
            'twisted_endpoint': 'tcp:host=127.0.0.1:port=0',
            'delivery_report_processor': 'vumi.transports.smpp.processors.'
                                         'DeliveryReportProcessor',
            'deliver_short_message_processor': (
                'vumi.transports.smpp.processors.'
                'DeliverShortMessageProcessor'),
            'system_id': 'foo',
            'password': 'bar',
            'deliver_short_message_processor_config': {
                'data_coding_overrides': {
                    0: 'utf-8',
                }
            }
        }

    @inlineCallbacks
    def get_transport(self, config={}, bind=True):
        cfg = self.default_config.copy()
        cfg.update(config)
        transport = yield self.tx_helper.get_transport(cfg)
        if bind:
            yield self.create_smpp_bind(transport)
        returnValue(transport)

    def create_smpp_bind(self, smpp_transport):
        d = smpp_transport.service.get_protocol()

        def cb(protocol):
            protocol.makeConnection(self.string_transport)
            return bind_protocol(self.string_transport, protocol)

        d.addCallback(cb)
        return d

    def send_pdu(self, transport, pdu):
        protocol = transport.service.get_protocol()
        protocol.dataReceived(pdu.get_bin())

    @inlineCallbacks
    def get_smpp_helper(self, *args, **kwargs):
        transport = yield self.get_transport(*args, **kwargs)
        protocol = yield transport.service.get_protocol()
        returnValue(SMPPHelper(self.string_transport, transport, protocol))


class SmppTransceiverTransportTestCase(SmppTransportTestCase):

    transport_class = SmppTransceiverTransport

    @inlineCallbacks
    def test_setup_transport(self):
        transport = yield self.get_transport()
        protocol = yield transport.service.get_protocol()
        self.assertTrue(protocol.is_bound())

    @inlineCallbacks
    def test_smpp_service(self):
        """
        Testing the real service because these tests use the
        fake DummyService implementation
        """

        transport = yield self.get_transport()
        protocol = yield transport.service.get_protocol()

        service = SmppService(None, None)
        self.assertEqual(service._protocol, None)
        self.assertEqual(service.is_bound(), False)

        d = service.get_protocol()
        self.assertEqual(len(service.wait_on_protocol_deferreds), 1)
        service.clientConnected(protocol)
        received_protocol = yield d
        self.assertEqual(received_protocol, protocol)
        self.assertEqual(len(service.wait_on_protocol_deferreds), 0)
        self.assertEqual(service.is_bound(), True)

        # Replace protocol.is_bound() to test that service.is_bound() uses it.
        received_protocol.is_bound = lambda: False
        self.assertEqual(service.is_bound(), False)

    @inlineCallbacks
    def test_setup_transport_host_port_fallback(self):
        self.default_config.pop('twisted_endpoint')
        transport = yield self.get_transport({
            'host': '127.0.0.1',
            'port': 0,
        })
        protocol = yield transport.service.get_protocol()
        self.assertTrue(protocol.is_bound())

    @inlineCallbacks
    def test_mo_sms(self):
        smpp_helper = yield self.get_smpp_helper()
        smpp_helper.send_mo(
            sequence_number=1, short_message='foo', source_addr='123',
            destination_addr='456')
        [deliver_sm_resp] = yield smpp_helper.wait_for_pdus(1)
        self.assertTrue(pdu_ok(deliver_sm_resp))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], 'foo')
        self.assertEqual(msg['from_addr'], '123')
        self.assertEqual(msg['to_addr'], '456')
        self.assertEqual(msg['transport_type'], 'sms')

    @inlineCallbacks
    def test_mo_delivery_report_pdu_opt_params(self):
        """
        We always treat a message with the optional PDU params set as a
        delivery report.
        """
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        pdu = DeliverSM(sequence_number=1, esm_class=4)
        pdu.add_optional_parameter('receipted_message_id', 'foo')
        pdu.add_optional_parameter('message_state', 2)
        yield smpp_helper.handle_pdu(pdu)

        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['user_message_id'], 'bar')

    @inlineCallbacks
    def test_mo_delivery_report_pdu_opt_params_esm_class_not_set(self):
        """
        We always treat a message with the optional PDU params set as a
        delivery report, even if ``esm_class`` is not set.
        """
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        pdu = DeliverSM(sequence_number=1)
        pdu.add_optional_parameter('receipted_message_id', 'foo')
        pdu.add_optional_parameter('message_state', 2)
        yield smpp_helper.handle_pdu(pdu)

        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['user_message_id'], 'bar')

    @inlineCallbacks
    def test_mo_delivery_report_pdu_esm_class_not_set(self):
        """
        We treat a content-based DR as a normal message if the ``esm_class``
        flags are not set.
        """
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        smpp_helper.send_mo(
            sequence_number=1, short_message=self.DR_TEMPLATE % ('foo',),
            source_addr='123', destination_addr='456')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], self.DR_TEMPLATE % ('foo',))
        self.assertEqual(msg['from_addr'], '123')
        self.assertEqual(msg['to_addr'], '456')
        self.assertEqual(msg['transport_type'], 'sms')

        events = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(events, [])

    @inlineCallbacks
    def test_mo_delivery_report_esm_class_with_full_content(self):
        """
        If ``esm_class`` and content are both set appropriately, we process the
        DR.
        """
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        smpp_helper.send_mo(
            sequence_number=1, short_message=self.DR_TEMPLATE % ('foo',),
            source_addr='123', destination_addr='456', esm_class=4)

        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['user_message_id'], 'bar')

    @inlineCallbacks
    def test_mo_delivery_report_esm_class_with_short_status(self):
        """
        If the delivery report has a shorter status field, the default regex
        still matches.
        """
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        short_message = (
            "id:foo sub:... dlvrd:... submit date:200101010030"
            " done date:200101020030 stat:FAILED err:042 text:Meep")
        smpp_helper.send_mo(
            sequence_number=1, short_message=short_message,
            source_addr='123', destination_addr='456', esm_class=4)

        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'failed')
        self.assertEqual(event['user_message_id'], 'bar')

    @inlineCallbacks
    def test_mo_delivery_report_esm_class_with_minimal_content(self):
        """
        If ``esm_class`` and content are both set appropriately, we process the
        DR even if the minimal subset of the content regex matches.
        """
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        smpp_helper.send_mo(
            sequence_number=1, source_addr='123', destination_addr='456',
            short_message=self.DR_MINIMAL_TEMPLATE % ('foo',), esm_class=4)

        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['user_message_id'], 'bar')

    @inlineCallbacks
    def test_mo_delivery_report_content_with_nulls(self):
        """
        If ``esm_class`` and content are both set appropriately, we process the
        DR even if some content fields contain null values.
        """
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        content = (
            "id:%s sub:null dlvrd:null submit date:200101010030"
            " done date:200101020030 stat:DELIVRD err:null text:Meep")
        smpp_helper.send_mo(
            sequence_number=1, short_message=content % ("foo",),
            source_addr='123', destination_addr='456', esm_class=4)

        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['user_message_id'], 'bar')

    @inlineCallbacks
    def test_mo_delivery_report_esm_class_with_bad_content(self):
        """
        If ``esm_class`` indicates a DR but the regex fails to match, we log a
        warning and do nothing.
        """
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        lc = LogCatcher(message="esm_class 4 indicates")
        with lc:
            smpp_helper.send_mo(
                sequence_number=1, source_addr='123', destination_addr='456',
                short_message="foo", esm_class=4)
            yield smpp_helper.wait_for_pdus(1)

        # check that failure to process delivery report was logged
        [warning] = lc.logs
        self.assertEqual(
            warning["message"][0],
            "esm_class 4 indicates delivery report, but content does not"
            " match regex: 'foo'")

        inbound = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(inbound, [])
        events = self.tx_helper.get_dispatched_events()
        self.assertEqual(events, [])

    @inlineCallbacks
    def test_mo_delivery_report_esm_class_with_no_content(self):
        """
        If ``esm_class`` indicates a DR but the content is empty, we log a
        warning and do nothing.
        """
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        lc = LogCatcher(message="esm_class 4 indicates")
        with lc:
            smpp_helper.send_mo(
                sequence_number=1, source_addr='123', destination_addr='456',
                short_message=None, esm_class=4)
            yield smpp_helper.wait_for_pdus(1)

        # check that failure to process delivery report was logged
        [warning] = lc.logs
        self.assertEqual(
            warning["message"][0],
            "esm_class 4 indicates delivery report, but content does not"
            " match regex: None")

        inbound = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(inbound, [])
        events = self.tx_helper.get_dispatched_events()
        self.assertEqual(events, [])

    @inlineCallbacks
    def test_mo_delivery_report_esm_disabled_with_full_content(self):
        """
        If ``esm_class`` checking is disabled and the content is set
        appropriately, we process the DR.
        """
        smpp_helper = yield self.get_smpp_helper(config={
            "delivery_report_processor_config": {
                "delivery_report_use_esm_class": False,
            }
        })
        transport = smpp_helper.transport
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        smpp_helper.send_mo(
            sequence_number=1, short_message=self.DR_TEMPLATE % ('foo',),
            source_addr='123', destination_addr='456', esm_class=0)

        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['user_message_id'], 'bar')

    @inlineCallbacks
    def test_mo_delivery_report_esm_disabled_with_minimal_content(self):
        """
        If ``esm_class`` checking is disabled and the content is set
        appropriately, we process the DR even if the minimal subset of the
        content regex matches.
        """
        smpp_helper = yield self.get_smpp_helper(config={
            "delivery_report_processor_config": {
                "delivery_report_use_esm_class": False,
            }
        })
        transport = smpp_helper.transport
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        smpp_helper.send_mo(
            sequence_number=1, source_addr='123', destination_addr='456',
            short_message=self.DR_MINIMAL_TEMPLATE % ('foo',), esm_class=0)

        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['user_message_id'], 'bar')

    @inlineCallbacks
    def test_mo_delivery_report_esm_disabled_content_with_nulls(self):
        """
        If ``esm_class`` checking is disabled and the content is set
        appropriately, we process the DR even if some content fields contain
        null values.
        """
        smpp_helper = yield self.get_smpp_helper(config={
            "delivery_report_processor_config": {
                "delivery_report_use_esm_class": False,
            }
        })
        transport = smpp_helper.transport
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        content = (
            "id:%s sub:null dlvrd:null submit date:200101010030"
            " done date:200101020030 stat:DELIVRD err:null text:Meep")
        smpp_helper.send_mo(
            sequence_number=1, short_message=content % ("foo",),
            source_addr='123', destination_addr='456', esm_class=0)

        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['user_message_id'], 'bar')

    @inlineCallbacks
    def test_mo_sms_unicode(self):
        smpp_helper = yield self.get_smpp_helper()
        smpp_helper.send_mo(sequence_number=1, short_message='Zo\xc3\xab',
                            data_coding=0)
        [deliver_sm_resp] = yield smpp_helper.wait_for_pdus(1)
        self.assertTrue(pdu_ok(deliver_sm_resp))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], u'Zoë')

    @inlineCallbacks
    def test_mo_sms_multipart_long(self):
        smpp_helper = yield self.get_smpp_helper()
        content = '1' * 255

        pdu = DeliverSM(sequence_number=1)
        pdu.add_optional_parameter('message_payload', content.encode('hex'))
        smpp_helper.send_pdu(pdu)

        [deliver_sm_resp] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(1, seq_no(deliver_sm_resp))
        self.assertTrue(pdu_ok(deliver_sm_resp))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], content)

    @inlineCallbacks
    def test_mo_sms_multipart_udh(self):
        smpp_helper = yield self.get_smpp_helper()
        deliver_sm_resps = []
        smpp_helper.send_mo(sequence_number=1,
                            short_message="\x05\x00\x03\xff\x03\x01back")
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])
        smpp_helper.send_mo(sequence_number=2,
                            short_message="\x05\x00\x03\xff\x03\x02 at")
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])
        smpp_helper.send_mo(sequence_number=3,
                            short_message="\x05\x00\x03\xff\x03\x03 you")
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])
        self.assertEqual([1, 2, 3], map(seq_no, deliver_sm_resps))
        self.assertTrue(all(map(pdu_ok, deliver_sm_resps)))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], u'back at you')

    @inlineCallbacks
    def test_mo_sms_multipart_udh_out_of_order(self):
        smpp_helper = yield self.get_smpp_helper()
        deliver_sm_resps = []
        smpp_helper.send_mo(sequence_number=1,
                            short_message="\x05\x00\x03\xff\x03\x01back")
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])

        smpp_helper.send_mo(sequence_number=3,
                            short_message="\x05\x00\x03\xff\x03\x03 you")
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])

        smpp_helper.send_mo(sequence_number=2,
                            short_message="\x05\x00\x03\xff\x03\x02 at")
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])

        self.assertEqual([1, 3, 2], map(seq_no, deliver_sm_resps))
        self.assertTrue(all(map(pdu_ok, deliver_sm_resps)))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], u'back at you')

    @inlineCallbacks
    def test_mo_sms_multipart_sar(self):
        smpp_helper = yield self.get_smpp_helper()
        deliver_sm_resps = []

        pdu1 = DeliverSM(sequence_number=1, short_message='back')
        pdu1.add_optional_parameter('sar_msg_ref_num', 1)
        pdu1.add_optional_parameter('sar_total_segments', 3)
        pdu1.add_optional_parameter('sar_segment_seqnum', 1)

        smpp_helper.send_pdu(pdu1)
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])

        pdu2 = DeliverSM(sequence_number=2, short_message=' at')
        pdu2.add_optional_parameter('sar_msg_ref_num', 1)
        pdu2.add_optional_parameter('sar_total_segments', 3)
        pdu2.add_optional_parameter('sar_segment_seqnum', 2)

        smpp_helper.send_pdu(pdu2)
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])

        pdu3 = DeliverSM(sequence_number=3, short_message=' you')
        pdu3.add_optional_parameter('sar_msg_ref_num', 1)
        pdu3.add_optional_parameter('sar_total_segments', 3)
        pdu3.add_optional_parameter('sar_segment_seqnum', 3)

        smpp_helper.send_pdu(pdu3)
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])

        self.assertEqual([1, 2, 3], map(seq_no, deliver_sm_resps))
        self.assertTrue(all(map(pdu_ok, deliver_sm_resps)))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], u'back at you')

    @inlineCallbacks
    def test_mo_bad_encoding(self):
        smpp_helper = yield self.get_smpp_helper()

        bad_pdu = DeliverSM(555,
                            short_message="SMS from server containing \xa7",
                            destination_addr="2772222222",
                            source_addr="2772000000",
                            data_coding=1)

        good_pdu = DeliverSM(555,
                             short_message="Next message",
                             destination_addr="2772222222",
                             source_addr="2772000000",
                             data_coding=1)

        yield smpp_helper.handle_pdu(bad_pdu)
        yield smpp_helper.handle_pdu(good_pdu)
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['message_type'], 'user_message')
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['content'], "Next message")

        dispatched_failures = self.tx_helper.get_dispatched_failures()
        self.assertEqual(dispatched_failures, [])

        [failure] = self.flushLoggedErrors(UnicodeDecodeError)
        message = failure.getErrorMessage()
        codec, rest = message.split(' ', 1)
        self.assertEqual(codec, "'ascii'")
        self.assertTrue(
            rest.startswith("codec can't decode byte 0xa7 in position 27"))

    @inlineCallbacks
    def test_mo_sms_failed_remote_id_lookup(self):
        smpp_helper = yield self.get_smpp_helper()

        lc = LogCatcher(message="Failed to retrieve message id")
        with lc:
            yield smpp_helper.handle_pdu(
                DeliverSM(sequence_number=1, esm_class=4,
                          short_message=self.DR_TEMPLATE % ('foo',)))

        # check that failure to send delivery report was logged
        [warning] = lc.logs
        expected_msg = (
            "Failed to retrieve message id for delivery report. Delivery"
            " report from %s discarded.") % (self.tx_helper.transport_name,)
        self.assertEqual(warning['message'], (expected_msg,))

    @inlineCallbacks
    def test_mt_sms(self):
        smpp_helper = yield self.get_smpp_helper()
        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)
        [pdu] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(command_id(pdu), 'submit_sm')
        self.assertEqual(short_message(pdu), 'hello world')

    @inlineCallbacks
    def test_mt_sms_bad_to_addr(self):
        yield self.get_transport()
        msg = yield self.tx_helper.make_dispatch_outbound(
            'hello world', to_addr=u'+\u2000')
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['nack_reason'], u'Invalid to_addr: +\u2000')

    @inlineCallbacks
    def test_mt_sms_bad_from_addr(self):
        yield self.get_transport()
        msg = yield self.tx_helper.make_dispatch_outbound(
            'hello world', from_addr=u'+\u2000')
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['nack_reason'], u'Invalid from_addr: +\u2000')

    @inlineCallbacks
    def test_mt_sms_submit_sm_encoding(self):
        smpp_helper = yield self.get_smpp_helper(config={
            'submit_short_message_processor_config': {
                'submit_sm_encoding': 'latin1',
            }
        })
        yield self.tx_helper.make_dispatch_outbound(u'Zoë destroyer of Ascii!')
        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(
            short_message(submit_sm_pdu),
            u'Zoë destroyer of Ascii!'.encode('latin-1'))

    @inlineCallbacks
    def test_mt_sms_submit_sm_null_message(self):
        """
        We can successfully send a message with null content.
        """
        smpp_helper = yield self.get_smpp_helper()
        msg = self.tx_helper.make_outbound(None)
        yield self.tx_helper.dispatch_outbound(msg)
        [pdu] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(command_id(pdu), 'submit_sm')
        self.assertEqual(short_message(pdu), None)

    @inlineCallbacks
    def test_submit_sm_data_coding(self):
        smpp_helper = yield self.get_smpp_helper(config={
            'submit_short_message_processor_config': {
                'submit_sm_data_coding': 8
            }
        })
        yield self.tx_helper.make_dispatch_outbound("hello world")
        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        params = submit_sm_pdu['body']['mandatory_parameters']
        self.assertEqual(params['data_coding'], 8)

    @inlineCallbacks
    def test_mt_sms_ack(self):
        smpp_helper = yield self.get_smpp_helper()
        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        smpp_helper.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                         message_id='foo'))
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['sent_message_id'], 'foo')

    @inlineCallbacks
    def test_mt_sms_nack(self):
        smpp_helper = yield self.get_smpp_helper()
        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        smpp_helper.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                         message_id='foo', command_status='ESME_RINVDSTADR'))
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['nack_reason'], 'ESME_RINVDSTADR')

    @inlineCallbacks
    def test_mt_sms_failure(self):
        smpp_helper = yield self.get_smpp_helper()
        message = yield self.tx_helper.make_dispatch_outbound(
            "message", message_id='446')
        [submit_sm] = yield smpp_helper.wait_for_pdus(1)
        response = SubmitSMResp(seq_no(submit_sm), "3rd_party_id_3",
                                command_status="ESME_RSUBMITFAIL")
        # A failure PDU might not have a body.
        response.obj.pop('body')
        smpp_helper.send_pdu(response)

        # There should be a nack
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)

        [failure] = yield self.tx_helper.get_dispatched_failures()
        self.assertEqual(failure['reason'], 'ESME_RSUBMITFAIL')
        self.assertEqual(failure['message'], message.payload)

    @inlineCallbacks
    def test_mt_sms_failure_with_no_reason(self):

        smpp_helper = yield self.get_smpp_helper()
        message = yield self.tx_helper.make_dispatch_outbound(
            "message", message_id='446')
        [submit_sm] = yield smpp_helper.wait_for_pdus(1)

        yield smpp_helper.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm),
                         message_id='foo',
                         command_status=None))

        # There should be a nack
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], message['message_id'])
        self.assertEqual(nack['nack_reason'], 'Unspecified')

        [failure] = yield self.tx_helper.get_dispatched_failures()
        self.assertEqual(failure['reason'], 'Unspecified')

    @inlineCallbacks
    def test_mt_sms_seq_num_lookup_failure(self):
        smpp_helper = yield self.get_smpp_helper()

        lc = LogCatcher(message="Failed to retrieve message id")
        with lc:
            yield smpp_helper.handle_pdu(
                SubmitSMResp(sequence_number=0xbad, message_id='bad'))

        # Make sure we didn't store 'None' in redis.
        message_stash = smpp_helper.transport.message_stash
        message_id = yield message_stash.get_internal_message_id('bad')
        self.assertEqual(message_id, None)

        # check that failure to send ack/nack was logged
        [warning] = lc.logs
        expected_msg = (
            "Failed to retrieve message id for deliver_sm_resp. ack/nack"
            " from %s discarded.") % (self.tx_helper.transport_name,)
        self.assertEqual(warning['message'], (expected_msg,))

    @inlineCallbacks
    def test_mt_sms_throttled(self):
        smpp_helper = yield self.get_smpp_helper()
        transport_config = smpp_helper.transport.get_static_config()
        msg = self.tx_helper.make_outbound('hello world')

        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        with LogCatcher(message="Throttling outbound messages.") as lc:
            yield smpp_helper.handle_pdu(
                SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                             message_id='foo',
                             command_status='ESME_RTHROTTLED'))
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)

        self.clock.advance(transport_config.throttle_delay)

        [submit_sm_pdu_retry] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu_retry),
                         message_id='bar',
                         command_status='ESME_ROK'))

        self.assertTrue(seq_no(submit_sm_pdu_retry) > seq_no(submit_sm_pdu))
        self.assertEqual(short_message(submit_sm_pdu), 'hello world')
        self.assertEqual(short_message(submit_sm_pdu_retry), 'hello world')
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

        # We're still throttled until our next attempt to unthrottle finds no
        # messages to retry.
        with LogCatcher(message="No longer throttling outbound") as lc:
            self.clock.advance(transport_config.throttle_delay)
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)

    @inlineCallbacks
    def test_mt_sms_throttle_while_throttled(self):
        smpp_helper = yield self.get_smpp_helper()
        transport_config = smpp_helper.transport.get_static_config()
        msg1 = self.tx_helper.make_outbound('hello world 1')
        msg2 = self.tx_helper.make_outbound('hello world 2')

        yield self.tx_helper.dispatch_outbound(msg1)
        yield self.tx_helper.dispatch_outbound(msg2)
        [ssm_pdu1, ssm_pdu2] = yield smpp_helper.wait_for_pdus(2)
        yield smpp_helper.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu1),
                         message_id='foo1', command_status='ESME_RTHROTTLED'))
        yield smpp_helper.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu2),
                         message_id='foo2', command_status='ESME_RTHROTTLED'))

        # Advance clock, still throttled.
        self.clock.advance(transport_config.throttle_delay)
        [ssm_pdu1_retry1] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu1_retry1),
                         message_id='bar1',
                         command_status='ESME_RTHROTTLED'))

        # Advance clock, message no longer throttled.
        self.clock.advance(transport_config.throttle_delay)
        [ssm_pdu2_retry1] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu2_retry1),
                         message_id='bar2',
                         command_status='ESME_ROK'))

        # Prod clock, message no longer throttled.
        self.clock.advance(0)
        [ssm_pdu1_retry2] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu1_retry2),
                         message_id='baz1',
                         command_status='ESME_ROK'))

        self.assertEqual(short_message(ssm_pdu1), 'hello world 1')
        self.assertEqual(short_message(ssm_pdu2), 'hello world 2')
        self.assertEqual(short_message(ssm_pdu1_retry1), 'hello world 1')
        self.assertEqual(short_message(ssm_pdu2_retry1), 'hello world 2')
        self.assertEqual(short_message(ssm_pdu1_retry2), 'hello world 1')
        [event2, event1] = yield self.tx_helper.wait_for_dispatched_events(2)
        self.assertEqual(event1['event_type'], 'ack')
        self.assertEqual(event1['user_message_id'], msg1['message_id'])
        self.assertEqual(event2['event_type'], 'ack')
        self.assertEqual(event2['user_message_id'], msg2['message_id'])

    @inlineCallbacks
    def test_mt_sms_reconnect_while_throttled(self):
        """
        If we reconnect while throttled, we don't try to unthrottle before the
        connection is in a suitable state.
        """
        smpp_helper = yield self.get_smpp_helper()
        smpp_service = smpp_helper.transport.service
        transport_config = smpp_helper.transport.get_static_config()
        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)
        [ssm_pdu] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu),
                         message_id='foo1',
                         command_status='ESME_RTHROTTLED'))

        # Drop SMPP connection and check throttling.
        with LogCatcher(message="Can't check throttling while unbound") as lc:
            smpp_service.stopService()
            self.clock.advance(transport_config.throttle_delay)
        [logmsg] = lc.logs
        self.assertEqual(
            logmsg["message"][0],
            "Can't check throttling while unbound, trying later.")

        # Reconnect (but don't bind) and check throttling.
        with LogCatcher(message="Can't check throttling while unbound") as lc:
            smpp_helper.transport.service.startService()
            protocol = yield smpp_service.get_protocol()
            protocol.makeConnection(self.string_transport)
            [bind_pdu] = yield smpp_helper.wait_for_pdus(1)
            self.assertTrue(
                bind_pdu["header"]["command_id"].startswith("bind_"))
            self.clock.advance(transport_config.throttle_delay)
        [logmsg] = lc.logs
        self.assertEqual(
            logmsg["message"][0],
            "Can't check throttling while unbound, trying later.")

        # Bind and check throttling.
        with LogCatcher(message="Can't check throttling while unbound") as lc:
            yield bind_protocol(
                self.string_transport, protocol, bind_pdu=bind_pdu)
            self.clock.advance(transport_config.throttle_delay)

        [ssm_pdu_retry] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu_retry),
                         message_id='foo',
                         command_status='ESME_ROK'))
        self.assertEqual(lc.logs, [])
        self.assertEqual(short_message(ssm_pdu), 'hello world')
        self.assertEqual(short_message(ssm_pdu_retry), 'hello world')
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_mt_sms_tps_limits(self):
        smpp_helper = yield self.get_smpp_helper(config={
            'mt_tps': 2,
        })
        transport = smpp_helper.transport

        with LogCatcher(message="Throttling outbound messages.") as lc:
            yield self.tx_helper.make_dispatch_outbound('hello world 1')
            yield self.tx_helper.make_dispatch_outbound('hello world 2')
            msg3_d = self.tx_helper.make_dispatch_outbound('hello world 3')
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)

        self.assertTrue(transport.throttled)
        [submit_sm_pdu1, submit_sm_pdu2] = yield smpp_helper.wait_for_pdus(2)
        self.assertEqual(short_message(submit_sm_pdu1), 'hello world 1')
        self.assertEqual(short_message(submit_sm_pdu2), 'hello world 2')
        self.assertNoResult(msg3_d)

        with LogCatcher(message="No longer throttling outbound") as lc:
            self.clock.advance(1)
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)

        self.assertFalse(transport.throttled)
        yield msg3_d
        [submit_sm_pdu3] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(short_message(submit_sm_pdu3), 'hello world 3')

    @inlineCallbacks
    def test_mt_sms_reconnect_while_tps_throttled(self):
        """
        If we reconnect while throttled due to the tps limit, we don't try to
        unthrottle before the connection is in a suitable state.
        """
        smpp_helper = yield self.get_smpp_helper(config={
            'mt_tps': 2,
        })
        transport = smpp_helper.transport

        with LogCatcher(message="Throttling outbound messages.") as lc:
            yield self.tx_helper.make_dispatch_outbound('hello world 1')
            yield self.tx_helper.make_dispatch_outbound('hello world 2')
            msg3_d = self.tx_helper.make_dispatch_outbound('hello world 3')
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)

        self.assertTrue(transport.throttled)
        [submit_sm_pdu1, submit_sm_pdu2] = yield smpp_helper.wait_for_pdus(2)
        self.assertEqual(short_message(submit_sm_pdu1), 'hello world 1')
        self.assertEqual(short_message(submit_sm_pdu2), 'hello world 2')
        self.assertNoResult(msg3_d)

        # Drop SMPP connection and check throttling.
        with LogCatcher(message="Can't stop throttling while unbound") as lc:
            transport.service.stopService()
            self.clock.advance(1)
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)
        self.assertTrue(transport.throttled)

        # Reconnect (but don't bind) and check throttling.
        with LogCatcher(message="Can't stop throttling while unbound") as lc:
            smpp_helper.transport.service.startService()
            protocol = yield transport.service.get_protocol()
            protocol.makeConnection(self.string_transport)
            [bind_pdu] = yield smpp_helper.wait_for_pdus(1)
            self.assertTrue(
                bind_pdu["header"]["command_id"].startswith("bind_"))
            self.clock.advance(1)
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)
        self.assertTrue(transport.throttled)

        # Bind and check throttling.
        with LogCatcher(message="No longer throttling outbound") as lc:
            yield bind_protocol(
                self.string_transport, protocol, bind_pdu=bind_pdu)
            self.clock.advance(1)
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)

        self.assertFalse(transport.throttled)
        [submit_sm_pdu2] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(short_message(submit_sm_pdu2), 'hello world 3')

    @inlineCallbacks
    def test_mt_sms_queue_full(self):
        smpp_helper = yield self.get_smpp_helper()
        transport_config = smpp_helper.transport.get_static_config()
        msg = self.tx_helper.make_outbound('hello world')

        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                         message_id='foo',
                         command_status='ESME_RMSGQFUL'))

        self.clock.advance(transport_config.throttle_delay)

        [submit_sm_pdu_retry] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu_retry),
                         message_id='bar',
                         command_status='ESME_ROK'))

        self.assertTrue(seq_no(submit_sm_pdu_retry) > seq_no(submit_sm_pdu))
        self.assertEqual(short_message(submit_sm_pdu), 'hello world')
        self.assertEqual(short_message(submit_sm_pdu_retry), 'hello world')
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_mt_sms_unicode(self):
        smpp_helper = yield self.get_smpp_helper()
        msg = self.tx_helper.make_outbound(u'Zoë')
        yield self.tx_helper.dispatch_outbound(msg)
        [pdu] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(command_id(pdu), 'submit_sm')
        self.assertEqual(short_message(pdu), 'Zo\xc3\xab')

    @inlineCallbacks
    def test_mt_sms_multipart_long(self):
        smpp_helper = yield self.get_smpp_helper(config={
            'submit_short_message_processor_config': {
                'send_long_messages': True,
            }
        })
        # SMPP specifies that messages longer than 254 bytes should
        # be put in the message_payload field using TLVs
        content = '1' * 255
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(pdu_tlv(submit_sm, 'message_payload').decode('hex'),
                         content)

    @inlineCallbacks
    def test_mt_sms_multipart_udh(self):
        smpp_helper = yield self.get_smpp_helper(config={
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield smpp_helper.wait_for_pdus(2)

        self.assertEqual(
            submit_sm1["body"]["mandatory_parameters"]["esm_class"], 0x40)
        self.assertEqual(
            submit_sm2["body"]["mandatory_parameters"]["esm_class"], 0x40)

        udh_hlen, udh_tag, udh_len, udh_ref, udh_tot, udh_seq = [
            ord(octet) for octet in short_message(submit_sm1)[:6]]
        self.assertEqual(5, udh_hlen)
        self.assertEqual(0, udh_tag)
        self.assertEqual(3, udh_len)
        self.assertEqual(udh_tot, 2)
        self.assertEqual(udh_seq, 1)

        _, _, _, ref_to_udh_ref, _, udh_seq = [
            ord(octet) for octet in short_message(submit_sm2)[:6]]
        self.assertEqual(ref_to_udh_ref, udh_ref)
        self.assertEqual(udh_seq, 2)

    @inlineCallbacks
    def test_mt_sms_multipart_udh_one_part(self):
        """
        Messages that fit in a single part should not have a UDH.
        """
        smpp_helper = yield self.get_smpp_helper(config={
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        content = "1" * 158
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm] = yield smpp_helper.wait_for_pdus(1)

        self.assertEqual(
            submit_sm["body"]["mandatory_parameters"]["esm_class"], 0)
        self.assertEqual(short_message(submit_sm), "1" * 158)

    @inlineCallbacks
    def test_mt_sms_multipart_sar(self):
        smpp_helper = yield self.get_smpp_helper(config={
            'submit_short_message_processor_config': {
                'send_multipart_sar': True,
            }
        })
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield smpp_helper.wait_for_pdus(2)

        ref_num = pdu_tlv(submit_sm1, 'sar_msg_ref_num')
        self.assertEqual(pdu_tlv(submit_sm1, 'sar_total_segments'), 2)
        self.assertEqual(pdu_tlv(submit_sm1, 'sar_segment_seqnum'), 1)

        self.assertEqual(pdu_tlv(submit_sm2, 'sar_msg_ref_num'), ref_num)
        self.assertEqual(pdu_tlv(submit_sm2, 'sar_total_segments'), 2)
        self.assertEqual(pdu_tlv(submit_sm2, 'sar_segment_seqnum'), 2)

    @inlineCallbacks
    def test_mt_sms_multipart_sar_one_part(self):
        """
        Messages that fit in a single part should not have SAR params set.
        """
        smpp_helper = yield self.get_smpp_helper(config={
            'submit_short_message_processor_config': {
                'send_multipart_sar': True,
            }
        })
        content = '1' * 158
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm] = yield smpp_helper.wait_for_pdus(1)

        self.assertEqual(unpacked_pdu_opts(submit_sm), {})
        self.assertEqual(short_message(submit_sm), "1" * 158)

    @inlineCallbacks
    def test_mt_sms_multipart_ack(self):
        smpp_helper = yield self.get_smpp_helper(config={
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield smpp_helper.wait_for_pdus(2)
        smpp_helper.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm1), message_id='foo'))
        smpp_helper.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm2), message_id='bar'))
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['sent_message_id'], 'bar,foo')

    @inlineCallbacks
    def test_mt_sms_multipart_fail_first_part(self):
        smpp_helper = yield self.get_smpp_helper(config={
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield smpp_helper.wait_for_pdus(2)
        smpp_helper.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm1),
                         message_id='foo', command_status='ESME_RSUBMITFAIL'))
        smpp_helper.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm2), message_id='bar'))
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_mt_sms_multipart_fail_second_part(self):
        smpp_helper = yield self.get_smpp_helper(config={
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield smpp_helper.wait_for_pdus(2)
        smpp_helper.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm1), message_id='foo'))
        smpp_helper.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm2),
                         message_id='bar', command_status='ESME_RSUBMITFAIL'))
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_mt_sms_multipart_fail_no_remote_id(self):
        smpp_helper = yield self.get_smpp_helper(config={
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield smpp_helper.wait_for_pdus(2)
        smpp_helper.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm1),
                         message_id='', command_status='ESME_RINVDSTADR'))
        smpp_helper.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm2),
                         message_id='', command_status='ESME_RINVDSTADR'))
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_message_persistence(self):
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        message_stash = transport.message_stash
        config = transport.get_static_config()

        msg = self.tx_helper.make_outbound("hello world")
        yield message_stash.cache_message(msg)

        ttl = yield transport.redis.ttl(message_key(msg['message_id']))
        self.assertTrue(0 < ttl <= config.submit_sm_expiry)

        retrieved_msg = yield message_stash.get_cached_message(
            msg['message_id'])
        self.assertEqual(msg, retrieved_msg)
        yield message_stash.delete_cached_message(msg['message_id'])
        self.assertEqual(
            (yield message_stash.get_cached_message(msg['message_id'])),
            None)

    @inlineCallbacks
    def test_message_clearing(self):
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        message_stash = transport.message_stash
        msg = self.tx_helper.make_outbound('hello world')
        yield message_stash.set_sequence_number_message_id(
            3, msg['message_id'])
        yield message_stash.cache_message(msg)
        yield smpp_helper.handle_pdu(SubmitSMResp(sequence_number=3,
                                                  message_id='foo',
                                                  command_status='ESME_ROK'))
        self.assertEqual(
            None,
            (yield message_stash.get_cached_message(msg['message_id'])))

    @inlineCallbacks
    def test_link_remote_message_id(self):
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        config = transport.get_static_config()

        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)

        [pdu] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(pdu),
                         message_id='foo',
                         command_status='ESME_ROK'))
        self.assertEqual(
            msg['message_id'],
            (yield transport.message_stash.get_internal_message_id('foo')))

        ttl = yield transport.redis.ttl(remote_message_key('foo'))
        self.assertTrue(0 < ttl <= config.third_party_id_expiry)

    @inlineCallbacks
    def test_out_of_order_responses(self):
        smpp_helper = yield self.get_smpp_helper()
        yield self.tx_helper.make_dispatch_outbound("msg 1", message_id='444')
        [submit_sm1] = yield smpp_helper.wait_for_pdus(1)
        response1 = SubmitSMResp(seq_no(submit_sm1), "3rd_party_id_1")

        yield self.tx_helper.make_dispatch_outbound("msg 2", message_id='445')
        [submit_sm2] = yield smpp_helper.wait_for_pdus(1)
        response2 = SubmitSMResp(seq_no(submit_sm2), "3rd_party_id_2")

        # respond out of order - just to keep things interesting
        yield smpp_helper.handle_pdu(response2)
        yield smpp_helper.handle_pdu(response1)

        [ack1, ack2] = yield self.tx_helper.wait_for_dispatched_events(2)
        self.assertEqual(ack1['user_message_id'], '445')
        self.assertEqual(ack1['sent_message_id'], '3rd_party_id_2')
        self.assertEqual(ack2['user_message_id'], '444')
        self.assertEqual(ack2['sent_message_id'], '3rd_party_id_1')

    @inlineCallbacks
    def test_delivery_report_for_unknown_message(self):
        dr = self.DR_TEMPLATE % ('foo',)
        deliver = DeliverSM(1, short_message=dr, esm_class=4)
        smpp_helper = yield self.get_smpp_helper()
        with LogCatcher(message="Failed to retrieve message id") as lc:
            yield smpp_helper.handle_pdu(deliver)
            [warning] = lc.logs
            self.assertEqual(warning['message'],
                             ("Failed to retrieve message id for delivery "
                              "report. Delivery report from %s "
                              "discarded." % self.tx_helper.transport_name,))

    @inlineCallbacks
    def test_reconnect(self):
        smpp_helper = yield self.get_smpp_helper(bind=False)
        transport = smpp_helper.transport
        connector = transport.connectors[transport.transport_name]
        self.assertTrue(connector._consumers['outbound'].paused)
        yield self.create_smpp_bind(transport)
        self.assertFalse(connector._consumers['outbound'].paused)
        transport.service.stopService()
        self.assertTrue(connector._consumers['outbound'].paused)
        transport.service.startService()
        self.assertTrue(connector._consumers['outbound'].paused)
        yield self.create_smpp_bind(transport)
        self.assertFalse(connector._consumers['outbound'].paused)

    @inlineCallbacks
    def test_bind_params(self):
        smpp_helper = yield self.get_smpp_helper(bind=False, config={
            'system_id': 'myusername',
            'password': 'mypasswd',
            'system_type': 'SMPP',
            'interface_version': '33',
            'address_range': '*12345',
        })
        transport = smpp_helper.transport
        bind_pdu = yield self.create_smpp_bind(transport)
        # This test runs for multiple bind types, so we only assert on the
        # common prefix of the command.
        self.assertEqual(bind_pdu['header']['command_id'][:5], 'bind_')
        self.assertEqual(bind_pdu['body'], {'mandatory_parameters': {
            'system_id': 'myusername',
            'password': 'mypasswd',
            'system_type': 'SMPP',
            'interface_version': '33',
            'address_range': '*12345',
            'addr_ton': 'unknown',
            'addr_npi': 'unknown',
        }})

    @inlineCallbacks
    def test_bind_params_long_password(self):
        smpp_helper = yield self.get_smpp_helper(bind=False, config={
            'system_id': 'myusername',
            'password': 'mypass789',
            'system_type': 'SMPP',
            'interface_version': '33',
            'address_range': '*12345',
        })
        transport = smpp_helper.transport
        lc = LogCatcher(message="Password longer than 8 characters,")
        with lc:
            bind_pdu = yield self.create_smpp_bind(transport)
        # This test runs for multiple bind types, so we only assert on the
        # common prefix of the command.
        self.assertEqual(bind_pdu['header']['command_id'][:5], 'bind_')
        self.assertEqual(bind_pdu['body'], {'mandatory_parameters': {
            'system_id': 'myusername',
            'password': 'mypass78',
            'system_type': 'SMPP',
            'interface_version': '33',
            'address_range': '*12345',
            'addr_ton': 'unknown',
            'addr_npi': 'unknown',
        }})

        # Check that the truncation was logged.
        [warning] = lc.logs
        expected_msg = "Password longer than 8 characters, truncating."
        self.assertEqual(warning['message'], (expected_msg,))

    @inlineCallbacks
    def test_default_bind_params(self):
        smpp_helper = yield self.get_smpp_helper(bind=False, config={})
        transport = smpp_helper.transport
        bind_pdu = yield self.create_smpp_bind(transport)
        # This test runs for multiple bind types, so we only assert on the
        # common prefix of the command.
        self.assertEqual(bind_pdu['header']['command_id'][:5], 'bind_')
        self.assertEqual(bind_pdu['body'], {'mandatory_parameters': {
            'system_id': 'foo',  # Mandatory param, defaulted by helper.
            'password': 'bar',   # Mandatory param, defaulted by helper.
            'system_type': '',
            'interface_version': '34',
            'address_range': '',
            'addr_ton': 'unknown',
            'addr_npi': 'unknown',
        }})

    @inlineCallbacks
    def test_startup_with_backlog(self):
        smpp_helper = yield self.get_smpp_helper(bind=False)

        for i in range(2):
            msg = self.tx_helper.make_outbound('hello world %s' % (i,))
            yield self.tx_helper.dispatch_outbound(msg)

        yield self.create_smpp_bind(smpp_helper.transport)
        [submit_sm1, submit_sm2] = yield smpp_helper.wait_for_pdus(2)
        self.assertEqual(short_message(submit_sm1), 'hello world 0')
        self.assertEqual(short_message(submit_sm2), 'hello world 1')


class SmppTransmitterTransportTestCase(SmppTransceiverTransportTestCase):
    transport_class = SmppTransmitterTransport


class SmppReceiverTransportTestCase(SmppTransceiverTransportTestCase):
    transport_class = SmppReceiverTransport


class SmppTransceiverTransportWithOldConfigTestCase(
        SmppTransceiverTransportTestCase):

    transport_class = SmppTransceiverTransportWithOldConfig

    def setUp(self):

        self.clock = Clock()
        self.patch(self.transport_class, 'service_class', DummyService)
        self.patch(self.transport_class, 'clock', self.clock)

        self.string_transport = proto_helpers.StringTransport()

        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.default_config = {
            'transport_name': self.tx_helper.transport_name,
            'twisted_endpoint': 'tcp:host=127.0.0.1:port=0',
            'system_id': 'foo',
            'password': 'bar',
            'data_coding_overrides': {
                0: 'utf-8',
            }
        }

    @inlineCallbacks
    def get_transport(self, config={}, bind=True):
        """
        The test cases assume the new config, this flattens the
        config key word arguments value to match an old config
        layout without the processor configs.
        """

        cfg = self.default_config.copy()

        processor_config_keys = [
            'submit_short_message_processor_config',
            'deliver_short_message_processor_config',
            'delivery_report_processor_config',
        ]

        for config_key in processor_config_keys:
            processor_config = config.pop(config_key, {})
            for name, value in processor_config.items():
                cfg[name] = value

        # Update with all remaining (non-processor) config values
        cfg.update(config)
        transport = yield self.tx_helper.get_transport(cfg)
        if bind:
            yield self.create_smpp_bind(transport)
        returnValue(transport)


class TataUssdSmppTransportTestCase(SmppTransportTestCase):

    transport_class = SmppTransceiverTransport

    @inlineCallbacks
    def test_submit_and_deliver_ussd_continue(self):
        smpp_helper = yield self.get_smpp_helper()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", transport_type="ussd")

        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(command_id(submit_sm_pdu), 'submit_sm')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'ussd_service_op'), '02')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'its_session_info'), '0000')

        # Server delivers a USSD message to the Client
        pdu = DeliverSM(seq_no(submit_sm_pdu) + 1, short_message="reply!")
        pdu.add_optional_parameter('ussd_service_op', '02')
        pdu.add_optional_parameter('its_session_info', '0000')

        yield smpp_helper.handle_pdu(pdu)

        [mess] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(mess['content'], "reply!")
        self.assertEqual(mess['transport_type'], "ussd")
        self.assertEqual(mess['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_submit_and_deliver_ussd_close(self):
        smpp_helper = yield self.get_smpp_helper()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", transport_type="ussd",
            session_event=TransportUserMessage.SESSION_CLOSE)

        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(command_id(submit_sm_pdu), 'submit_sm')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'ussd_service_op'), '02')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'its_session_info'), '0001')

        # Server delivers a USSD message to the Client
        pdu = DeliverSM(seq_no(submit_sm_pdu) + 1, short_message="reply!")
        pdu.add_optional_parameter('ussd_service_op', '02')
        pdu.add_optional_parameter('its_session_info', '0001')

        yield smpp_helper.handle_pdu(pdu)

        [mess] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(mess['content'], "reply!")
        self.assertEqual(mess['transport_type'], "ussd")
        self.assertEqual(mess['session_event'],
                         TransportUserMessage.SESSION_CLOSE)


class TestSubmitShortMessageProcessorConfig(VumiTestCase):

    def get_config(self, config_dict):
        return SubmitShortMessageProcessor.CONFIG_CLASS(config_dict)

    def assert_config_error(self, config_dict):
        try:
            self.get_config(config_dict)
            self.fail("ConfigError not raised.")
        except ConfigError as err:
            return err.args[0]

    def test_long_message_params(self):
        self.get_config({})
        self.get_config({'send_long_messages': True})
        self.get_config({'send_multipart_sar': True})
        self.get_config({'send_multipart_udh': True})
        errmsg = self.assert_config_error({
            'send_long_messages': True,
            'send_multipart_sar': True,
        })
        self.assertEqual(errmsg, (
            "The following parameters are mutually exclusive: "
            "send_long_messages, send_multipart_sar"))
        errmsg = self.assert_config_error({
            'send_long_messages': True,
            'send_multipart_sar': True,
            'send_multipart_udh': True,
        })
        self.assertEqual(errmsg, (
            "The following parameters are mutually exclusive: "
            "send_long_messages, send_multipart_sar, send_multipart_udh"))
