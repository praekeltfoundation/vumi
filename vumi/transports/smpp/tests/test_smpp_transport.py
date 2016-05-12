# -*- coding: utf-8 -*-

import logging

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import Clock

from smpp.pdu_builder import DeliverSM, SubmitSMResp
from vumi.config import ConfigError
from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import LogCatcher
from vumi.transports.smpp.smpp_transport import (
    message_key, remote_message_key, multipart_info_key, sequence_number_key,
    SmppTransceiverTransport, SmppTransmitterTransport, SmppReceiverTransport,
    SmppTransceiverTransportWithOldConfig)
from vumi.transports.smpp.pdu_utils import (
    pdu_ok, short_message, command_id, seq_no, pdu_tlv, unpacked_pdu_opts)
from vumi.transports.smpp.processors import SubmitShortMessageProcessor
from vumi.transports.smpp.tests.fake_smsc import FakeSMSC
from vumi.transports.tests.helpers import TransportHelper


class TestSmppTransportConfig(VumiTestCase):
    def test_host_port_fallback(self):
        """
        Old-style 'host' and 'port' fields are still supported in configs.
        """

        def parse_config(extra_config):
            config = {
                'transport_name': 'name',
                'system_id': 'foo',
                'password': 'bar',
            }
            config.update(extra_config)
            return SmppTransceiverTransport.CONFIG_CLASS(config, static=True)

        # If we don't provide an endpoint config, we get an error.
        self.assertRaises(ConfigError, parse_config, {})

        # If we do provide an endpoint config, we get an endpoint.
        cfg = {'twisted_endpoint': 'tcp:host=example.com:port=1337'}
        self.assertNotEqual(parse_config(cfg).twisted_endpoint.connect, None)

        # If we provide host and port configs, we get an endpoint.
        cfg = {'host': 'example.com', 'port': 1337}
        self.assertNotEqual(parse_config(cfg).twisted_endpoint.connect, None)


class SmppTransportTestCase(VumiTestCase):

    DR_TEMPLATE = ("id:%s sub:... dlvrd:... submit date:200101010030"
                   " done date:200101020030 stat:DELIVRD err:... text:Meep")
    DR_MINIMAL_TEMPLATE = "id:%s stat:DELIVRD text:Meep"
    transport_class = None

    def setUp(self):
        self.clock = Clock()
        self.fake_smsc = FakeSMSC()
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.default_config = {
            'transport_name': self.tx_helper.transport_name,
            'worker_name': self.tx_helper.transport_name,
            'twisted_endpoint': self.fake_smsc.endpoint,
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

    def _get_transport_config(self, config):
        """
        This is overridden in a subclass.
        """
        cfg = self.default_config.copy()
        cfg.update(config)
        return cfg

    @inlineCallbacks
    def get_transport(self, config={}, bind=True):
        cfg = self._get_transport_config(config)
        transport = yield self.tx_helper.get_transport(cfg, start=False)
        transport.clock = self.clock
        yield transport.startWorker()
        self.clock.advance(0)
        if bind:
            yield self.fake_smsc.bind()
        returnValue(transport)


class SmppTransceiverTransportTestCase(SmppTransportTestCase):

    transport_class = SmppTransceiverTransport

    @inlineCallbacks
    def test_setup_transport(self):
        transport = yield self.get_transport(bind=False)
        protocol = yield transport.service.get_protocol()
        self.assertEqual(protocol.is_bound(), False)
        yield self.fake_smsc.bind()
        self.assertEqual(protocol.is_bound(), True)

    @inlineCallbacks
    def test_mo_sms(self):
        yield self.get_transport()
        self.fake_smsc.send_mo(
            sequence_number=1, short_message='foo', source_addr='123',
            destination_addr='456')
        deliver_sm_resp = yield self.fake_smsc.await_pdu()
        self.assertTrue(pdu_ok(deliver_sm_resp))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], 'foo')
        self.assertEqual(msg['from_addr'], '123')
        self.assertEqual(msg['to_addr'], '456')
        self.assertEqual(msg['transport_type'], 'sms')

    @inlineCallbacks
    def test_mo_sms_empty_sms_allowed(self):
        yield self.get_transport({
            'deliver_short_message_processor_config': {
                'allow_empty_messages': True,
            }
        })
        self.fake_smsc.send_mo(
            sequence_number=1, short_message='', source_addr='123',
            destination_addr='456')
        deliver_sm_resp = yield self.fake_smsc.await_pdu()
        self.assertTrue(pdu_ok(deliver_sm_resp))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], '')

    @inlineCallbacks
    def test_mo_sms_empty_sms_disallowed(self):
        yield self.get_transport()
        with LogCatcher(message=r"^(Not all parts|WARNING)") as lc:
            self.fake_smsc.send_mo(
                sequence_number=1, short_message='', source_addr='123',
                destination_addr='456')
            deliver_sm_resp = yield self.fake_smsc.await_pdu()

        self.assertFalse(pdu_ok(deliver_sm_resp))

        # check that failure to process delivery report was logged
        self.assertEqual(lc.messages(), [
            "WARNING: Not decoding `None` message with data_coding=1",
            "Not all parts of the PDU were able to be decoded. "
            "Responding with ESME_RDELIVERYFAILURE.",
        ])
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

        inbound = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(inbound, [])
        events = self.tx_helper.get_dispatched_events()
        self.assertEqual(events, [])

    @inlineCallbacks
    def test_mo_delivery_report_pdu_opt_params(self):
        """
        We always treat a message with the optional PDU params set as a
        delivery report.
        """
        transport = yield self.get_transport()
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        pdu = DeliverSM(sequence_number=1, esm_class=4)
        pdu.add_optional_parameter('receipted_message_id', 'foo')
        pdu.add_optional_parameter('message_state', 2)
        yield self.fake_smsc.handle_pdu(pdu)

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
        transport = yield self.get_transport()
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        pdu = DeliverSM(sequence_number=1)
        pdu.add_optional_parameter('receipted_message_id', 'foo')
        pdu.add_optional_parameter('message_state', 2)
        yield self.fake_smsc.handle_pdu(pdu)

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
        transport = yield self.get_transport()
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        self.fake_smsc.send_mo(
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
        transport = yield self.get_transport()
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        self.fake_smsc.send_mo(
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
        transport = yield self.get_transport()
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        short_message = (
            "id:foo sub:... dlvrd:... submit date:200101010030"
            " done date:200101020030 stat:FAILED err:042 text:Meep")
        self.fake_smsc.send_mo(
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
        transport = yield self.get_transport()
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        self.fake_smsc.send_mo(
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
        transport = yield self.get_transport()
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        content = (
            "id:%s sub:null dlvrd:null submit date:200101010030"
            " done date:200101020030 stat:DELIVRD err:null text:Meep")
        self.fake_smsc.send_mo(
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
        transport = yield self.get_transport()
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        lc = LogCatcher(message="esm_class 4 indicates")
        with lc:
            self.fake_smsc.send_mo(
                sequence_number=1, source_addr='123', destination_addr='456',
                short_message="foo", esm_class=4)
            yield self.fake_smsc.await_pdu()

        # check that failure to process delivery report was logged
        [warning] = lc.logs
        self.assertEqual(
            warning["message"][0],
            "esm_class 4 indicates delivery report, but content does not"
            " match regex: 'foo'")
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

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
        transport = yield self.get_transport()
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        lc = LogCatcher(message="esm_class 4 indicates")
        with lc:
            self.fake_smsc.send_mo(
                sequence_number=1, source_addr='123', destination_addr='456',
                short_message=None, esm_class=4)
            yield self.fake_smsc.await_pdu()

        # check that failure to process delivery report was logged
        [warning] = lc.logs
        self.assertEqual(
            warning["message"][0],
            "esm_class 4 indicates delivery report, but content does not"
            " match regex: None")
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

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
        transport = yield self.get_transport({
            "delivery_report_processor_config": {
                "delivery_report_use_esm_class": False,
            }
        })
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        self.fake_smsc.send_mo(
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
        transport = yield self.get_transport({
            "delivery_report_processor_config": {
                "delivery_report_use_esm_class": False,
            }
        })
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        self.fake_smsc.send_mo(
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
        transport = yield self.get_transport({
            "delivery_report_processor_config": {
                "delivery_report_use_esm_class": False,
            }
        })
        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        content = (
            "id:%s sub:null dlvrd:null submit date:200101010030"
            " done date:200101020030 stat:DELIVRD err:null text:Meep")
        self.fake_smsc.send_mo(
            sequence_number=1, short_message=content % ("foo",),
            source_addr='123', destination_addr='456', esm_class=0)

        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['user_message_id'], 'bar')

    @inlineCallbacks
    def test_mo_sms_unicode(self):
        yield self.get_transport()
        self.fake_smsc.send_mo(
            sequence_number=1, short_message='Zo\xc3\xab', data_coding=0)
        deliver_sm_resp = yield self.fake_smsc.await_pdu()
        self.assertTrue(pdu_ok(deliver_sm_resp))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], u'Zoë')

    @inlineCallbacks
    def test_mo_sms_multipart_long(self):
        yield self.get_transport()
        content = '1' * 255

        pdu = DeliverSM(sequence_number=1)
        pdu.add_optional_parameter('message_payload', content.encode('hex'))
        self.fake_smsc.send_pdu(pdu)

        deliver_sm_resp = yield self.fake_smsc.await_pdu()
        self.assertEqual(1, seq_no(deliver_sm_resp))
        self.assertTrue(pdu_ok(deliver_sm_resp))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], content)

    @inlineCallbacks
    def test_mo_sms_multipart_udh(self):
        yield self.get_transport()
        deliver_sm_resps = []
        self.fake_smsc.send_mo(
            sequence_number=1, short_message="\x05\x00\x03\xff\x03\x01back")
        deliver_sm_resps.append((yield self.fake_smsc.await_pdu()))
        self.fake_smsc.send_mo(
            sequence_number=2, short_message="\x05\x00\x03\xff\x03\x02 at")
        deliver_sm_resps.append((yield self.fake_smsc.await_pdu()))
        self.fake_smsc.send_mo(
            sequence_number=3, short_message="\x05\x00\x03\xff\x03\x03 you")
        deliver_sm_resps.append((yield self.fake_smsc.await_pdu()))
        self.assertEqual([1, 2, 3], map(seq_no, deliver_sm_resps))
        self.assertTrue(all(map(pdu_ok, deliver_sm_resps)))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], u'back at you')

    @inlineCallbacks
    def test_mo_sms_multipart_udh_out_of_order(self):
        yield self.get_transport()
        deliver_sm_resps = []
        self.fake_smsc.send_mo(
            sequence_number=1, short_message="\x05\x00\x03\xff\x03\x01back")
        deliver_sm_resps.append((yield self.fake_smsc.await_pdu()))

        self.fake_smsc.send_mo(
            sequence_number=3, short_message="\x05\x00\x03\xff\x03\x03 you")
        deliver_sm_resps.append((yield self.fake_smsc.await_pdu()))

        self.fake_smsc.send_mo(
            sequence_number=2, short_message="\x05\x00\x03\xff\x03\x02 at")
        deliver_sm_resps.append((yield self.fake_smsc.await_pdu()))

        self.assertEqual([1, 3, 2], map(seq_no, deliver_sm_resps))
        self.assertTrue(all(map(pdu_ok, deliver_sm_resps)))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], u'back at you')

    @inlineCallbacks
    def test_mo_sms_multipart_sar(self):
        yield self.get_transport()
        deliver_sm_resps = []

        pdu1 = DeliverSM(sequence_number=1, short_message='back')
        pdu1.add_optional_parameter('sar_msg_ref_num', 1)
        pdu1.add_optional_parameter('sar_total_segments', 3)
        pdu1.add_optional_parameter('sar_segment_seqnum', 1)

        self.fake_smsc.send_pdu(pdu1)
        deliver_sm_resps.append((yield self.fake_smsc.await_pdu()))

        pdu2 = DeliverSM(sequence_number=2, short_message=' at')
        pdu2.add_optional_parameter('sar_msg_ref_num', 1)
        pdu2.add_optional_parameter('sar_total_segments', 3)
        pdu2.add_optional_parameter('sar_segment_seqnum', 2)

        self.fake_smsc.send_pdu(pdu2)
        deliver_sm_resps.append((yield self.fake_smsc.await_pdu()))

        pdu3 = DeliverSM(sequence_number=3, short_message=' you')
        pdu3.add_optional_parameter('sar_msg_ref_num', 1)
        pdu3.add_optional_parameter('sar_total_segments', 3)
        pdu3.add_optional_parameter('sar_segment_seqnum', 3)

        self.fake_smsc.send_pdu(pdu3)
        deliver_sm_resps.append((yield self.fake_smsc.await_pdu()))

        self.assertEqual([1, 2, 3], map(seq_no, deliver_sm_resps))
        self.assertTrue(all(map(pdu_ok, deliver_sm_resps)))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], u'back at you')

    @inlineCallbacks
    def test_mo_bad_encoding(self):
        yield self.get_transport()

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

        yield self.fake_smsc.handle_pdu(bad_pdu)
        yield self.fake_smsc.handle_pdu(good_pdu)
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
        yield self.get_transport()

        lc = LogCatcher(message="Failed to retrieve message id")
        with lc:
            yield self.fake_smsc.handle_pdu(
                DeliverSM(sequence_number=1, esm_class=4,
                          short_message=self.DR_TEMPLATE % ('foo',)))

        # check that failure to send delivery report was logged
        [warning] = lc.logs
        expected_msg = (
            "Failed to retrieve message id for delivery report. Delivery"
            " report from %s discarded.") % (self.tx_helper.transport_name,)
        self.assertEqual(warning['message'], (expected_msg,))
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

    @inlineCallbacks
    def test_mt_sms(self):
        yield self.get_transport()
        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)
        pdu = yield self.fake_smsc.await_pdu()
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
        yield self.get_transport({
            'submit_short_message_processor_config': {
                'submit_sm_encoding': 'latin1',
            }
        })
        yield self.tx_helper.make_dispatch_outbound(u'Zoë destroyer of Ascii!')
        submit_sm_pdu = yield self.fake_smsc.await_pdu()
        self.assertEqual(
            short_message(submit_sm_pdu),
            u'Zoë destroyer of Ascii!'.encode('latin-1'))

    @inlineCallbacks
    def test_mt_sms_submit_sm_null_message(self):
        """
        We can successfully send a message with null content.
        """
        yield self.get_transport()
        msg = self.tx_helper.make_outbound(None)
        yield self.tx_helper.dispatch_outbound(msg)
        pdu = yield self.fake_smsc.await_pdu()
        self.assertEqual(command_id(pdu), 'submit_sm')
        self.assertEqual(short_message(pdu), None)

    @inlineCallbacks
    def test_submit_sm_data_coding(self):
        yield self.get_transport({
            'submit_short_message_processor_config': {
                'submit_sm_data_coding': 8
            }
        })
        yield self.tx_helper.make_dispatch_outbound("hello world")
        submit_sm_pdu = yield self.fake_smsc.await_pdu()
        params = submit_sm_pdu['body']['mandatory_parameters']
        self.assertEqual(params['data_coding'], 8)

    @inlineCallbacks
    def test_mt_sms_ack(self):
        yield self.get_transport()
        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)
        submit_sm_pdu = yield self.fake_smsc.await_pdu()
        self.fake_smsc.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                         message_id='foo'))
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['sent_message_id'], 'foo')

    @inlineCallbacks
    def assert_no_events(self):
        # NOTE: We can't test for the absence of an event in isolation but we
        #       can test that for the presence of a second event only.
        fail_msg = self.tx_helper.make_outbound('hello fail')
        yield self.tx_helper.dispatch_outbound(fail_msg)
        submit_sm_fail_pdu = yield self.fake_smsc.await_pdu()
        self.fake_smsc.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_fail_pdu),
                         message_id='__assert_no_events__',
                         command_status='ESME_RINVDSTADR'))
        [fail] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(fail['event_type'], 'nack')

    @inlineCallbacks
    def test_mt_sms_disabled_ack(self):
        yield self.get_transport({'disable_ack': True})
        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)
        submit_sm_pdu = yield self.fake_smsc.await_pdu()
        self.fake_smsc.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                         message_id='foo'))
        yield self.assert_no_events()

    @inlineCallbacks
    def test_mt_sms_nack(self):
        yield self.get_transport()
        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)
        submit_sm_pdu = yield self.fake_smsc.await_pdu()
        self.fake_smsc.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                         message_id='foo', command_status='ESME_RINVDSTADR'))
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['nack_reason'], 'ESME_RINVDSTADR')

    @inlineCallbacks
    def test_mt_sms_failure(self):
        yield self.get_transport()
        message = yield self.tx_helper.make_dispatch_outbound(
            "message", message_id='446')
        submit_sm = yield self.fake_smsc.await_pdu()
        response = SubmitSMResp(seq_no(submit_sm), "3rd_party_id_3",
                                command_status="ESME_RSUBMITFAIL")
        # A failure PDU might not have a body.
        response.obj.pop('body')
        self.fake_smsc.send_pdu(response)

        # There should be a nack
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)

        [failure] = yield self.tx_helper.get_dispatched_failures()
        self.assertEqual(failure['reason'], 'ESME_RSUBMITFAIL')
        self.assertEqual(failure['message'], message.payload)

    @inlineCallbacks
    def test_mt_sms_failure_with_no_reason(self):

        yield self.get_transport()
        message = yield self.tx_helper.make_dispatch_outbound(
            "message", message_id='446')
        submit_sm = yield self.fake_smsc.await_pdu()

        yield self.fake_smsc.handle_pdu(
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
        transport = yield self.get_transport()

        lc = LogCatcher(message="Failed to retrieve message id")
        with lc:
            yield self.fake_smsc.handle_pdu(
                SubmitSMResp(sequence_number=0xbad, message_id='bad'))

        # Make sure we didn't store 'None' in redis.
        message_stash = transport.message_stash
        message_id = yield message_stash.get_internal_message_id('bad')
        self.assertEqual(message_id, None)

        # check that failure to send ack/nack was logged
        [warning] = lc.logs
        expected_msg = (
            "Failed to retrieve message id for deliver_sm_resp. ack/nack"
            " from %s discarded.") % (self.tx_helper.transport_name,)
        self.assertEqual(warning['message'], (expected_msg,))
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

    @inlineCallbacks
    def test_mt_sms_throttled(self):
        transport = yield self.get_transport()
        transport_config = transport.get_static_config()
        msg = self.tx_helper.make_outbound('hello world')

        yield self.tx_helper.dispatch_outbound(msg)
        submit_sm_pdu = yield self.fake_smsc.await_pdu()
        with LogCatcher(message="Throttling outbound messages.") as lc:
            yield self.fake_smsc.handle_pdu(
                SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                             message_id='foo',
                             command_status='ESME_RTHROTTLED'))
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

        self.clock.advance(transport_config.throttle_delay)

        submit_sm_pdu_retry = yield self.fake_smsc.await_pdu()
        yield self.fake_smsc.handle_pdu(
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
        # messages to retry. After a non-throttle submit_sm_resp, that happens
        # with no delay.
        with LogCatcher(message="No longer throttling outbound") as lc:
            self.clock.advance(0)
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

    @inlineCallbacks
    def test_mt_sms_multipart_throttled(self):
        """
        When parts of a multipart message are throttled, we retry only those
        PDUs.
        """
        transport = yield self.get_transport({
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        transport_config = transport.get_static_config()
        msg = self.tx_helper.make_outbound('a' * 350)  # Three parts.

        yield self.tx_helper.dispatch_outbound(msg)
        [pdu1, pdu2, pdu3] = yield self.fake_smsc.await_pdus(3)
        self.assertEqual(short_message(pdu1)[4:6], "\x03\x01")
        self.assertEqual(short_message(pdu2)[4:6], "\x03\x02")
        self.assertEqual(short_message(pdu3)[4:6], "\x03\x03")
        # Let two parts through.
        yield self.fake_smsc.submit_sm_resp(pdu1)
        yield self.fake_smsc.submit_sm_resp(pdu2)
        self.assertEqual(transport.throttled, False)
        # Throttle the third part.
        yield self.fake_smsc.submit_sm_resp(
            pdu3, command_status='ESME_RTHROTTLED')
        self.assertEqual(transport.throttled, True)

        self.clock.advance(transport_config.throttle_delay)
        retry_pdu = yield self.fake_smsc.await_pdu()
        # Assume nothing else is incrementing seuqnce numbers.
        self.assertEqual(seq_no(retry_pdu), seq_no(pdu3) + 1)
        # The retry should be identical to pdu3 except for the sequence number.
        pdu3_retry = dict((k, v.copy()) for k, v in pdu3.iteritems())
        pdu3_retry['header']['sequence_number'] = seq_no(retry_pdu)
        self.assertEqual(retry_pdu, pdu3_retry)

        # Let the retry through.
        yield self.fake_smsc.submit_sm_resp(retry_pdu)
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(transport.throttled, True)
        # Prod the clock to notice there are no more retries and unthrottle.
        self.clock.advance(0)
        self.assertEqual(transport.throttled, False)

    @inlineCallbacks
    def test_mt_sms_throttle_while_throttled(self):
        transport = yield self.get_transport()
        transport_config = transport.get_static_config()
        msg1 = self.tx_helper.make_outbound('hello world 1')
        msg2 = self.tx_helper.make_outbound('hello world 2')

        yield self.tx_helper.dispatch_outbound(msg1)
        yield self.tx_helper.dispatch_outbound(msg2)
        [ssm_pdu1, ssm_pdu2] = yield self.fake_smsc.await_pdus(2)
        yield self.fake_smsc.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu1),
                         message_id='foo1', command_status='ESME_RTHROTTLED'))
        yield self.fake_smsc.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu2),
                         message_id='foo2', command_status='ESME_RTHROTTLED'))

        # Advance clock, still throttled.
        self.clock.advance(transport_config.throttle_delay)
        ssm_pdu1_retry1 = yield self.fake_smsc.await_pdu()
        yield self.fake_smsc.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu1_retry1),
                         message_id='bar1',
                         command_status='ESME_RTHROTTLED'))

        # Advance clock, message no longer throttled.
        self.clock.advance(transport_config.throttle_delay)
        ssm_pdu2_retry1 = yield self.fake_smsc.await_pdu()
        yield self.fake_smsc.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu2_retry1),
                         message_id='bar2',
                         command_status='ESME_ROK'))

        # Prod clock, message no longer throttled.
        self.clock.advance(0)
        ssm_pdu1_retry2 = yield self.fake_smsc.await_pdu()
        yield self.fake_smsc.handle_pdu(
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
        transport = yield self.get_transport(bind=False)
        yield self.fake_smsc.bind()
        transport_config = transport.get_static_config()
        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)
        ssm_pdu = yield self.fake_smsc.await_pdu()
        yield self.fake_smsc.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu),
                         message_id='foo1',
                         command_status='ESME_RTHROTTLED'))

        # Drop SMPP connection and check throttling.
        yield self.fake_smsc.disconnect()
        with LogCatcher(message="Can't check throttling while unbound") as lc:
            self.clock.advance(transport_config.throttle_delay)
        [logmsg] = lc.logs
        self.assertEqual(
            logmsg["message"][0],
            "Can't check throttling while unbound, trying later.")
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

        # Fast-forward to reconnect (but don't bind) and check throttling.
        self.clock.advance(transport.service.delay)
        bind_pdu = yield self.fake_smsc.await_pdu()
        self.assertTrue(
            bind_pdu["header"]["command_id"].startswith("bind_"))
        with LogCatcher(message="Can't check throttling while unbound") as lc:
            self.clock.advance(transport_config.throttle_delay)
        [logmsg] = lc.logs
        self.assertEqual(
            logmsg["message"][0],
            "Can't check throttling while unbound, trying later.")
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

        # Bind and check throttling.
        yield self.fake_smsc.bind(bind_pdu)
        with LogCatcher(message="Can't check throttling while unbound") as lc:
            self.clock.advance(transport_config.throttle_delay)
        self.assertEqual(lc.logs, [])

        ssm_pdu_retry = yield self.fake_smsc.await_pdu()
        yield self.fake_smsc.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(ssm_pdu_retry),
                         message_id='foo',
                         command_status='ESME_ROK'))
        self.assertEqual(short_message(ssm_pdu), 'hello world')
        self.assertEqual(short_message(ssm_pdu_retry), 'hello world')
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_mt_sms_tps_limits(self):
        transport = yield self.get_transport({'mt_tps': 2})

        with LogCatcher(message="Throttling outbound messages.") as lc:
            yield self.tx_helper.make_dispatch_outbound('hello world 1')
            yield self.tx_helper.make_dispatch_outbound('hello world 2')
            msg3_d = self.tx_helper.make_dispatch_outbound('hello world 3')
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

        self.assertTrue(transport.throttled)
        [submit_sm_pdu1, submit_sm_pdu2] = yield self.fake_smsc.await_pdus(2)
        self.assertEqual(short_message(submit_sm_pdu1), 'hello world 1')
        self.assertEqual(short_message(submit_sm_pdu2), 'hello world 2')
        self.assertNoResult(msg3_d)

        with LogCatcher(message="No longer throttling outbound") as lc:
            self.clock.advance(1)
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

        self.assertFalse(transport.throttled)
        yield msg3_d
        submit_sm_pdu3 = yield self.fake_smsc.await_pdu()
        self.assertEqual(short_message(submit_sm_pdu3), 'hello world 3')

    @inlineCallbacks
    def test_mt_sms_tps_limits_multipart(self):
        """
        TPS throttling counts PDUs, but finishes sending the current message.
        """
        transport = yield self.get_transport({
            'mt_tps': 3,
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            },
        })
        self.assertEqual(transport.throttled, False)

        with LogCatcher(message="Throttling outbound messages.") as lc:
            yield self.tx_helper.make_dispatch_outbound('1' * 200 + 'a')
            yield self.tx_helper.make_dispatch_outbound('2' * 200 + 'b')
            msg3_d = self.tx_helper.make_dispatch_outbound('3' * 200 + 'c')
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')
        self.assertEqual(transport.throttled, True)
        [pdu1_1, pdu1_2, pdu2_1, pdu2_2] = yield self.fake_smsc.await_pdus(4)
        self.assertEqual(short_message(pdu1_1)[-5:], '11111')
        self.assertEqual(short_message(pdu1_2)[-5:], '1111a')
        self.assertEqual(short_message(pdu2_1)[-5:], '22222')
        self.assertEqual(short_message(pdu2_2)[-5:], '2222b')
        self.assertNoResult(msg3_d)

        with LogCatcher(message="No longer throttling outbound") as lc:
            self.clock.advance(1)
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)
        self.assertEqual(transport.throttled, False)
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

        yield msg3_d
        [pdu3_1, pdu3_2] = yield self.fake_smsc.await_pdus(2)
        self.assertEqual(short_message(pdu3_1)[-5:], '33333')
        self.assertEqual(short_message(pdu3_2)[-5:], '3333c')

    @inlineCallbacks
    def test_mt_sms_reconnect_while_tps_throttled(self):
        """
        If we reconnect while throttled due to the tps limit, we don't try to
        unthrottle before the connection is in a suitable state.
        """
        transport = yield self.get_transport({'mt_tps': 2})

        with LogCatcher(message="Throttling outbound messages.") as lc:
            yield self.tx_helper.make_dispatch_outbound('hello world 1')
            yield self.tx_helper.make_dispatch_outbound('hello world 2')
            msg3_d = self.tx_helper.make_dispatch_outbound('hello world 3')
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

        self.assertTrue(transport.throttled)
        [submit_sm_pdu1, submit_sm_pdu2] = yield self.fake_smsc.await_pdus(2)
        self.assertEqual(short_message(submit_sm_pdu1), 'hello world 1')
        self.assertEqual(short_message(submit_sm_pdu2), 'hello world 2')
        self.assertNoResult(msg3_d)

        # Drop SMPP connection and check throttling.
        yield self.fake_smsc.disconnect()
        with LogCatcher(message="Can't stop throttling while unbound") as lc:
            self.clock.advance(1)
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)
        self.assertTrue(transport.throttled)
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

        # Fast-forward to reconnect (but don't bind) and check throttling.
        self.clock.advance(transport.service.delay)
        bind_pdu = yield self.fake_smsc.await_pdu()
        self.assertTrue(
            bind_pdu["header"]["command_id"].startswith("bind_"))
        with LogCatcher(message="Can't stop throttling while unbound") as lc:
            self.clock.advance(1)
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)
        self.assertTrue(transport.throttled)
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

        # Bind and check throttling.
        yield self.fake_smsc.bind(bind_pdu)
        with LogCatcher(message="No longer throttling outbound") as lc:
            self.clock.advance(1)
        [logmsg] = lc.logs
        self.assertEqual(logmsg['logLevel'], logging.INFO)
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

        self.assertFalse(transport.throttled)
        submit_sm_pdu2 = yield self.fake_smsc.await_pdu()
        self.assertEqual(short_message(submit_sm_pdu2), 'hello world 3')

    @inlineCallbacks
    def test_mt_sms_queue_full(self):
        transport = yield self.get_transport()
        transport_config = transport.get_static_config()
        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)
        submit_sm_pdu = yield self.fake_smsc.await_pdu()
        yield self.fake_smsc.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                         message_id='foo',
                         command_status='ESME_RMSGQFUL'))

        self.clock.advance(transport_config.throttle_delay)

        submit_sm_pdu_retry = yield self.fake_smsc.await_pdu()
        yield self.fake_smsc.handle_pdu(
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
    def test_mt_sms_remote_id_stored_only_on_rok(self):
        transport = yield self.get_transport()

        yield self.tx_helper.make_dispatch_outbound("msg1")
        submit_sm1 = yield self.fake_smsc.await_pdu()
        response = SubmitSMResp(
            seq_no(submit_sm1), "remote_1", command_status="ESME_RSUBMITFAIL")
        self.fake_smsc.send_pdu(response)

        yield self.tx_helper.make_dispatch_outbound("msg2")
        submit_sm2 = yield self.fake_smsc.await_pdu()
        response = SubmitSMResp(
            seq_no(submit_sm2), "remote_2", command_status="ESME_ROK")
        self.fake_smsc.send_pdu(response)

        yield self.tx_helper.wait_for_dispatched_events(2)

        self.assertFalse(
            (yield transport.redis.exists(remote_message_key('remote_1'))))

        self.assertTrue(
            (yield transport.redis.exists(remote_message_key('remote_2'))))

    @inlineCallbacks
    def test_mt_sms_unicode(self):
        yield self.get_transport()
        msg = self.tx_helper.make_outbound(u'Zoë')
        yield self.tx_helper.dispatch_outbound(msg)
        pdu = yield self.fake_smsc.await_pdu()
        self.assertEqual(command_id(pdu), 'submit_sm')
        self.assertEqual(short_message(pdu), 'Zo\xc3\xab')

    @inlineCallbacks
    def test_mt_sms_multipart_long(self):
        yield self.get_transport({
            'submit_short_message_processor_config': {
                'send_long_messages': True,
            }
        })
        # SMPP specifies that messages longer than 254 bytes should
        # be put in the message_payload field using TLVs
        content = '1' * 255
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        submit_sm = yield self.fake_smsc.await_pdu()
        self.assertEqual(pdu_tlv(submit_sm, 'message_payload').decode('hex'),
                         content)

    @inlineCallbacks
    def test_mt_sms_multipart_udh(self):
        """
        Sufficiently long messages are split into multiple PDUs with a UDH at
        the front of each.
        """
        transport = yield self.get_transport({
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield self.fake_smsc.await_pdus(2)

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

        # Our multipart_info Redis hash should contain the number of parts and
        # have an appropriate TTL.
        mstash = transport.message_stash
        multipart_info = yield mstash.get_multipart_info(msg['message_id'])
        self.assertEqual(multipart_info, {"parts": "2"})
        mpi_ttl = yield mstash.redis.ttl(multipart_info_key(msg['message_id']))
        self.assertTrue(
            mpi_ttl <= mstash.config.submit_sm_expiry,
            "mpi_ttl (%s) > submit_sm_expiry (%s)" % (
                mpi_ttl, mstash.config.submit_sm_expiry))

    @inlineCallbacks
    def test_mt_sms_multipart_udh_one_part(self):
        """
        Messages that fit in a single part should not have a UDH.
        """
        yield self.get_transport({
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        content = "1" * 158
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        submit_sm = yield self.fake_smsc.await_pdu()

        self.assertEqual(
            submit_sm["body"]["mandatory_parameters"]["esm_class"], 0)
        self.assertEqual(short_message(submit_sm), "1" * 158)

    @inlineCallbacks
    def test_mt_sms_multipart_sar(self):
        yield self.get_transport({
            'submit_short_message_processor_config': {
                'send_multipart_sar': True,
            }
        })
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield self.fake_smsc.await_pdus(2)

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
        yield self.get_transport({
            'submit_short_message_processor_config': {
                'send_multipart_sar': True,
            }
        })
        content = '1' * 158
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        submit_sm = yield self.fake_smsc.await_pdu()

        self.assertEqual(unpacked_pdu_opts(submit_sm), {})
        self.assertEqual(short_message(submit_sm), "1" * 158)

    @inlineCallbacks
    def test_mt_sms_multipart_ack(self):
        """
        When all PDUs of a multipart message have been successfully
        acknowledged, we clean up the relevant transient state and send an ack.
        """
        transport = yield self.get_transport({
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield self.fake_smsc.await_pdus(2)

        # Our multipart_info Redis hash should contain the number of parts and
        # have an appropriate TTL.
        mstash = transport.message_stash
        multipart_info = yield mstash.get_multipart_info(msg['message_id'])
        self.assertEqual(multipart_info, {"parts": "2"})
        mpi_ttl = yield mstash.redis.ttl(multipart_info_key(msg['message_id']))
        self.assertTrue(
            mpi_ttl <= mstash.config.submit_sm_expiry,
            "mpi_ttl (%s) > submit_sm_expiry (%s)" % (
                mpi_ttl, mstash.config.submit_sm_expiry))

        # We get one response per PDU, so we only send the ack after receiving
        # both responses.
        self.fake_smsc.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm1), message_id='foo'))
        self.fake_smsc.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm2), message_id='bar'))
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['sent_message_id'], 'bar,foo')

        # After all parts are acknowledged, our multipart_info hash should have
        # the details of the responses and a much shorter TTL.
        mstash = transport.message_stash
        multipart_info = yield mstash.get_multipart_info(msg['message_id'])
        self.assertEqual(multipart_info, {
            "parts": "2",
            "event_counter": "2",
            "part:foo": "ack",
            "part:bar": "ack",
        })
        mpi_ttl = yield mstash.redis.ttl(multipart_info_key(msg['message_id']))
        self.assertTrue(
            mpi_ttl <= mstash.config.completed_multipart_info_expiry,
            "mpi_ttl (%s) > completed_multipart_info_expiry (%s)" % (
                mpi_ttl, mstash.config.completed_multipart_info_expiry))

    @inlineCallbacks
    def test_mt_sms_multipart_fail_first_part(self):
        """
        When all PDUs of a multipart message have been acknowledged and at
        least one of them failed, we clean up the relevant transient state and
        send a nack.
        """
        transport = yield self.get_transport({
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield self.fake_smsc.await_pdus(2)

        # Our multipart_info Redis hash should contain the number of parts and
        # have an appropriate TTL.
        mstash = transport.message_stash
        multipart_info = yield mstash.get_multipart_info(msg['message_id'])
        self.assertEqual(multipart_info, {"parts": "2"})
        mpi_ttl = yield mstash.redis.ttl(multipart_info_key(msg['message_id']))
        self.assertTrue(
            mpi_ttl <= mstash.config.submit_sm_expiry,
            "mpi_ttl (%s) > submit_sm_expiry (%s)" % (
                mpi_ttl, mstash.config.submit_sm_expiry))

        # We get one response per PDU, so we only send the nack after receiving
        # both responses.
        self.fake_smsc.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm1),
                         message_id='foo', command_status='ESME_RSUBMITFAIL'))
        self.fake_smsc.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm2), message_id='bar'))
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

        # After all parts are acknowledged, our multipart_info hash should have
        # the details of the responses and a much shorter TTL.
        mstash = transport.message_stash
        multipart_info = yield mstash.get_multipart_info(msg['message_id'])
        self.assertEqual(multipart_info, {
            "parts": "2",
            "event_counter": "2",
            "part:foo": "fail",
            "part:bar": "ack",
            "event_result": "fail",
        })
        mpi_ttl = yield mstash.redis.ttl(multipart_info_key(msg['message_id']))
        self.assertTrue(
            mpi_ttl <= mstash.config.completed_multipart_info_expiry,
            "mpi_ttl (%s) > completed_multipart_info_expiry (%s)" % (
                mpi_ttl, mstash.config.completed_multipart_info_expiry))

    @inlineCallbacks
    def test_mt_sms_multipart_fail_second_part(self):
        yield self.get_transport({
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield self.fake_smsc.await_pdus(2)
        self.fake_smsc.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm1), message_id='foo'))
        self.fake_smsc.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm2),
                         message_id='bar', command_status='ESME_RSUBMITFAIL'))
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_mt_sms_multipart_fail_no_remote_id(self):
        yield self.get_transport({
            'submit_short_message_processor_config': {
                'send_multipart_udh': True,
            }
        })
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield self.fake_smsc.await_pdus(2)
        self.fake_smsc.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm1),
                         message_id='', command_status='ESME_RINVDSTADR'))
        self.fake_smsc.send_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm2),
                         message_id='', command_status='ESME_RINVDSTADR'))
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_message_persistence(self):
        transport = yield self.get_transport()
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
        transport = yield self.get_transport()
        message_stash = transport.message_stash
        msg = self.tx_helper.make_outbound('hello world')
        yield message_stash.set_sequence_number_message_id(
            3, msg['message_id'])
        yield message_stash.cache_message(msg)
        yield self.fake_smsc.handle_pdu(SubmitSMResp(
            sequence_number=3, message_id='foo', command_status='ESME_ROK'))
        self.assertEqual(
            None,
            (yield message_stash.get_cached_message(msg['message_id'])))

    @inlineCallbacks
    def test_sequence_number_persistence(self):
        """
        We create sequence_number to message_id mappings with an appropriate
        TTL and can delete them when we're done.
        """
        transport = yield self.get_transport()
        message_stash = transport.message_stash
        config = transport.get_static_config()

        yield message_stash.set_sequence_number_message_id(12, "abc")
        ttl = yield transport.redis.ttl(sequence_number_key(12))
        self.assertTrue(0 < ttl <= config.submit_sm_expiry)

        message_id = yield message_stash.get_sequence_number_message_id(12)
        self.assertEqual(message_id, "abc")

        yield message_stash.delete_sequence_number_message_id(12)
        message_id = yield message_stash.get_sequence_number_message_id(12)
        self.assertEqual(message_id, None)

    @inlineCallbacks
    def test_sequence_number_clearing(self):
        """
        When we finish processing a PDU response, the mapping gets deleted.
        """
        transport = yield self.get_transport()
        message_stash = transport.message_stash

        yield message_stash.set_sequence_number_message_id(37, "def")
        message_id = yield message_stash.get_sequence_number_message_id(37)
        self.assertEqual(message_id, "def")

        yield self.fake_smsc.handle_pdu(SubmitSMResp(
            sequence_number=37, message_id='foo', command_status='ESME_ROK'))
        message_id = yield message_stash.get_sequence_number_message_id(37)
        self.assertEqual(message_id, None)

    @inlineCallbacks
    def test_link_remote_message_id(self):
        transport = yield self.get_transport()
        config = transport.get_static_config()

        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)

        pdu = yield self.fake_smsc.await_pdu()
        yield self.fake_smsc.handle_pdu(
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
        yield self.get_transport()
        yield self.tx_helper.make_dispatch_outbound("msg 1", message_id='444')
        submit_sm1 = yield self.fake_smsc.await_pdu()
        response1 = SubmitSMResp(seq_no(submit_sm1), "3rd_party_id_1")

        yield self.tx_helper.make_dispatch_outbound("msg 2", message_id='445')
        submit_sm2 = yield self.fake_smsc.await_pdu()
        response2 = SubmitSMResp(seq_no(submit_sm2), "3rd_party_id_2")

        # respond out of order - just to keep things interesting
        yield self.fake_smsc.handle_pdu(response2)
        yield self.fake_smsc.handle_pdu(response1)

        [ack1, ack2] = yield self.tx_helper.wait_for_dispatched_events(2)
        self.assertEqual(ack1['user_message_id'], '445')
        self.assertEqual(ack1['sent_message_id'], '3rd_party_id_2')
        self.assertEqual(ack2['user_message_id'], '444')
        self.assertEqual(ack2['sent_message_id'], '3rd_party_id_1')

    @inlineCallbacks
    def test_delivery_report_for_unknown_message(self):
        dr = self.DR_TEMPLATE % ('foo',)
        deliver = DeliverSM(1, short_message=dr, esm_class=4)
        yield self.get_transport()
        with LogCatcher(message="Failed to retrieve message id") as lc:
            yield self.fake_smsc.handle_pdu(deliver)
            [warning] = lc.logs
            self.assertEqual(warning['message'],
                             ("Failed to retrieve message id for delivery "
                              "report. Delivery report from %s "
                              "discarded." % self.tx_helper.transport_name,))

    @inlineCallbacks
    def test_delivery_report_delivered_delete_stored_remote_id(self):
        transport = yield self.get_transport({
            'final_dr_third_party_id_expiry': 23,
        })

        yield transport.message_stash.set_remote_message_id('bar', 'foo')
        remote_id_ttl = yield transport.redis.ttl(remote_message_key('foo'))

        self.assertTrue(
            remote_id_ttl > 23,
            "remote_id_ttl (%s) <= final_dr_third_party_id_expiry (23)"
            % (remote_id_ttl,))

        pdu = DeliverSM(sequence_number=1, esm_class=4)
        pdu.add_optional_parameter('receipted_message_id', 'foo')
        pdu.add_optional_parameter('message_state', 2)
        yield self.fake_smsc.handle_pdu(pdu)

        [dr] = yield self.tx_helper.wait_for_dispatched_events(1)

        remote_id_ttl = yield transport.redis.ttl(remote_message_key('foo'))

        self.assertTrue(
            remote_id_ttl <= 23,
            "remote_id_ttl (%s) > final_dr_third_party_id_expiry (23)"
            % (remote_id_ttl,))

        self.assertEqual(dr['event_type'], u'delivery_report')
        self.assertEqual(dr['delivery_status'], u'delivered')
        self.assertEqual(dr['transport_metadata'], {
            u'smpp_delivery_status': u'DELIVERED',
        })

    @inlineCallbacks
    def test_delivery_report_failed_delete_stored_remote_id(self):
        transport = yield self.get_transport({
            'final_dr_third_party_id_expiry': 23,
        })

        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        remote_id_ttl = yield transport.redis.ttl(remote_message_key('foo'))

        self.assertTrue(
            remote_id_ttl > 23,
            "remote_id_ttl (%s) <= final_dr_third_party_id_expiry (23)"
            % (remote_id_ttl,))

        pdu = DeliverSM(sequence_number=1, esm_class=4)
        pdu.add_optional_parameter('receipted_message_id', 'foo')
        pdu.add_optional_parameter('message_state', 8)
        yield self.fake_smsc.handle_pdu(pdu)

        [dr] = yield self.tx_helper.wait_for_dispatched_events(1)

        remote_id_ttl = yield transport.redis.ttl(remote_message_key('foo'))

        self.assertTrue(
            remote_id_ttl <= 23,
            "remote_id_ttl (%s) > final_dr_third_party_id_expiry (23)"
            % (remote_id_ttl,))

        self.assertEqual(dr['event_type'], u'delivery_report')
        self.assertEqual(dr['delivery_status'], u'failed')
        self.assertEqual(dr['transport_metadata'], {
            u'smpp_delivery_status': u'REJECTED',
        })

    @inlineCallbacks
    def test_delivery_report_pending_keep_stored_remote_id(self):
        transport = yield self.get_transport({
            'final_dr_third_party_id_expiry': 23,
        })

        yield transport.message_stash.set_remote_message_id('bar', 'foo')

        remote_id_ttl = yield transport.redis.ttl(remote_message_key('foo'))

        self.assertTrue(
            remote_id_ttl > 23,
            "remote_id_ttl (%s) <= final_dr_third_party_id_expiry (23)"
            % (remote_id_ttl,))

        pdu = DeliverSM(sequence_number=1, esm_class=4)
        pdu.add_optional_parameter('receipted_message_id', 'foo')
        pdu.add_optional_parameter('message_state', 1)
        yield self.fake_smsc.handle_pdu(pdu)

        [dr] = yield self.tx_helper.wait_for_dispatched_events(1)

        remote_id_ttl = yield transport.redis.ttl(remote_message_key('foo'))

        self.assertTrue(
            remote_id_ttl > 23,
            "remote_id_ttl (%s) <= final_dr_third_party_id_expiry (23)"
            % (remote_id_ttl,))

        self.assertEqual(dr['event_type'], u'delivery_report')
        self.assertEqual(dr['delivery_status'], u'pending')
        self.assertEqual(dr['transport_metadata'], {
            u'smpp_delivery_status': u'ENROUTE',
        })

    @inlineCallbacks
    def test_disable_delivery_report_delivered_delete_stored_remote_id(self):
        transport = yield self.get_transport({
            'final_dr_third_party_id_expiry': 23,
            'disable_delivery_report': True,
        })

        yield transport.message_stash.set_remote_message_id('bar', 'foo')
        remote_id_ttl = yield transport.redis.ttl(remote_message_key('foo'))

        self.assertTrue(
            remote_id_ttl > 23,
            "remote_id_ttl (%s) <= final_dr_third_party_id_expiry (23)"
            % (remote_id_ttl,))

        pdu = DeliverSM(sequence_number=1, esm_class=4)
        pdu.add_optional_parameter('receipted_message_id', 'foo')
        pdu.add_optional_parameter('message_state', 2)
        yield self.fake_smsc.handle_pdu(pdu)
        yield self.fake_smsc.await_pdu()

        yield self.assert_no_events()

        remote_id_ttl = yield transport.redis.ttl(remote_message_key('foo'))

        self.assertTrue(
            remote_id_ttl <= 23,
            "remote_id_ttl (%s) > final_dr_third_party_id_expiry (23)"
            % (remote_id_ttl,))

    @inlineCallbacks
    def test_reconnect(self):
        transport = yield self.get_transport(bind=False)
        connector = transport.connectors[transport.transport_name]
        # Unbound and disconnected.
        self.assertEqual(connector._consumers['outbound'].paused, True)
        # Connect and bind.
        yield self.fake_smsc.bind()
        self.assertEqual(connector._consumers['outbound'].paused, False)
        # Disconnect.
        yield self.fake_smsc.disconnect()
        self.assertEqual(connector._consumers['outbound'].paused, True)
        # Wait for reconnect, but don't bind.
        self.clock.advance(transport.service.delay)
        yield self.fake_smsc.await_connected()
        self.assertEqual(connector._consumers['outbound'].paused, True)
        # Bind.
        yield self.fake_smsc.bind()
        self.assertEqual(connector._consumers['outbound'].paused, False)

    @inlineCallbacks
    def test_bind_params(self):
        yield self.get_transport({
            'system_id': 'myusername',
            'password': 'mypasswd',
            'system_type': 'SMPP',
            'interface_version': '33',
            'address_range': '*12345',
        }, bind=False)
        bind_pdu = yield self.fake_smsc.await_pdu()
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
        lc = LogCatcher(message="Password longer than 8 characters,")
        with lc:
            yield self.get_transport({
                'worker_name': 'sphex',
                'system_id': 'myusername',
                'password': 'mypass789',
                'system_type': 'SMPP',
                'interface_version': '33',
                'address_range': '*12345',
            }, bind=False)
            bind_pdu = yield self.fake_smsc.await_pdu()
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
        for l in lc.logs:
            self.assertEqual(l['system'], 'sphex')

    @inlineCallbacks
    def test_default_bind_params(self):
        yield self.get_transport(bind=False)
        bind_pdu = yield self.fake_smsc.await_pdu()
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
        yield self.get_transport(bind=False)

        # Disconnected.
        for i in range(2):
            msg = self.tx_helper.make_outbound('hello world %s' % (i,))
            yield self.tx_helper.dispatch_outbound(msg)

        # Connect and bind.
        yield self.fake_smsc.bind()
        [submit_sm1, submit_sm2] = yield self.fake_smsc.await_pdus(2)
        self.assertEqual(short_message(submit_sm1), 'hello world 0')
        self.assertEqual(short_message(submit_sm2), 'hello world 1')

    @inlineCallbacks
    def test_starting_status(self):
        """
        The SMPP bind process emits three status events.
        """
        yield self.get_transport({'publish_status': True})
        msgs = yield self.tx_helper.wait_for_dispatched_statuses()
        [msg_starting, msg_binding, msg_bound] = msgs

        self.assertEqual(msg_starting['status'], 'down')
        self.assertEqual(msg_starting['component'], 'smpp')
        self.assertEqual(msg_starting['type'], 'starting')
        self.assertEqual(msg_starting['message'], 'Starting')

        self.assertEqual(msg_binding['status'], 'down')
        self.assertEqual(msg_binding['component'], 'smpp')
        self.assertEqual(msg_binding['type'], 'binding')
        self.assertEqual(msg_binding['message'], 'Binding')

        self.assertEqual(msg_bound['status'], 'ok')
        self.assertEqual(msg_bound['component'], 'smpp')
        self.assertEqual(msg_bound['type'], 'bound')
        self.assertEqual(msg_bound['message'], 'Bound')

    @inlineCallbacks
    def test_connect_status(self):
        transport = yield self.get_transport(
            {'publish_status': True}, bind=False)

        # disconnect
        yield self.fake_smsc.disconnect()
        self.tx_helper.clear_dispatched_statuses()

        # reconnect
        self.clock.advance(transport.service.delay)
        yield self.fake_smsc.await_connected()

        [msg] = yield self.tx_helper.wait_for_dispatched_statuses()
        self.assertEqual(msg['status'], 'down')
        self.assertEqual(msg['component'], 'smpp')
        self.assertEqual(msg['type'], 'binding')
        self.assertEqual(msg['message'], 'Binding')

    @inlineCallbacks
    def test_unbinding_status(self):
        transport = yield self.get_transport({'publish_status': True})
        self.tx_helper.clear_dispatched_statuses()
        yield transport.service.get_protocol().unbind()

        [msg] = yield self.tx_helper.wait_for_dispatched_statuses()
        self.assertEqual(msg['status'], 'down')
        self.assertEqual(msg['component'], 'smpp')
        self.assertEqual(msg['type'], 'unbinding')
        self.assertEqual(msg['message'], 'Unbinding')

    @inlineCallbacks
    def test_bind_status(self):
        yield self.get_transport({'publish_status': True}, bind=False)

        self.tx_helper.clear_dispatched_statuses()

        yield self.fake_smsc.bind()

        [msg] = yield self.tx_helper.wait_for_dispatched_statuses()
        self.assertEqual(msg['status'], 'ok')
        self.assertEqual(msg['component'], 'smpp')
        self.assertEqual(msg['type'], 'bound')
        self.assertEqual(msg['message'], 'Bound')

    @inlineCallbacks
    def test_bind_timeout_status(self):
        yield self.get_transport({
            'publish_status': True,
            'smpp_bind_timeout': 3,
        }, bind=False)

        # wait for bind pdu
        yield self.fake_smsc.await_pdu()

        self.tx_helper.clear_dispatched_statuses()
        self.clock.advance(3)

        [msg] = yield self.tx_helper.wait_for_dispatched_statuses()
        self.assertEqual(msg['status'], 'down')
        self.assertEqual(msg['component'], 'smpp')
        self.assertEqual(msg['type'], 'bind_timeout')
        self.assertEqual(msg['message'], 'Timed out awaiting bind')

        yield self.fake_smsc.disconnect()

    @inlineCallbacks
    def test_connection_lost_status(self):
        yield self.get_transport({'publish_status': True})

        self.tx_helper.clear_dispatched_statuses()
        yield self.fake_smsc.disconnect()

        [msg] = yield self.tx_helper.wait_for_dispatched_statuses()
        self.assertEqual(msg['status'], 'down')
        self.assertEqual(msg['status'], 'down')
        self.assertEqual(msg['component'], 'smpp')
        self.assertEqual(msg['type'], 'connection_lost')

        self.assertEqual(
            msg['message'],
            'Connection was closed cleanly: Connection done.')

    @inlineCallbacks
    def test_smsc_throttle_status(self):
        yield self.get_transport({
            'publish_status': True,
            'throttle_delay': 3
        })

        self.tx_helper.clear_dispatched_statuses()

        msg = self.tx_helper.make_outbound("throttle me")
        yield self.tx_helper.dispatch_outbound(msg)
        submit_sm_pdu = yield self.fake_smsc.await_pdu()

        yield self.fake_smsc.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                         message_id='foo',
                         command_status='ESME_RTHROTTLED'))

        [msg] = yield self.tx_helper.wait_for_dispatched_statuses()
        self.assertEqual(msg['status'], 'degraded')
        self.assertEqual(msg['component'], 'smpp')
        self.assertEqual(msg['type'], 'throttled')
        self.assertEqual(msg['message'], 'Throttled')

        self.tx_helper.clear_dispatched_statuses()
        self.clock.advance(3)

        submit_sm_pdu_retry = yield self.fake_smsc.await_pdu()
        yield self.fake_smsc.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu_retry),
                         message_id='bar',
                         command_status='ESME_ROK'))

        self.clock.advance(0)

        [msg] = yield self.tx_helper.wait_for_dispatched_statuses()
        self.assertEqual(msg['status'], 'ok')
        self.assertEqual(msg['component'], 'smpp')
        self.assertEqual(msg['type'], 'throttled_end')
        self.assertEqual(msg['message'], 'No longer throttled')

    @inlineCallbacks
    def test_smsc_throttle_reconnect_status(self):
        transport = yield self.get_transport({
            'publish_status': True,
        })

        self.tx_helper.clear_dispatched_statuses()

        msg = self.tx_helper.make_outbound("throttle me")
        yield self.tx_helper.dispatch_outbound(msg)
        submit_sm_pdu = yield self.fake_smsc.await_pdu()

        yield self.fake_smsc.handle_pdu(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                         message_id='foo',
                         command_status='ESME_RTHROTTLED'))

        yield self.fake_smsc.disconnect()
        self.tx_helper.clear_dispatched_statuses()
        self.clock.advance(transport.service.delay)

        yield self.fake_smsc.bind()

        msgs = yield self.tx_helper.wait_for_dispatched_statuses()
        [msg1, msg2, msg3] = msgs

        self.assertEqual(msg1['type'], 'binding')
        self.assertEqual(msg2['type'], 'bound')

        self.assertEqual(msg3['status'], 'degraded')
        self.assertEqual(msg3['component'], 'smpp')
        self.assertEqual(msg3['type'], 'throttled')
        self.assertEqual(msg3['message'], 'Throttled')

    @inlineCallbacks
    def test_tps_throttle_status(self):
        yield self.get_transport({
            'publish_status': True,
            'mt_tps': 2
        })

        self.tx_helper.clear_dispatched_statuses()

        yield self.tx_helper.make_dispatch_outbound('hello world 1')
        yield self.tx_helper.make_dispatch_outbound('hello world 2')
        self.tx_helper.make_dispatch_outbound('hello world 3')
        yield self.fake_smsc.await_pdus(2)

        # We can't wait here because that requires throttling to end.
        [msg] = self.tx_helper.get_dispatched_statuses()
        self.assertEqual(msg['status'], 'degraded')
        self.assertEqual(msg['component'], 'smpp')
        self.assertEqual(msg['type'], 'throttled')
        self.assertEqual(msg['message'], 'Throttled')

        self.tx_helper.clear_dispatched_statuses()

        self.clock.advance(1)

        [msg] = yield self.tx_helper.wait_for_dispatched_statuses()
        self.assertEqual(msg['status'], 'ok')
        self.assertEqual(msg['component'], 'smpp')
        self.assertEqual(msg['type'], 'throttled_end')
        self.assertEqual(msg['message'], 'No longer throttled')

    @inlineCallbacks
    def test_tps_throttle_reconnect_status(self):
        transport = yield self.get_transport({
            'publish_status': True,
            'mt_tps': 2
        })

        self.tx_helper.clear_dispatched_statuses()

        yield self.tx_helper.make_dispatch_outbound('hello world 1')
        yield self.tx_helper.make_dispatch_outbound('hello world 2')
        self.tx_helper.make_dispatch_outbound('hello world 3')
        yield self.fake_smsc.await_pdus(2)

        yield self.fake_smsc.disconnect()
        self.tx_helper.clear_dispatched_statuses()
        self.clock.advance(transport.service.delay)

        yield self.fake_smsc.bind()

        msgs = yield self.tx_helper.wait_for_dispatched_statuses()
        [msg1, msg2, msg3] = msgs

        self.assertEqual(msg1['type'], 'binding')
        self.assertEqual(msg2['type'], 'bound')

        self.assertEqual(msg3['status'], 'degraded')
        self.assertEqual(msg3['component'], 'smpp')
        self.assertEqual(msg3['type'], 'throttled')
        self.assertEqual(msg3['message'], 'Throttled')


class SmppTransmitterTransportTestCase(SmppTransceiverTransportTestCase):
    transport_class = SmppTransmitterTransport


class SmppReceiverTransportTestCase(SmppTransceiverTransportTestCase):
    transport_class = SmppReceiverTransport


class SmppTransceiverTransportWithOldConfigTestCase(
        SmppTransceiverTransportTestCase):

    transport_class = SmppTransceiverTransportWithOldConfig

    def setUp(self):
        self.clock = Clock()
        self.fake_smsc = FakeSMSC()
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.default_config = {
            'transport_name': self.tx_helper.transport_name,
            'worker_name': self.tx_helper.transport_name,
            'twisted_endpoint': self.fake_smsc.endpoint,
            'system_id': 'foo',
            'password': 'bar',
            'data_coding_overrides': {
                0: 'utf-8',
            }
        }

    def _get_transport_config(self, config):
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
        return cfg


class TataUssdSmppTransportTestCase(SmppTransportTestCase):

    transport_class = SmppTransceiverTransport

    @inlineCallbacks
    def test_submit_and_deliver_ussd_continue(self):
        yield self.get_transport()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", transport_type="ussd")

        submit_sm_pdu = yield self.fake_smsc.await_pdu()
        self.assertEqual(command_id(submit_sm_pdu), 'submit_sm')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'ussd_service_op'), '02')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'its_session_info'), '0000')

        # Server delivers a USSD message to the Client
        pdu = DeliverSM(seq_no(submit_sm_pdu) + 1, short_message="reply!")
        pdu.add_optional_parameter('ussd_service_op', '02')
        pdu.add_optional_parameter('its_session_info', '0000')

        yield self.fake_smsc.handle_pdu(pdu)

        [mess] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(mess['content'], "reply!")
        self.assertEqual(mess['transport_type'], "ussd")
        self.assertEqual(mess['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_submit_and_deliver_ussd_close(self):
        yield self.get_transport()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", transport_type="ussd",
            session_event=TransportUserMessage.SESSION_CLOSE)

        submit_sm_pdu = yield self.fake_smsc.await_pdu()
        self.assertEqual(command_id(submit_sm_pdu), 'submit_sm')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'ussd_service_op'), '02')
        self.assertEqual(pdu_tlv(submit_sm_pdu, 'its_session_info'), '0001')

        # Server delivers a USSD message to the Client
        pdu = DeliverSM(seq_no(submit_sm_pdu) + 1, short_message="reply!")
        pdu.add_optional_parameter('ussd_service_op', '02')
        pdu.add_optional_parameter('its_session_info', '0001')

        yield self.fake_smsc.handle_pdu(pdu)

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
