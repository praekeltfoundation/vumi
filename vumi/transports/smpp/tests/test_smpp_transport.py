# -*- coding: utf-8 -*-

from twisted.test import proto_helpers
from twisted.internet import reactor
from twisted.internet.defer import (inlineCallbacks, returnValue, Deferred,
                                    maybeDeferred, succeed)
from twisted.internet.error import ConnectionDone
from twisted.internet.task import Clock
from twisted.application.service import Service

from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import LogCatcher
from vumi.transports.tests.helpers import TransportHelper

from vumi.transports.smpp.smpp_transport import (
    SmppTransport, SmppTransceiverProtocol, message_key, remote_message_key)
from vumi.transports.smpp.pdu_utils import (
    pdu_ok, short_message, command_id, seq_no, pdu_tlv)
from vumi.transports.smpp.clientserver.tests.test_new_client import (
    bind_protocol, wait_for_pdus)

from smpp.pdu_builder import DeliverSM, SubmitSMResp
from smpp.pdu import unpack_pdu

from vumi import log


class DummyService(Service):

    def __init__(self, factory):
        self.factory = factory
        self.protocol = None

    def startService(self):
        self.protocol = self.factory.buildProtocol(('120.0.0.1', 0))

    def stopService(self):
        if self.protocol and self.protocol.transport:
            self.protocol.transport.loseConnection()
            self.protocol.connectionLost(reason=ConnectionDone)


class SMPPHelper(object):
    def __init__(self, string_transport, smpp_transport):
        self.string_transport = string_transport
        self.transport = smpp_transport
        self.protocol = smpp_transport.service.protocol

    def sendPDU(self, pdu):
        """put it on the wire and don't wait for a response"""
        self.protocol.dataReceived(pdu.get_bin())

    def handlePDU(self, pdu):
        """short circuit the wire so we get a deferred so we know
        when it's been handled, also allows us to test PDUs that are invalid
        because we're skipping the encode/decode step."""
        return self.protocol.onPdu(pdu.obj)

    def send_mo(self, sequence_number, short_message, data_coding=1, **kwargs):
        return self.sendPDU(
            DeliverSM(sequence_number, short_message=short_message,
                      data_coding=data_coding, **kwargs))

    def wait_for_pdus(self, count):
        return wait_for_pdus(self.string_transport, count)

    def no_pdus(self):
        return self.string_transport.value() == ''


class TestSmppTransport(VumiTestCase):

    timeout = 1

    def setUp(self):

        self.clock = Clock()
        self.patch(SmppTransport, 'start_service', self.patched_start_service)
        self.patch(SmppTransport, 'clock', self.clock)

        self.string_transport = proto_helpers.StringTransport()

        self.tx_helper = self.add_helper(TransportHelper(SmppTransport))
        self.default_config = {
            'transport_name': self.tx_helper.transport_name,
            'twisted_endpoint': 'tcp:host=127.0.0.1:port=0',
            'delivery_report_processor': 'vumi.transports.smpp.processors.'
                                         'DeliveryReportProcessor',
            'short_message_processor': 'vumi.transports.smpp.processors.'
                                       'DeliverShortMessageProcessor',
            'smpp_config': {
                'system_id': 'foo',
                'password': 'bar',
            },
            'short_message_processor_config': {
                'data_coding_overrides': {
                    0: 'utf-8',
                }
            }
        }

    def patched_start_service(self, factory):
        service = DummyService(factory)
        service.startService()
        return service

    @inlineCallbacks
    def get_transport(self, config={}, smpp_config=None, bind=True):
        cfg = self.default_config.copy()
        cfg.update(config)

        if smpp_config is not None:
            cfg['smpp_config'].update(smpp_config)

        transport = yield self.tx_helper.get_transport(cfg)
        if bind:
            yield self.create_smpp_bind(transport)
        returnValue(transport)

    def create_smpp_bind(self, smpp_transport):
        d = Deferred()

        def cb(smpp_transport):
            protocol = smpp_transport.service.protocol
            if protocol is None:
                # Still setting up factory
                reactor.callLater(0, cb, smpp_transport)
                return

            if not protocol.transport:
                # Factory setup, needs bind pdus
                protocol.makeConnection(self.string_transport)
                bind_d = bind_protocol(self.string_transport, protocol)
                bind_d.addCallback(
                    lambda _: reactor.callLater(0, cb, smpp_transport))
                return bind_d

            d.callback(smpp_transport)

        cb(smpp_transport)
        return d

    def sendPDU(self, transport, pdu):
        protocol = transport.service.protocol
        protocol.dataReceived(pdu.get_bin())

    @inlineCallbacks
    def get_smpp_helper(self, *args, **kwargs):
        transport = yield self.get_transport(*args, **kwargs)
        returnValue(SMPPHelper(self.string_transport, transport))

    @inlineCallbacks
    def test_setup_transport(self):
        transport = yield self.get_transport()
        self.assertTrue(transport.service.protocol.isBound())

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
        smpp_helper.sendPDU(pdu)

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
    def test_mo_sms_multipart_sar(self):
        smpp_helper = yield self.get_smpp_helper()
        deliver_sm_resps = []

        pdu1 = DeliverSM(sequence_number=1, short_message='back')
        pdu1.add_optional_parameter('sar_msg_ref_num', 1)
        pdu1.add_optional_parameter('sar_total_segments', 3)
        pdu1.add_optional_parameter('sar_segment_seqnum', 1)

        smpp_helper.sendPDU(pdu1)
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])

        pdu2 = DeliverSM(sequence_number=2, short_message=' at')
        pdu2.add_optional_parameter('sar_msg_ref_num', 1)
        pdu2.add_optional_parameter('sar_total_segments', 3)
        pdu2.add_optional_parameter('sar_segment_seqnum', 2)

        smpp_helper.sendPDU(pdu2)
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])

        pdu3 = DeliverSM(sequence_number=3, short_message=' you')
        pdu3.add_optional_parameter('sar_msg_ref_num', 1)
        pdu3.add_optional_parameter('sar_total_segments', 3)
        pdu3.add_optional_parameter('sar_segment_seqnum', 3)

        smpp_helper.sendPDU(pdu3)
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])

        self.assertEqual([1, 2, 3], map(seq_no, deliver_sm_resps))
        self.assertTrue(all(map(pdu_ok, deliver_sm_resps)))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], u'back at you')

    @inlineCallbacks
    def test_mt_sms(self):
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)
        [pdu] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(command_id(pdu), 'submit_sm')
        self.assertEqual(short_message(pdu), 'hello world')

    @inlineCallbacks
    def test_mt_sms_ack(self):
        smpp_helper = yield self.get_smpp_helper()
        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        smpp_helper.sendPDU(
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
        smpp_helper.sendPDU(
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
        smpp_helper.sendPDU(response)

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

        yield smpp_helper.handlePDU(
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
    def test_mt_sms_throttled(self):
        smpp_helper = yield self.get_smpp_helper()
        transport_config = smpp_helper.transport.get_static_config()
        msg = self.tx_helper.make_outbound('hello world')

        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handlePDU(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                         message_id='foo',
                         command_status='ESME_RTHROTTLED'))

        self.clock.advance(transport_config.throttle_delay)

        [submit_sm_pdu_retry] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handlePDU(
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
    def test_mt_sms_queue_full(self):
        smpp_helper = yield self.get_smpp_helper()
        transport_config = smpp_helper.transport.get_static_config()
        msg = self.tx_helper.make_outbound('hello world')

        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm_pdu] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handlePDU(
            SubmitSMResp(sequence_number=seq_no(submit_sm_pdu),
                         message_id='foo',
                         command_status='ESME_RMSGQFUL'))

        self.clock.advance(transport_config.throttle_delay)

        [submit_sm_pdu_retry] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handlePDU(
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
        smpp_helper = yield self.get_smpp_helper(smpp_config={
            'send_long_messages': True,
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
        smpp_helper = yield self.get_smpp_helper(smpp_config={
            'send_multipart_udh': True,
        })
        # SMPP specifies that messages longer than 254 bytes should
        # be put in the message_payload field using TLVs
        content = '1' * 161
        msg = self.tx_helper.make_outbound(content)
        yield self.tx_helper.dispatch_outbound(msg)
        [submit_sm1, submit_sm2] = yield smpp_helper.wait_for_pdus(2)

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
    def test_mt_sms_multipart_sar(self):
        smpp_helper = yield self.get_smpp_helper(smpp_config={
            'send_multipart_sar': True,
        })
        # SMPP specifies that messages longer than 254 bytes should
        # be put in the message_payload field using TLVs
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
    def test_message_persistence(self):
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        config = transport.get_static_config()

        msg = self.tx_helper.make_outbound("hello world")
        yield transport.cache_message(msg)

        ttl = yield transport.redis.ttl(message_key(msg['message_id']))
        self.assertTrue(0 < ttl <= config.submit_sm_expiry)

        retrieved_msg = yield smpp_helper.transport.get_cached_message(
            msg['message_id'])
        self.assertEqual(msg, retrieved_msg)
        yield smpp_helper.transport.delete_cached_message(msg['message_id'])
        self.assertEqual(
            (yield smpp_helper.transport.get_cached_message(msg['message_id'])),
            None)

    @inlineCallbacks
    def test_message_clearing(self):
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        msg = self.tx_helper.make_outbound('hello world')
        yield transport.set_sequence_number_message_id(3, msg['message_id'])
        yield transport.cache_message(msg)
        yield smpp_helper.handlePDU(SubmitSMResp(sequence_number=3,
                                                 message_id='foo',
                                                 command_status='ESME_ROK'))
        self.assertEqual(
            None,
            (yield transport.get_cached_message(msg['message_id'])))

    @inlineCallbacks
    def test_link_remote_message_id(self):
        smpp_helper = yield self.get_smpp_helper()
        transport = smpp_helper.transport
        config = transport.get_static_config()

        msg = self.tx_helper.make_outbound('hello world')
        yield self.tx_helper.dispatch_outbound(msg)

        [pdu] = yield smpp_helper.wait_for_pdus(1)
        yield smpp_helper.handlePDU(
            SubmitSMResp(sequence_number=seq_no(pdu),
                         message_id='foo',
                         command_status='ESME_ROK'))
        self.assertEqual(
            msg['message_id'],
            (yield transport.get_internal_message_id('foo')))

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
        yield smpp_helper.handlePDU(response2)
        yield smpp_helper.handlePDU(response1)

        [ack1, ack2] = yield self.tx_helper.wait_for_dispatched_events(2)
        self.assertEqual(ack1['user_message_id'], '445')
        self.assertEqual(ack1['sent_message_id'], '3rd_party_id_2')
        self.assertEqual(ack2['user_message_id'], '444')
        self.assertEqual(ack2['sent_message_id'], '3rd_party_id_1')

    @inlineCallbacks
    def test_delivery_report_for_unknown_message(self):
        dr = ("id:123 sub:... dlvrd:... submit date:200101010030"
              " done date:200101020030 stat:DELIVRD err:... text:Meep")
        deliver = DeliverSM(1, short_message=dr)
        smpp_helper = yield self.get_smpp_helper()
        with LogCatcher(message="Failed to retrieve message id") as lc:
            yield smpp_helper.handlePDU(deliver)
            [warning] = lc.logs
            self.assertEqual(warning['message'],
                             ("Failed to retrieve message id for delivery "
                              "report. Delivery report from %s "
                              "discarded." % self.tx_helper.transport_name,))

    @inlineCallbacks
    def test_reconnect(self):
        self.patch(SmppTransceiverProtocol, 'bind', lambda *a: succeed(True))
        smpp_helper = yield self.get_smpp_helper(bind=False)
        transport = smpp_helper.transport
        protocol = smpp_helper.protocol
        connector = transport.connectors[transport.transport_name]
        self.assertFalse(connector._consumers['outbound'].paused)
        yield protocol.onConnectionLost(ConnectionDone)
        self.assertTrue(connector._consumers['outbound'].paused)
        yield protocol.onConnectionLost(ConnectionDone)
        self.assertTrue(connector._consumers['outbound'].paused)

        yield protocol.onConnectionMade()
        self.assertFalse(connector._consumers['outbound'].paused)
        yield protocol.onConnectionMade()
        self.assertFalse(connector._consumers['outbound'].paused)
