from twisted.test import proto_helpers
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from twisted.internet.error import ConnectionDone
from twisted.application.service import Service

from vumi.tests.helpers import VumiTestCase
from vumi.transports.tests.helpers import TransportHelper

from vumi.transports.smpp.smpp_transport import SmppTransport
from vumi.transports.smpp.clientserver.new_client import pdu_ok
from vumi.transports.smpp.clientserver.tests.test_new_client import (
    bind_protocol, wait_for_pdus)

from smpp.pdu_builder import DeliverSM


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
        self.protocol = smpp_transport.service.protocol

    def sendPDU(self, pdu):
        self.protocol.dataReceived(pdu.get_bin())

    def send_mo(self, sequence_number, short_message, data_coding=0, **kwargs):
        """
        data_coding=0 is set to utf-8 in the ``data_coding_overrides``
        """
        return self.sendPDU(
            DeliverSM(sequence_number, short_message=short_message,
                      data_coding=data_coding, **kwargs))

    def wait_for_pdus(self, count):
        return wait_for_pdus(self.string_transport, count)


class TestSmppTransport(VumiTestCase):

    def setUp(self):
        self.patch(SmppTransport, 'start_service', self.patched_start_service)

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
    def get_transport(self, config={}, bind=True):
        cfg = self.default_config.copy()
        cfg.update(config)
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

    def wait_for_pdus(self, count):
        return wait_for_pdus(self.string_transport, count)

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

