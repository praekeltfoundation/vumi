from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import Clock

from vumi.tests.helpers import VumiTestCase
from vumi.transports.smpp.smpp_transport import SmppTransceiverTransport
from vumi.transports.smpp.tests.fake_smsc import FakeSMSC
from vumi.transports.tests.helpers import TransportHelper


class TestFakeSMSC(VumiTestCase):
    """
    Tests for FakeSMSC.

    TODO: Use a StringTransport or something instead of an SmppTransport.
    """

    def setUp(self):
        self.clock = Clock()
        self.tx_helper = self.add_helper(
            TransportHelper(SmppTransceiverTransport))

        self.default_config = {
            'transport_name': self.tx_helper.transport_name,
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
    def connect_transport(self, fake_smsc, config={}):
        cfg = self.default_config.copy()
        cfg['twisted_endpoint'] = fake_smsc.endpoint
        cfg.update(config)
        transport = yield self.tx_helper.get_transport(cfg, start=False)
        transport.clock = self.clock
        yield transport.startWorker()
        self.clock.advance(0)
        returnValue(transport)

    @inlineCallbacks
    def test_connect(self):
        """
        A transport can connect to a FakeSMSC and send a bind request.
        """
        fake_smsc = FakeSMSC()
        self.assertEqual(fake_smsc._client_protocol, None)
        yield self.connect_transport(fake_smsc)
        yield fake_smsc.await_connected()
        self.assertEqual(fake_smsc._client_protocol.connected, True)
        pdu = yield fake_smsc.pdu_queue.get()
        self.assertEqual(pdu['header']['command_id'], 'bind_transceiver')

    @inlineCallbacks
    def test_connect_no_auto(self):
        """
        A FakeSMSC can make the transport wait before accepting the connection.
        """
        fake_smsc = FakeSMSC(auto_accept=False)
        self.assertEqual(fake_smsc._client_protocol, None)
        yield self.connect_transport(fake_smsc)
        # The connection has been started, but is not yet completed.
        yield fake_smsc.await_connecting()
        self.assertEqual(fake_smsc._client_protocol.connected, False)
        self.clock.advance(0.1)
        self.assertEqual(fake_smsc._client_protocol.connected, False)
        # After accepting the connection, everything proceeds as normal.
        fake_smsc.accept_connection()
        self.assertEqual(fake_smsc._client_protocol.connected, True)
        yield fake_smsc.await_connected()
        self.assertEqual(fake_smsc._client_protocol.connected, True)
        pdu = yield fake_smsc.await_pdu()
        self.assertEqual(pdu['header']['command_id'], 'bind_transceiver')

    @inlineCallbacks
    def test_bind(self):
        """
        A FakeSMSC can reply to a bind PDU.
        """
        fake_smsc = FakeSMSC()
        self.assertEqual(fake_smsc._client_protocol, None)
        yield self.connect_transport(fake_smsc)
        yield fake_smsc.await_connected()
        self.assertEqual(fake_smsc._client_protocol.connected, True)
        self.assertEqual(fake_smsc._client_protocol.is_bound(), False)
        yield fake_smsc.bind()
        self.assertEqual(fake_smsc._client_protocol.is_bound(), True)

    @inlineCallbacks
    def test_bind_explicit(self):
        """
        A FakeSMSC can reply to a specific bind PDU.
        """
        fake_smsc = FakeSMSC()
        self.assertEqual(fake_smsc._client_protocol, None)
        yield self.connect_transport(fake_smsc)
        yield fake_smsc.await_connected()
        self.assertEqual(fake_smsc._client_protocol.connected, True)
        self.assertEqual(fake_smsc._client_protocol.is_bound(), False)
        bind_pdu = yield fake_smsc.await_pdu()
        self.assertEqual(bind_pdu['header']['command_id'], 'bind_transceiver')
        yield fake_smsc.bind(bind_pdu)
        self.assertEqual(fake_smsc._client_protocol.is_bound(), True)
