from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet.task import Clock, deferLater

from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (
    BindTransceiver, BindTransceiverResp, BindTransmitter, BindTransmitterResp,
    BindReceiver, BindReceiverResp, EnquireLink, EnquireLinkResp, DeliverSM,
    Unbind, UnbindResp, SubmitSM, SubmitSMResp)
from vumi.tests.helpers import VumiTestCase
from vumi.transports.smpp.tests.fake_smsc import FakeSMSC


def wait0():
    from twisted.internet import reactor
    return deferLater(reactor, 0, lambda: None)


class FakeESME(Protocol):
    def __init__(self):
        self.received = b""
        self.pdus_handled = []
        self.handle_pdu_d = None

    def dataReceived(self, data):
        self.received += data

    def connectionLost(self, reason):
        self.connected = False

    def on_pdu(self, pdu):
        self.handle_pdu_d = Deferred()
        self.handle_pdu_d.addCallback(self._handle_pdu, pdu)
        return self.handle_pdu_d

    def _handle_pdu(self, r, pdu):
        self.pdus_handled.append(pdu)
        self.handle_pdu_d = None

    def write(self, data):
        self.transport.write(data)
        return wait0()


class FakeESMEFactory(ClientFactory):
    protocol = FakeESME

    def __init__(self):
        self.proto = None

    def buildProtocol(self, addr):
        self.proto = ClientFactory.buildProtocol(self, addr)
        return self.proto


class TestFakeSMSC(VumiTestCase):
    """
    Tests for FakeSMSC.
    """

    def setUp(self):
        self.clock = Clock()
        self.client_factory = FakeESMEFactory()

    def connect(self, fake_smsc):
        return fake_smsc.endpoint.connect(self.client_factory)

    def test_await_connecting(self):
        """
        The caller can wait for a client connection attempt.
        """
        fake_smsc = FakeSMSC(auto_accept=False)
        await_connecting_d = fake_smsc.await_connecting()
        self.assertNoResult(await_connecting_d)
        self.assertEqual(self.client_factory.proto, None)
        self.assertEqual(fake_smsc._client_protocol, None)

        connect_d = self.connect(fake_smsc)
        # The client connection has started ...
        self.successResultOf(await_connecting_d)
        client = self.client_factory.proto
        self.assertNotEqual(client, None)
        self.assertEqual(fake_smsc._client_protocol, client)
        # ... but has not yet been accepted.
        self.assertNoResult(connect_d)
        self.assertEqual(client.connected, False)

    def test_await_connected(self):
        """
        The caller can wait for a client to connect.
        """
        fake_smsc = FakeSMSC(auto_accept=True)
        await_connected_d = fake_smsc.await_connected()
        self.assertNoResult(await_connected_d)
        self.assertEqual(self.client_factory.proto, None)
        self.assertEqual(fake_smsc._client_protocol, None)

        self.connect(fake_smsc)
        # The client has connected.
        self.successResultOf(await_connected_d)
        client = self.client_factory.proto
        self.assertNotEqual(client, None)
        self.assertEqual(fake_smsc._client_protocol, client)
        self.assertEqual(client.connected, True)

    def test_accept_connection(self):
        """
        With auto-accept disabled, a connection must be manually accepted.
        """
        fake_smsc = FakeSMSC(auto_accept=False)
        await_connecting_d = fake_smsc.await_connecting()
        await_connected_d = fake_smsc.await_connected()
        self.assertNoResult(await_connecting_d)
        self.assertNoResult(await_connected_d)

        connect_d = self.connect(fake_smsc)
        # The client connection is pending.
        self.successResultOf(await_connecting_d)
        self.assertNoResult(await_connected_d)
        self.assertNoResult(connect_d)
        client = self.client_factory.proto
        self.assertEqual(client.connected, False)

        accept_d = fake_smsc.accept_connection()
        # The client is connected.
        self.successResultOf(await_connected_d)
        self.successResultOf(accept_d)
        self.assertEqual(client.connected, True)
        self.assertEqual(self.successResultOf(connect_d), client)

    def test_accept_connection_no_pending(self):
        """
        There must be a pending connection to accept.
        """
        fake_smsc = FakeSMSC(auto_accept=False)
        self.assertRaises(Exception, fake_smsc.accept_connection)

    def test_reject_connection(self):
        """
        With auto-accept disabled, a connection may be rejected.
        """
        fake_smsc = FakeSMSC(auto_accept=False)
        await_connecting_d = fake_smsc.await_connecting()
        await_connected_d = fake_smsc.await_connected()
        self.assertNoResult(await_connecting_d)
        self.assertNoResult(await_connected_d)

        connect_d = self.connect(fake_smsc)
        # The client connection is pending.
        self.successResultOf(await_connecting_d)
        self.assertNoResult(await_connected_d)
        self.assertNoResult(connect_d)
        client = self.client_factory.proto
        self.assertEqual(client.connected, False)

        fake_smsc.reject_connection()
        # The client is not connected.
        self.failureResultOf(connect_d, ConnectionRefusedError)
        self.assertNoResult(await_connected_d)
        self.assertEqual(client.connected, False)

    def test_reject_connection_no_pending(self):
        """
        There must be a pending connection to reject.
        """
        fake_smsc = FakeSMSC(auto_accept=False)
        self.assertRaises(Exception, fake_smsc.reject_connection)

    def test_has_pending_connection(self):
        """
        FakeSMSC knows if there's a pending connection.
        """
        fake_smsc = FakeSMSC(auto_accept=False)
        self.assertEqual(fake_smsc.has_pending_connection(), False)

        # Pending connection we reject.
        connect_d = self.connect(fake_smsc)
        self.assertEqual(fake_smsc.has_pending_connection(), True)
        fake_smsc.reject_connection()
        self.assertEqual(fake_smsc.has_pending_connection(), False)
        self.failureResultOf(connect_d)

        # Pending connection we accept.
        connected_d = self.connect(fake_smsc)
        self.assertEqual(fake_smsc.has_pending_connection(), True)
        fake_smsc.accept_connection()
        self.assertEqual(fake_smsc.has_pending_connection(), False)
        self.successResultOf(connected_d)

    @inlineCallbacks
    def test_send_bytes(self):
        """
        Bytes can be sent to the client.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")

        send_d = fake_smsc.send_bytes(b"abc")
        # Bytes sent, not yet received.
        self.assertNoResult(send_d)
        self.assertEqual(client.received, b"")

        yield send_d
        # Bytes received.
        self.assertEqual(client.received, b"abc")

    @inlineCallbacks
    def test_send_pdu(self):
        """
        A PDU can be sent to the client over the wire.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")
        self.assertEqual(client.pdus_handled, [])

        pdu = DeliverSM(0)
        send_d = fake_smsc.send_pdu(pdu)
        # PDU sent, not yet received.
        self.assertNoResult(send_d)
        self.assertNotEqual(client.received, pdu.get_bin())

        yield send_d
        # PDU received.
        self.assertEqual(client.received, pdu.get_bin())
        self.assertEqual(client.pdus_handled, [])

    def test_handle_pdu(self):
        """
        A PDU can be sent to the client for direct processing.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")
        self.assertEqual(client.pdus_handled, [])

        pdu = DeliverSM(0)
        handle_d = fake_smsc.handle_pdu(pdu)
        # PDU sent, not yet processed.
        self.assertNoResult(handle_d)
        self.assertEqual(client.pdus_handled, [])

        client.handle_pdu_d.callback(None)
        # PDU processed.
        self.successResultOf(handle_d)
        self.assertEqual(client.received, b"")
        self.assertEqual(client.pdus_handled, [pdu.obj])

    @inlineCallbacks
    def test_bind(self):
        """
        FakeSMSC can accept a bind request and respond to the first
        enquire_link.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")

        bind_d = fake_smsc.bind()
        yield client.write(BindTransceiver(0).get_bin())
        # Bind response received.
        self.assertNoResult(bind_d)
        self.assertEqual(client.received, BindTransceiverResp(0).get_bin())
        client.received = b""

        yield client.write(EnquireLink(1).get_bin())
        # enquire_link response received.
        self.assertNoResult(bind_d)
        self.assertEqual(client.received, EnquireLinkResp(1).get_bin())

        yield wait0()
        # Bind complete.
        self.successResultOf(bind_d)

    @inlineCallbacks
    def test_bind_mode_TRX(self):
        """
        FakeSMSC can accept tranceiver bind requests.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")

        bind_d = fake_smsc.bind()
        yield client.write(BindTransceiver(0).get_bin())
        yield client.write(EnquireLink(1).get_bin())
        self.assertEqual(client.received, b"".join([
            BindTransceiverResp(0).get_bin(),
            EnquireLinkResp(1).get_bin()]))
        yield wait0()
        self.successResultOf(bind_d)

    @inlineCallbacks
    def test_bind_mode_TX(self):
        """
        FakeSMSC can accept transmitter bind requests.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")

        bind_d = fake_smsc.bind()
        yield client.write(BindTransmitter(0).get_bin())
        yield client.write(EnquireLink(1).get_bin())
        self.assertEqual(client.received, b"".join([
            BindTransmitterResp(0).get_bin(),
            EnquireLinkResp(1).get_bin()]))
        yield wait0()
        self.successResultOf(bind_d)

    @inlineCallbacks
    def test_bind_mode_RX(self):
        """
        FakeSMSC can accept receiver bind requests.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")

        bind_d = fake_smsc.bind()
        yield client.write(BindReceiver(0).get_bin())
        yield client.write(EnquireLink(1).get_bin())
        self.assertEqual(client.received, b"".join([
            BindReceiverResp(0).get_bin(),
            EnquireLinkResp(1).get_bin()]))
        yield wait0()
        self.successResultOf(bind_d)

    @inlineCallbacks
    def test_bind_explicit(self):
        """
        FakeSMSC can bind using a PDU explicitly passed in.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")

        bind_d = fake_smsc.bind(BindTransceiver(0).obj)
        yield wait0()
        # Bind response received.
        self.assertNoResult(bind_d)
        self.assertEqual(client.received, BindTransceiverResp(0).get_bin())
        client.received = b""

        yield client.write(EnquireLink(1).get_bin())
        # enquire_link response received.
        self.assertNoResult(bind_d)
        self.assertEqual(client.received, EnquireLinkResp(1).get_bin())

        yield wait0()
        # Bind complete.
        self.successResultOf(bind_d)

    @inlineCallbacks
    def test_bind_wrong_pdu(self):
        """
        FakeSMSC will raise an exception if asked to bind with a non-bind PDU.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))

        bind_d = fake_smsc.bind()
        yield client.write(EnquireLink(0).get_bin())
        self.failureResultOf(bind_d, ValueError)

    @inlineCallbacks
    def test_respond_to_enquire_link(self):
        """
        FakeSMSC can respond to an enquire_link.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")

        rtel_d = fake_smsc.respond_to_enquire_link()
        yield client.write(EnquireLink(2).get_bin())
        # enquire_link response received.
        self.assertNoResult(rtel_d)
        self.assertEqual(client.received, EnquireLinkResp(2).get_bin())

        yield wait0()
        self.successResultOf(rtel_d)

    @inlineCallbacks
    def test_respond_to_enquire_link_explicit(self):
        """
        FakeSMSC can respond to an enquire_link PDU explicitly passed in.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")

        rtel_d = fake_smsc.respond_to_enquire_link(EnquireLink(2).obj)
        yield wait0()
        # enquire_link response received.
        self.successResultOf(rtel_d)
        self.assertEqual(client.received, EnquireLinkResp(2).get_bin())

    @inlineCallbacks
    def test_respond_to_enquire_link_wrong_pdu(self):
        """
        FakeSMSC will raise an exception if asked to respond to an enquire_link
        that isn't an enquire_link.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))

        rtel_d = fake_smsc.respond_to_enquire_link()
        yield client.write(DeliverSM(0).get_bin())
        self.failureResultOf(rtel_d, ValueError)

    @inlineCallbacks
    def test_await_pdu(self):
        """
        The caller can wait for a PDU to arrive.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))

        pdu_d = fake_smsc.await_pdu()
        # No PDU yet.
        self.assertNoResult(pdu_d)

        client.write(EnquireLink(1).get_bin())  # No yield.
        # PDU sent, not yet received.
        self.assertNoResult(pdu_d)

        yield wait0()
        # PDU received.
        self.assertEqual(
            self.successResultOf(pdu_d), unpack_pdu(EnquireLink(1).get_bin()))

    @inlineCallbacks
    def test_await_pdu_arrived(self):
        """
        The caller can wait for a PDU that has already arrived.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        yield client.write(EnquireLink(1).get_bin())
        yield client.write(EnquireLink(2).get_bin())

        self.assertEqual(
            self.successResultOf(fake_smsc.await_pdu()),
            unpack_pdu(EnquireLink(1).get_bin()))
        self.assertEqual(
            self.successResultOf(fake_smsc.await_pdu()),
            unpack_pdu(EnquireLink(2).get_bin()))

    @inlineCallbacks
    def test_await_pdus(self):
        """
        The caller can wait for multiple PDUs to arrive.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))

        pdus_d = fake_smsc.await_pdus(2)
        # No PDUs yet.
        self.assertNoResult(pdus_d)

        yield client.write(EnquireLink(1).get_bin())
        # One PDU received, no result.
        self.assertNoResult(pdus_d)

        yield client.write(EnquireLink(2).get_bin())
        # Both PDUs received.
        self.assertEqual(self.successResultOf(pdus_d), [
            unpack_pdu(EnquireLink(1).get_bin()),
            unpack_pdu(EnquireLink(2).get_bin())])

    @inlineCallbacks
    def test_await_pdus_arrived(self):
        """
        The caller can wait for multiple PDU that have already arrived.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        yield client.write(EnquireLink(1).get_bin())
        yield client.write(EnquireLink(2).get_bin())

        self.assertEqual(self.successResultOf(fake_smsc.await_pdus(2)), [
            unpack_pdu(EnquireLink(1).get_bin()),
            unpack_pdu(EnquireLink(2).get_bin())])

    @inlineCallbacks
    def test_waiting_pdu_count(self):
        """
        FakeSMSC knows how many received PDUs are waiting.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))

        # Nothing received yet.
        self.assertEqual(fake_smsc.waiting_pdu_count(), 0)

        # Some PDUs received.
        yield client.write(EnquireLink(1).get_bin())
        self.assertEqual(fake_smsc.waiting_pdu_count(), 1)
        yield client.write(EnquireLink(2).get_bin())
        self.assertEqual(fake_smsc.waiting_pdu_count(), 2)

        # Some PDUs returned.
        self.successResultOf(fake_smsc.await_pdu())
        self.assertEqual(fake_smsc.waiting_pdu_count(), 1)
        self.successResultOf(fake_smsc.await_pdu())
        self.assertEqual(fake_smsc.waiting_pdu_count(), 0)

        # Wait for a PDU that arrives later.
        pdu_d = fake_smsc.await_pdu()
        self.assertNoResult(pdu_d)
        self.assertEqual(fake_smsc.waiting_pdu_count(), 0)
        yield client.write(EnquireLink(3).get_bin())
        self.assertEqual(fake_smsc.waiting_pdu_count(), 0)
        self.successResultOf(pdu_d)

    @inlineCallbacks
    def test_send_mo(self):
        """
        FakeSMSC can send a DeliverSM PDU.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")

        yield fake_smsc.send_mo(5, "hello")
        # First MO received.
        self.assertEqual(client.received, DeliverSM(
            5, short_message="hello", data_coding=1).get_bin())
        client.received = b""

        yield fake_smsc.send_mo(6, "hello again", 8, destination_addr="123")
        # Second MO received.
        self.assertEqual(client.received, DeliverSM(
            6, short_message="hello again", data_coding=8,
            destination_addr="123").get_bin())

    @inlineCallbacks
    def test_disconnect(self):
        """
        FakeSMSC can disconnect from the client.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(fake_smsc.connected, True)
        self.assertEqual(client.connected, True)

        disconnect_d = fake_smsc.disconnect()
        # Disconnect triggered, but not completed.
        self.assertNoResult(disconnect_d)
        self.assertEqual(client.connected, True)
        self.assertEqual(fake_smsc.connected, True)

        yield wait0()
        # Disconnect completed.
        self.successResultOf(disconnect_d)
        self.assertEqual(client.connected, False)
        self.assertEqual(fake_smsc.connected, False)
        self.assertEqual(fake_smsc.protocol, None)
        self.assertEqual(fake_smsc._client_protocol, None)
        self.assertNoResult(fake_smsc._listen_d)
        self.assertNoResult(fake_smsc._connected_d)

    @inlineCallbacks
    def test_await_disconnect(self):
        """
        FakeSMSC can wait for the connection to close.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))

        disconnect_d = fake_smsc.await_disconnect()
        yield wait0()
        self.assertNoResult(disconnect_d)

        client.transport.loseConnection()
        # Disconnect triggered, but not completed.
        self.assertNoResult(disconnect_d)

        yield wait0()
        # Disconnect completed.
        self.successResultOf(disconnect_d)

    @inlineCallbacks
    def test_auto_unbind(self):
        """
        FakeSMSC will automatically respond to an unbind request by default.
        The unbind PDU remains in the queue.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")
        self.assertEqual(fake_smsc.waiting_pdu_count(), 0)

        yield client.write(Unbind(7).get_bin())
        self.assertEqual(client.received, UnbindResp(7).get_bin())
        self.assertEqual(fake_smsc.waiting_pdu_count(), 1)

    @inlineCallbacks
    def test_submit_sm_resp(self):
        """
        FakeSMSC can respond to a SubmitSM PDU and wait for it to finish being
        processed.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")

        # No params.
        submit_sm_resp_d = fake_smsc.submit_sm_resp()
        yield client.write(SubmitSM(123).get_bin())
        self.assertNoResult(submit_sm_resp_d)

        client.handle_pdu_d.callback(None)
        self.successResultOf(submit_sm_resp_d)
        resp = SubmitSMResp(123, message_id="id123", command_status="ESME_ROK")
        self.assertEqual(client.pdus_handled, [resp.obj])

        client.pdus_handled[:] = []
        # Explicit message_id.
        submit_sm_resp_d = fake_smsc.submit_sm_resp(message_id="foo")
        yield client.write(SubmitSM(124).get_bin())
        self.assertNoResult(submit_sm_resp_d)

        client.handle_pdu_d.callback(None)
        self.successResultOf(submit_sm_resp_d)
        resp = SubmitSMResp(124, message_id="foo", command_status="ESME_ROK")
        self.assertEqual(client.pdus_handled, [resp.obj])

    @inlineCallbacks
    def test_submit_sm_resp_with_failure(self):
        """
        FakeSMSC can respond to a SubmitSM PDU with a failure.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")

        # No params.
        submit_sm_resp_d = fake_smsc.submit_sm_resp(
            command_status="ESME_RSUBMITFAIL")
        yield client.write(SubmitSM(123).get_bin())
        self.assertNoResult(submit_sm_resp_d)

        client.handle_pdu_d.callback(None)
        self.successResultOf(submit_sm_resp_d)
        resp = SubmitSMResp(
            123, message_id="id123", command_status="ESME_RSUBMITFAIL")
        self.assertEqual(client.pdus_handled, [resp.obj])

        client.pdus_handled[:] = []
        # Explicit message_id.
        submit_sm_resp_d = fake_smsc.submit_sm_resp(
            command_status="ESME_RSUBMITFAIL", message_id="foo")
        yield client.write(SubmitSM(124).get_bin())
        self.assertNoResult(submit_sm_resp_d)

        client.handle_pdu_d.callback(None)
        self.successResultOf(submit_sm_resp_d)
        resp = SubmitSMResp(
            124, message_id="foo", command_status="ESME_RSUBMITFAIL")
        self.assertEqual(client.pdus_handled, [resp.obj])

    @inlineCallbacks
    def test_submit_sm_resp_explicit(self):
        """
        FakeSMSC can respond to a SubmitSM PDU that is explicitly passed in.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))
        self.assertEqual(client.received, b"")

        # No params.
        submit_sm_resp_d = fake_smsc.submit_sm_resp(SubmitSM(123).obj)
        self.assertNoResult(submit_sm_resp_d)

        client.handle_pdu_d.callback(None)
        self.successResultOf(submit_sm_resp_d)
        resp = SubmitSMResp(123, message_id="id123", command_status="ESME_ROK")
        self.assertEqual(client.pdus_handled, [resp.obj])

        client.pdus_handled[:] = []
        # Explicit message_id.
        submit_sm_resp_d = fake_smsc.submit_sm_resp(
            SubmitSM(124).obj, message_id="foo")
        yield client.write(SubmitSM(124).get_bin())
        self.assertNoResult(submit_sm_resp_d)

        client.handle_pdu_d.callback(None)
        self.successResultOf(submit_sm_resp_d)
        resp = SubmitSMResp(124, message_id="foo", command_status="ESME_ROK")
        self.assertEqual(client.pdus_handled, [resp.obj])

    @inlineCallbacks
    def test_test_submit_sm_resp_wrong_pdu(self):
        """
        FakeSMSC will raise an exception if asked to bind with a non-bind PDU.
        """
        fake_smsc = FakeSMSC()
        client = self.successResultOf(self.connect(fake_smsc))

        submit_sm_resp_d = fake_smsc.submit_sm_resp()
        yield client.write(EnquireLink(0).get_bin())
        self.failureResultOf(submit_sm_resp_d, ValueError)
