# coding: utf-8

"""Tests for vumi.transports.telnet.transport."""

from twisted.internet.defer import (
    inlineCallbacks, DeferredQueue, returnValue, Deferred)
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor, protocol

from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.transports.telnet import (
    TelnetServerTransport, AddressedTelnetServerTransport)
from vumi.transports.tests.helpers import TransportHelper


NON_ASCII = u"öæł"


class ClientProtocol(LineReceiver):

    def __init__(self):
        self.queue = DeferredQueue()
        self.connect_d = Deferred()
        self.disconnect_d = Deferred()

    def connectionMade(self):
        self.connect_d.callback(None)

    def lineReceived(self, line):
        self.queue.put(line)

    def connectionLost(self, reason):
        self.queue.put("DONE")
        self.disconnect_d.callback(None)


class BaseTelnetServerTransortTestCase(VumiTestCase):

    transport_class = TelnetServerTransport
    transport_type = 'telnet'

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.worker = yield self.tx_helper.get_transport({'telnet_port': 0})
        self.client = yield self.make_client()
        self.add_cleanup(self.wait_for_client_deregistration)
        yield self.wait_for_client_start()

    @inlineCallbacks
    def wait_for_client_deregistration(self):
        if self.client.transport.connected:
            self.client.transport.loseConnection()
            yield self.client.disconnect_d
            # Kick off the delivery of the deregistration message.
            yield self.tx_helper.kick_delivery()

    def wait_for_client_start(self):
        return self.client.connect_d

    @inlineCallbacks
    def make_client(self):
        addr = self.worker.telnet_server.getHost()
        cc = protocol.ClientCreator(reactor, ClientProtocol)
        client = yield cc.connectTCP("127.0.0.1", addr.port)
        returnValue(client)


class TestTelnetServerTransport(BaseTelnetServerTransortTestCase):

    @inlineCallbacks
    def test_client_register(self):
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], None)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NEW)

    @inlineCallbacks
    def test_client_deregister(self):
        self.client.transport.loseConnection()
        [reg, msg] = yield self.tx_helper.wait_for_dispatched_inbound(2)
        self.assertEqual(msg['content'], None)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def test_handle_input(self):
        self.client.transport.write("foo\n")
        [reg, msg] = yield self.tx_helper.wait_for_dispatched_inbound(2)
        self.assertEqual(msg['content'], "foo")
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_handle_non_ascii_input(self):
        self.client.transport.write(NON_ASCII.encode("utf-8"))
        [reg, msg] = yield self.tx_helper.wait_for_dispatched_inbound(2)
        self.assertEqual(msg['content'], NON_ASCII)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_outbound_reply(self):
        [reg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.make_dispatch_reply(reg, "reply_foo")
        line = yield self.client.queue.get()
        self.assertEqual(line, "reply_foo")
        self.assertTrue(self.client.transport.connected)
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')

    @inlineCallbacks
    def test_non_ascii_outbound_reply(self):
        [reg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.make_dispatch_reply(reg, NON_ASCII)
        line = yield self.client.queue.get()
        self.assertEqual(line, NON_ASCII.encode('utf-8'))
        self.assertTrue(self.client.transport.connected)

    @inlineCallbacks
    def test_non_ascii_outbound_unknown_address(self):
        [reg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        reg['from_addr'] = 'nowhere'
        yield self.tx_helper.make_dispatch_reply(reg, NON_ASCII)
        line = yield self.client.queue.get()
        self.assertEqual(
            line,
            (u"UNKNOWN ADDR [nowhere]: %s" % (NON_ASCII,)).encode('utf-8'))
        self.assertTrue(self.client.transport.connected)
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['nack_reason'], u'Unknown address.')

    @inlineCallbacks
    def test_outbound_close_event(self):
        [reg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.make_dispatch_reply(
            reg, "reply_done", continue_session=False)
        line = yield self.client.queue.get()
        self.assertEqual(line, "reply_done")
        line = yield self.client.queue.get()
        self.assertEqual(line, "DONE")
        self.assertFalse(self.client.transport.connected)

    @inlineCallbacks
    def test_outbound_send(self):
        [reg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.make_dispatch_outbound(
            "send_foo", to_addr=reg['from_addr'])
        line = yield self.client.queue.get()
        self.assertEqual(line, "send_foo")
        self.assertTrue(self.client.transport.connected)
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')

    @inlineCallbacks
    def test_to_addr_override(self):
        old_worker = self.worker
        self.assertEqual(
            old_worker._to_addr,
            old_worker._format_addr(old_worker.telnet_server.getHost()))
        worker = yield self.tx_helper.get_transport({
            'telnet_port': 0,
            'to_addr': 'foo'
        })
        self.assertEqual(worker._to_addr, 'foo')
        yield worker.stopWorker()

    @inlineCallbacks
    def test_transport_type_override(self):
        self.assertEqual(self.worker._transport_type, 'telnet')
        # Clean up existing unused client.
        self.client.transport.loseConnection()
        [m_new, m_close] = yield self.tx_helper.wait_for_dispatched_inbound(2)
        self.assertEqual(m_new['transport_type'], 'telnet')
        self.assertEqual(m_close['transport_type'], 'telnet')
        self.tx_helper.clear_dispatched_inbound()

        self.worker = yield self.tx_helper.get_transport({
            'telnet_port': 0,
            'transport_type': 'foo',
        })
        self.assertEqual(self.worker._transport_type, 'foo')

        self.client = yield self.make_client()
        yield self.wait_for_client_start()
        self.client.transport.write("foo\n")
        [m_new, msg] = yield self.tx_helper.wait_for_dispatched_inbound(2)
        self.assertEqual(m_new['transport_type'], 'foo')
        self.assertEqual(msg['transport_type'], 'foo')


class TestAddressedTelnetServerTransport(BaseTelnetServerTransortTestCase):

    transport_class = AddressedTelnetServerTransport

    def wait_for_server(self):
        """Wait for first message from client to be ready."""
        return self.client.queue.get()

    @inlineCallbacks
    def test_handle_input(self):
        to_addr_prompt = yield self.wait_for_server()
        self.assertEqual('Please provide "to_addr":', to_addr_prompt)
        self.client.transport.write('to_addr\n')
        from_addr_prompt = yield self.wait_for_server()
        self.assertEqual('Please provide "from_addr":', from_addr_prompt)
        self.client.transport.write('from_addr\n')
        summary = yield self.wait_for_server()
        self.assertEqual(
            summary, "[Sending all messages to: to_addr and from: from_addr]")
        self.client.transport.write('foo!\n')
        [reg, msg] = yield self.tx_helper.wait_for_dispatched_inbound(2)

        self.assertEqual(reg['from_addr'], 'from_addr')
        self.assertEqual(reg['to_addr'], 'to_addr')

        self.assertEqual(msg['from_addr'], 'from_addr')
        self.assertEqual(msg['to_addr'], 'to_addr')
        self.assertEqual(msg['content'], 'foo!')
