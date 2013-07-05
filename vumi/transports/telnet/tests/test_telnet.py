# coding: utf-8

"""Tests for vumi.transports.telnet.transport."""

from twisted.internet.defer import (inlineCallbacks, DeferredQueue,
                                    returnValue)
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor, protocol

from vumi.message import TransportUserMessage
from vumi.transports.telnet import (TelnetServerTransport,
                                    AddressedTelnetServerTransport)
from vumi.transports.tests.utils import TransportTestCase


NON_ASCII = u"öæł"


class ClientProtocol(LineReceiver):

    def __init__(self):
        self.queue = DeferredQueue()

    def lineReceived(self, line):
        self.queue.put(line)

    def connectionLost(self, reason):
        self.queue.put("DONE")


class BaseTelnetServerTransortTestCase(TransportTestCase):

    transport_name = 'test'
    transport_type = 'telnet'
    transport_class = TelnetServerTransport

    @inlineCallbacks
    def setUp(self):
        super(BaseTelnetServerTransortTestCase, self).setUp()
        self.worker = yield self.get_transport({'telnet_port': 0})
        self.client = yield self.make_client()
        yield self.wait_for_client_start()

    @inlineCallbacks
    def tearDown(self):
        if self.client.transport.connected:
            self.client.transport.loseConnection()
            self.clear_dispatched_messages()
            # Wait for all registered clients to get their disconnects.
            yield self.wait_for_dispatched_messages(len(self.worker._clients))
        yield super(BaseTelnetServerTransortTestCase, self).tearDown()

    def wait_for_client_start(self):
        """Wait for first message from client to be ready."""
        return self.wait_for_dispatched_messages(1)

    def get_dispatched_messages(self):
        return [TransportUserMessage.from_json(m.to_json())
                for m in super(BaseTelnetServerTransortTestCase,
                               self).get_dispatched_messages()]

    @inlineCallbacks
    def make_client(self):
        addr = self.worker.telnet_server.getHost()
        cc = protocol.ClientCreator(reactor, ClientProtocol)
        client = yield cc.connectTCP("127.0.0.1", addr.port)
        returnValue(client)


class TelnetServerTransportTestCase(BaseTelnetServerTransortTestCase):

    @inlineCallbacks
    def test_client_register(self):
        [msg] = yield self.get_dispatched_messages()
        self.assertEqual(msg['content'], None)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NEW)

    @inlineCallbacks
    def test_client_deregister(self):
        self.client.transport.loseConnection()
        yield self.wait_for_dispatched_messages(2)
        [reg, msg] = yield self.get_dispatched_messages()
        self.assertEqual(msg['content'], None)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def test_handle_input(self):
        self.client.transport.write("foo\n")
        yield self.wait_for_dispatched_messages(2)
        [reg, msg] = yield self.get_dispatched_messages()
        self.assertEqual(msg['content'], "foo")
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_handle_non_ascii_input(self):
        self.client.transport.write(NON_ASCII.encode("utf-8"))
        yield self.wait_for_dispatched_messages(2)
        [reg, msg] = yield self.get_dispatched_messages()
        self.assertEqual(msg['content'], NON_ASCII)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_outbound_reply(self):
        [reg] = yield self.get_dispatched_messages()
        reply = reg.reply(content="reply_foo", continue_session=False)
        yield self.dispatch(reply)
        line = yield self.client.transport.protocol.queue.get()
        self.assertEqual(line, "reply_foo")
        self.assertTrue(self.client.transport.connected)

    @inlineCallbacks
    def test_non_ascii_outbound_reply(self):
        [reg] = yield self.get_dispatched_messages()
        reply = reg.reply(content=NON_ASCII, continue_session=False)
        yield self.dispatch(reply)
        line = yield self.client.transport.protocol.queue.get()
        self.assertEqual(line, NON_ASCII.encode('utf-8'))
        self.assertTrue(self.client.transport.connected)

    @inlineCallbacks
    def test_non_ascii_outbound_unknown_address(self):
        [reg] = yield self.get_dispatched_messages()
        reply = reg.reply(content=NON_ASCII, continue_session=False)
        reply['to_addr'] = 'nowhere'
        yield self.dispatch(reply)
        line = yield self.client.transport.protocol.queue.get()
        self.assertEqual(line,
            (u"UNKNOWN ADDR [nowhere]: %s" % (NON_ASCII,)).encode('utf-8'))
        self.assertTrue(self.client.transport.connected)

    @inlineCallbacks
    def test_outbound_close_event(self):
        [reg] = yield self.get_dispatched_messages()
        reply = reg.reply(content="reply_done", continue_session=False)
        yield self.dispatch(reply)
        line = yield self.client.transport.protocol.queue.get()
        self.assertEqual(line, "reply_done")
        line = yield self.client.transport.protocol.queue.get()
        self.assertEqual(line, "DONE")
        self.assertFalse(self.client.transport.connected)

    @inlineCallbacks
    def test_outbound_send(self):
        [reg] = yield self.get_dispatched_messages()
        msg = self.mkmsg_out(content="send_foo", to_addr=reg['from_addr'])
        yield self.dispatch(msg)
        line = yield self.client.transport.protocol.queue.get()
        self.assertEqual(line, "send_foo")
        self.assertTrue(self.client.transport.connected)

    @inlineCallbacks
    def test_to_addr_override(self):
        old_worker = self.worker
        self.assertEqual(old_worker._to_addr,
            old_worker._format_addr(old_worker.telnet_server.getHost()))
        worker = yield self.get_transport({
            'telnet_port': 0,
            'to_addr': 'foo'
        })
        self.assertEqual(worker._to_addr, 'foo')
        yield worker.stopWorker()

    @inlineCallbacks
    def test_transport_type_override(self):
        self.assertEqual(self.worker._transport_type, 'telnet')
        self.worker = yield self.get_transport({
            'telnet_port': 0,
            'transport_type': 'foo',
        })
        self.assertEqual(self.worker._transport_type, 'foo')
        # Clean up existing unused client.
        self.client.transport.loseConnection()

        self.client = yield self.make_client()
        yield self.wait_for_client_start()
        self.client.transport.write("foo\n")
        [r1, r2, msg] = yield self.wait_for_dispatched_messages(3)
        self.assertEqual(msg['transport_type'], 'foo')


class AddressedTelnetServerTransportTestCase(BaseTelnetServerTransortTestCase):

    transport_class = AddressedTelnetServerTransport

    @inlineCallbacks
    def setUp(self):
        super(BaseTelnetServerTransortTestCase, self).setUp()
        self.worker = yield self.get_transport({'telnet_port': 0})
        self.client = yield self.make_client()

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
        self.assertEqual(summary,
            "[Sending all messages to: to_addr and from: from_addr]")
        self.client.transport.write('foo!\n')
        [reg, msg] = yield self.wait_for_dispatched_messages(2)

        self.assertEqual(reg['from_addr'], 'from_addr')
        self.assertEqual(reg['to_addr'], 'to_addr')

        self.assertEqual(msg['from_addr'], 'from_addr')
        self.assertEqual(msg['to_addr'], 'to_addr')
        self.assertEqual(msg['content'], 'foo!')
