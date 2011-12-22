# -*- encoding: utf-8 -*-

"""Tests for vumi.transports.telnet.transport."""

from twisted.internet.defer import (inlineCallbacks, DeferredQueue,
                                    returnValue)
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor, protocol

from vumi.message import TransportUserMessage
from vumi.transports.telnet import TelnetServerTransport
from vumi.transports.tests.test_base import TransportTestCase


class ClientProtocol(LineReceiver):

    def __init__(self):
        self.queue = DeferredQueue()

    def lineReceived(self, line):
        self.queue.put(line)

    def connectionLost(self, reason):
        self.queue.put("DONE")


class TelnetServerTransportTestCase(TransportTestCase):

    transport_name = 'test'
    transport_type = 'telnet'
    transport_class = TelnetServerTransport

    @inlineCallbacks
    def setUp(self):
        super(TelnetServerTransportTestCase, self).setUp()
        self.worker = yield self.get_transport({'telnet_port': 0})
        self.client = yield self.make_client()
        yield self.wait_for_client_start()

    def tearDown(self):
        super(TelnetServerTransportTestCase, self).tearDown()
        if self.client.transport.connected:
            self.client.transport.loseConnection()

    def get_messages(self, rkey, cls=TransportUserMessage):
        contents = self._amqp.get_dispatched('vumi', rkey)
        messages = [cls.from_json(content.body)
                    for content in contents]
        return messages

    def wait_for_client_start(self):
        """Wait for first message from client to be ready."""
        return self._amqp.wait_messages('vumi', 'test.inbound', 1)

    @inlineCallbacks
    def make_client(self):
        addr = self.worker.telnet_server.getHost()
        cc = protocol.ClientCreator(reactor, ClientProtocol)
        client = yield cc.connectTCP("127.0.0.1", addr.port)
        returnValue(client)

    def test_client_register(self):
        [msg] = self.get_messages('test.inbound')
        self.assertEqual(msg['content'], None)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NEW)

    @inlineCallbacks
    def test_client_deregister(self):
        self.client.transport.loseConnection()
        yield self._amqp.wait_messages('vumi', 'test.inbound', 2)
        [reg, msg] = self.get_messages('test.inbound')
        self.assertEqual(msg['content'], None)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def test_handle_input(self):
        self.client.transport.write("foo\n")
        yield self._amqp.wait_messages('vumi', 'test.inbound', 2)
        [reg, msg] = self.get_messages('test.inbound')
        self.assertEqual(msg['content'], "foo")
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_handle_non_ascii_input(self):
        self.client.transport.write(u"öæł".encode("utf-8"))
        yield self._amqp.wait_messages('vumi', 'test.inbound', 2)
        [reg, msg] = self.get_messages('test.inbound')
        self.assertEqual(msg['content'], u"öæł")
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)

    @inlineCallbacks
    def test_outbound_message(self):
        [reg] = self.get_messages('test.inbound')
        reply = reg.reply(content="reply_foo", continue_session=False)
        yield self.dispatch(reply)
        line = yield self.client.transport.protocol.queue.get()
        self.assertEqual(line, "reply_foo")
        self.assertTrue(self.client.transport.connected)

    @inlineCallbacks
    def test_outbound_close_event(self):
        [reg] = self.get_messages('test.inbound')
        reply = reg.reply(content="reply_done", continue_session=False)
        yield self.dispatch(reply)
        line = yield self.client.transport.protocol.queue.get()
        self.assertEqual(line, "reply_done")
        line = yield self.client.transport.protocol.queue.get()
        self.assertEqual(line, "DONE")
        self.assertFalse(self.client.transport.connected)
