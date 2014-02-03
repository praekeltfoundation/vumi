# coding: utf-8

"""Tests for vumi.transports.voice."""

from twisted.internet.defer import (
    inlineCallbacks, DeferredQueue, returnValue, Deferred)
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor, protocol

from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.transports.voice import VoiceServerTransport
from vumi.transports.tests.helpers import TransportHelper


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


class BaseVoiceServerTransportTestCase(VumiTestCase):

    transport_class = VoiceServerTransport
    transport_type = 'voice'

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.worker = yield self.tx_helper.get_transport({'freeswitch_listenport': 0})
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
        cc = protocol.ClientCreator(reactor, ClientProtocol)
        client = yield cc.connectTCP("127.0.0.1", 0)
        returnValue(client)


class TestVoiceServerTransport(BaseVoiceServerTransportTestCase):

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

    

    
