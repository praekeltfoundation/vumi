# coding: utf-8

"""Tests for vumi.transports.voice."""

from twisted.internet.defer import (
    inlineCallbacks, DeferredQueue, returnValue, Deferred)
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor, protocol
from twisted.python import log

from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.transports.voice import VoiceServerTransport
from vumi.transports.tests.helpers import TransportHelper


class FakeFreeswitchProtocol(LineReceiver):

    def __init__(self):
        log.msg("TRACE:Test client proto __init__")
        self.queue = DeferredQueue()
        self.connect_d = Deferred()
        self.disconnect_d = Deferred()
        self.setRawMode()

    def connectionMade(self):
        log.msg("TRACE:Test client proto connection made")
        self.connected=True        
        self.connect_d.callback(None)

    def sendCommandReply(self, params=""):
        self.sendLine("Content-Type: command/reply\nReply-Text: +OK\n%s\n\n" %
                      params)
                      
    def sendDisconnectEvent(self):
        self.sendLine("Content-Type: text/disconnect-notice\n\n")

    def rawDataReceived(self, data):
        log.msg("TRACE: Client Protocol got raw data: " + data)
        if data.startswith("connect"):
            self.sendCommandReply("variable-call-uuid: TESTTESTTESTTEST")
        if data.startswith("myevents"):
            self.sendCommandReply()
        if data.startswith("sendmsg"):
            self.sendCommandReply()

    def lineReceived(self, line):
        self.queue.put(line)

    def connectionLost(self, reason):
        log.msg("TRACE: Freeswitch Connection lost")    
        self.connected=False    
        self.disconnect_d.callback(None)


class BaseVoiceServerTransportTestCase(VumiTestCase):

    transport_class = VoiceServerTransport
    transport_type = 'voice'
    timeout=1;

    @inlineCallbacks
    def setUp(self):
        log.msg("TRACE:Set Up ")
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.worker = yield self.tx_helper.get_transport(
             {'freeswitch_listenport': 8084})
        self.client = yield self.make_client()
        self.add_cleanup(self.wait_for_client_deregistration)
        yield self.wait_for_client_start()
        log.msg("TRACE:Set Up Complete")

    @inlineCallbacks
    def wait_for_client_deregistration(self):
        if self.client.transport.connected:
            self.client.sendDisconnectEvent()
            self.client.transport.loseConnection()
            yield self.client.disconnect_d        
            yield self.tx_helper.kick_delivery()            

    def wait_for_client_start(self):
        return self.client.connect_d

    @inlineCallbacks
    def make_client(self):
        addr = self.worker.voice_server.getHost()
        cc = protocol.ClientCreator(reactor, FakeFreeswitchProtocol)
        client = yield cc.connectTCP("127.0.0.1", addr.port)
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
        #wait for registration message
        yield self.tx_helper.wait_for_dispatched_inbound(1) 
        self.tx_helper.clear_dispatched_inbound()
        self.client.sendDisconnectEvent()
        self.client.transport.loseConnection()
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], None)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)
        
