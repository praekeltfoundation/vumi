# coding: utf-8

"""Tests for vumi.transports.voice."""

from twisted.internet.defer import (
    inlineCallbacks, DeferredQueue, returnValue, Deferred)
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor, protocol
from twisted.python import log
from twisted.test import proto_helpers

from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.transports.voice import VoiceServerTransport
from vumi.transports.voice.voice import FreeSwitchESLProtocol
from vumi.transports.tests.helpers import TransportHelper


class FakeFreeswitchProtocol(LineReceiver):

    testAddr = 'TESTTEST'

    def __init__(self):
        self.queue = DeferredQueue()
        self.connect_d = Deferred()
        self.disconnect_d = Deferred()
        self.setRawMode()

    def connectionMade(self):
        self.connected = True
        self.connect_d.callback(None)

    def sendCommandReply(self, params=""):
        self.sendLine('Content-Type: command/reply\nReply-Text: +OK\n%s\n\n' %
                      params)

    def sendDisconnectEvent(self):
        self.sendLine('Content-Type: text/disconnect-notice\n\n')

    def sendDtmfEvent(self, digit):
        data = 'Event-Name: DTMF\nDTMF-Digit: %s\n\n' % digit
        self.sendLine(
            'Content-Length: %d\nContent-Type: text/event-plain\n\n%s' %
            (len(data), data))

    def rawDataReceived(self, data):
        log.msg('TRACE: Client Protocol got raw data: ' + data)
        if data.startswith('connect'):
            self.sendCommandReply('variable-call-uuid: %s' % self.testAddr)
        if data.startswith('myevents'):
            self.sendCommandReply()
        if data.startswith('sendmsg'):
            self.sendCommandReply()
            if (data.find("execute-app-name: speak") > 0):
                self.queue.put("TTS")

    def connectionLost(self, reason):
        self.connected = False
        self.disconnect_d.callback(None)


class TestFreeSwitchESLProtocol(VumiTestCase):

    transport_class = VoiceServerTransport
    transport_type = 'voice'

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(
            TransportHelper(self.transport_class))
        self.worker = yield self.tx_helper.get_transport(
            {'freeswitch_listenport': 0})
        self.proto = FreeSwitchESLProtocol(self.worker)
        self.tr = proto_helpers.StringTransport()
        self.proto.transport = self.tr

    def send_event(self, params):
        for key, value in params:
            self.proto.dataReceived("%s:%s\n" % (key, value))
        self.proto.dataReceived("\n")

    def send_command_reply(self, response):
        self.send_event([
            ("Content_Type", "command/reply"),
            ("Reply_Text", response),
        ])

    def assert_commands(self, expected_commands):
        commands = []
        command = None
        for line in self.tr.value().splitlines():
            line = line.strip()
            if not line:
                if command is not None:
                    commands.append(command)
                command = None
            elif line == "sendmsg":
                command = {}
            else:
                key, value = line.split(":", 1)
                command[key] = value.strip()
        for cmd in expected_commands:
            cmd.setdefault("call-command", "execute")
            cmd.setdefault("event-lock", "true")
            if "name" in cmd:
                cmd["execute-app-name"] = cmd.pop("name")
            if "arg" in cmd:
                cmd["execute-app-arg"] = cmd.pop("arg")
        self.assertEqual(commands, expected_commands)

    @inlineCallbacks
    def test_create_and_stream_text_as_speech_file_found(self):
        d = self.proto.create_and_stream_text_as_speech(
            "engine", "voice1", "Hello!")
        self.send_command_reply("+OK")
        self.send_command_reply("+OK")
        yield d
        self.assert_commands([{
            "name": "set",
            "arg": "playback_terminators=None",
        }, {
            "name": "playback",
            "arg": "/tmp/voice5908486175754136239.wav",
        }])

    @inlineCallbacks
    def test_create_and_stream_text_as_speech_file_not_found(self):
        yield None


class TestVoiceServerTransport(VumiTestCase):

    transport_class = VoiceServerTransport
    transport_type = 'voice'

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.worker = yield self.tx_helper.get_transport(
            {'freeswitch_listenport': 0})
        self.client = yield self.make_client()
        self.add_cleanup(self.wait_for_client_deregistration)
        yield self.wait_for_client_start()

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

    @inlineCallbacks
    def test_client_register(self):
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], None)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NEW)

    @inlineCallbacks
    def test_client_deregister(self):
        # wait for registration message
        yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.clear_dispatched_inbound()
        self.client.sendDisconnectEvent()
        self.client.transport.loseConnection()
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], None)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def test_simplemessage(self):
        [reg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.make_dispatch_reply(reg, "voice test")
        line = yield self.client.queue.get()
        self.assertEqual(line, "TTS")

    @inlineCallbacks
    def test_simpledigitcapture(self):
        yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.clear_dispatched_inbound()
        self.client.sendDtmfEvent('5')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], '5')

    @inlineCallbacks
    def test_multidigitcapture(self):
        [reg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.clear_dispatched_inbound()

        yield self.tx_helper.make_dispatch_reply(
            reg, 'voice test', helper_metadata={'voice': {'wait_for': '#'}})
        line = yield self.client.queue.get()
        self.assertEqual(line, "TTS")
        self.client.sendDtmfEvent('5')
        self.client.sendDtmfEvent('7')
        self.client.sendDtmfEvent('2')
        self.client.sendDtmfEvent('#')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], '572')
