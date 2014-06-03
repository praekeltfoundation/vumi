# coding: utf-8

"""Tests for vumi.transports.voice."""

import md5
import os

from twisted.internet.defer import (
    inlineCallbacks, DeferredQueue, returnValue, Deferred)
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor, protocol
from twisted.python import log
from twisted.test.proto_helpers import StringTransport

from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import LogCatcher
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


class EslTransport(StringTransport):

    def __init__(self):
        StringTransport.__init__(self)
        self.cmds = DeferredQueue()

    def write(self, data):
        StringTransport.write(self, data)
        self.parse_commands()

    def parse_commands(self):
        data = self.value()
        while "\n\n" in data:
            cmd_data, data = data.split("\n\n", 1)
            command = {}
            for line in cmd_data.splitlines():
                if ":" in line:
                    key, value = line.split(":", 1)
                    command[key] = value.strip()
            self.cmds.put(command)
        self.clear()
        self.io.write(data)


class TestFreeSwitchESLProtocol(VumiTestCase):

    transport_class = VoiceServerTransport
    transport_type = 'voice'

    VOICE_CMD = """
        python -c open("{filename}","w").write("{text}")
    """

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(
            TransportHelper(self.transport_class))
        self.worker = yield self.tx_helper.get_transport(
            {'freeswitch_listenport': 0})

        self.tr = EslTransport()

        self.proto = FreeSwitchESLProtocol(self.worker)
        self.proto.transport = self.tr

        self.voice_cache_folder = self.mktemp()
        os.mkdir(self.voice_cache_folder)

    def send_event(self, params):
        for key, value in params:
            self.proto.dataReceived("%s:%s\n" % (key, value))
        self.proto.dataReceived("\n")

    def send_command_reply(self, response):
        self.send_event([
            ("Content_Type", "command/reply"),
            ("Reply_Text", response),
        ])

    def assert_command(self, cmd, expected):
        expected.setdefault("call-command", "execute")
        expected.setdefault("event-lock", "true")
        if "name" in expected:
            expected["execute-app-name"] = expected.pop("name")
        if "arg" in expected:
            expected["execute-app-arg"] = expected.pop("arg")
        self.assertEqual(cmd, expected)

    @inlineCallbacks
    def assert_and_reply(self, expected, response):
        cmd = yield self.tr.cmds.get()
        self.assert_command(cmd, expected)
        self.send_command_reply(response)

    @inlineCallbacks
    def test_create_and_stream_text_as_speech_file_found(self):
        content = "Hello!"
        voice_key = md5.md5(content).hexdigest()
        voice_filename = os.path.join(
            self.voice_cache_folder, "voice-%s.wav" % voice_key)
        with open(voice_filename, "w") as f:
            f.write("Dummy voice file")

        with LogCatcher() as lc:
            d = self.proto.create_and_stream_text_as_speech(
                self.voice_cache_folder, self.VOICE_CMD, "wav", content)
            self.assertEqual(lc.messages(), [
                "Using cached voice file %r" % (voice_filename,)
            ])

        yield self.assert_and_reply({
            "name": "set",
            "arg": "playback_terminators=None",
            }, "+OK")
        yield self.assert_and_reply({
            "name": "playback",
            "arg": voice_filename,
            }, "+OK")

        yield d

        with open(voice_filename) as f:
            self.assertEqual(f.read(), "Dummy voice file")

    @inlineCallbacks
    def test_create_and_stream_text_as_speech_file_not_found(self):
        content = "Hello!"
        voice_key = md5.md5(content).hexdigest()
        voice_filename = os.path.join(
            self.voice_cache_folder, "voice-%s.wav" % voice_key)

        with LogCatcher() as lc:
            d = self.proto.create_and_stream_text_as_speech(
                self.voice_cache_folder, self.VOICE_CMD, "wav", content)
            self.assertEqual(lc.messages(), [
                "Generating voice file %r" % (voice_filename,)
            ])

        yield self.assert_and_reply({
            "name": "set",
            "arg": "playback_terminators=None",
            }, "+OK")
        yield self.assert_and_reply({
            "name": "playback",
            "arg": voice_filename,
            }, "+OK")

        yield d

        with open(voice_filename) as f:
            self.assertEqual(f.read(), "Hello!")


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
