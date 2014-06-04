# coding: utf-8

"""Tests for vumi.transports.voice."""

import md5
import os

from twisted.internet.defer import (
    inlineCallbacks, DeferredQueue, returnValue, Deferred)
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor, protocol
from twisted.test.proto_helpers import StringTransport

from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import LogCatcher
from vumi.transports.voice import VoiceServerTransport
from vumi.transports.voice.voice import FreeSwitchESLProtocol
from vumi.transports.tests.helpers import TransportHelper


class EslCommand(object):
    """
    An object representing an ESL command.
    """
    def __init__(self, cmd_type, params=None):
        self.cmd_type = cmd_type
        self.params = params if params is not None else {}

    def __repr__(self):
        return "<%s cmd_type=%r params=%r>" % (
            self.__class__.__name__, self.cmd_type, self.params)

    def __eq__(self, other):
        if not isinstance(other, EslCommand):
            return NotImplemented
        return (self.cmd_type == other.cmd_type and
                self.params == other.params)

    def __getitem__(self, name):
        return self.params.get(name)

    def __setitem__(self, name, value):
        self.params[name] = value

    @classmethod
    def from_dict(cls, d):
        """
        Convert a dict to an :class:`EslCommand`.
        """
        cmd_type = d.get("type")
        params = {
            "call-command": d.get("call-command", "execute"),
            "event-lock": d.get("event-lock", "true"),
        }
        if "name" in d:
            params["execute-app-name"] = d.get("name")
        if "arg" in d:
            params["execute-app-arg"] = d.get("arg")
        return cls(cmd_type, params)


class EslParser(object):
    """
    Simple in-efficient parser for the FreeSwitch eventsocket protocol.
    """

    def __init__(self):
        self.data = ""

    def parse(self, new_data):
        data = self.data + new_data
        cmds = []
        while "\n\n" in data:
            cmd_data, data = data.split("\n\n", 1)
            command = EslCommand("unknown")
            first_line = True
            for line in cmd_data.splitlines():
                line = line.strip()
                if not line:
                    continue
                if first_line:
                    command.cmd_type = line.strip()
                    first_line = False
                    continue
                if ":" in line:
                    key, value = line.split(":", 1)
                    command[key] = value.strip()
            cmds.append(command)
        self.data = data
        return cmds


class FakeFreeswitchProtocol(LineReceiver):

    testAddr = 'TESTTEST'

    def __init__(self):
        self.esl_parser = EslParser()
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
        for cmd in self.esl_parser.parse(data):
            if cmd.cmd_type == "connect":
                self.sendCommandReply('variable-call-uuid: %s' % self.testAddr)
            elif cmd.cmd_type == "myevents":
                self.sendCommandReply()
            elif cmd.cmd_type == "sendmsg":
                self.sendCommandReply()
                cmd_name = cmd.params.get('execute-app-name')
                if cmd_name == "speak":
                    self.queue.put(cmd)
                elif cmd_name == "playback":
                    self.queue.put(cmd)

    def connectionLost(self, reason):
        self.connected = False
        self.disconnect_d.callback(None)


class EslTransport(StringTransport):

    def __init__(self):
        StringTransport.__init__(self)
        self.cmds = DeferredQueue()
        self.esl_parser = EslParser()

    def write(self, data):
        StringTransport.write(self, data)
        for cmd in self.esl_parser.parse(data):
            self.cmds.put(cmd)


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
            {'twisted_endpoint': 'tcp:port=0'})

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

    @inlineCallbacks
    def assert_and_reply(self, expected, response):
        cmd = yield self.tr.cmds.get()
        expected_cmd = EslCommand.from_dict(expected)
        self.assertEqual(cmd, expected_cmd)
        self.send_command_reply(response)

    def test_create_tts_command(self):
        self.assertEqual(
            self.proto.create_tts_command("foo", "myfile", "hi!"),
            ("foo", []))
        self.assertEqual(
            self.proto.create_tts_command(
                "foo -f {filename} -t {text}", "myfile", "hi!"),
            ("foo", ["-f", "myfile", "-t", "hi!"]))

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
            "type": "sendmsg", "name": "set",
            "arg": "playback_terminators=None",
        }, "+OK")
        yield self.assert_and_reply({
            "type": "sendmsg", "name": "playback",
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
            "type": "sendmsg", "name": "set",
            "arg": "playback_terminators=None",
        }, "+OK")
        yield self.assert_and_reply({
            "type": "sendmsg", "name": "playback",
            "arg": voice_filename,
        }, "+OK")

        yield d

        with open(voice_filename) as f:
            self.assertEqual(f.read(), "Hello!")

    @inlineCallbacks
    def test_send_text_as_speech(self):
        d = self.proto.send_text_as_speech(
            "thomas", "his_masters_voice", "hi!")

        yield self.assert_and_reply({
            "type": "sendmsg", "name": "set",
            "arg": "tts_engine=thomas",
        }, "+OK")
        yield self.assert_and_reply({
            "type": "sendmsg", "name": "set",
            "arg": "tts_voice=his_masters_voice",
        }, "+OK")
        yield self.assert_and_reply({
            "type": "sendmsg", "name": "speak",
            "arg": "hi!",
        }, "+OK")

        yield d

    def test_unboundEvent(self):
        with LogCatcher() as lc:
            self.proto.unboundEvent({"some": "data"}, "custom_event")
            self.assertEqual(lc.messages(), [
                "Unbound event 'custom_event': {'some': 'data'}",
            ])


class TestVoiceServerTransport(VumiTestCase):

    transport_class = VoiceServerTransport
    transport_type = 'voice'

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.worker = yield self.tx_helper.get_transport(
            {'twisted_endpoint': 'tcp:port=0'})
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
        cmd = yield self.client.queue.get()
        self.assertEqual(cmd, EslCommand.from_dict({
            'type': 'sendmsg', 'name': 'speak', 'arg': 'voice test .',
        }))

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

        cmd = yield self.client.queue.get()
        self.assertEqual(cmd, EslCommand.from_dict({
            'type': 'sendmsg', 'name': 'speak', 'arg': 'voice test .',
        }))

        self.client.sendDtmfEvent('5')
        self.client.sendDtmfEvent('7')
        self.client.sendDtmfEvent('2')
        self.client.sendDtmfEvent('#')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], '572')

    @inlineCallbacks
    def test_speech_url(self):
        [reg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.clear_dispatched_inbound()

        yield self.tx_helper.make_dispatch_reply(
            reg, 'speech url test', helper_metadata={
                'voice': {
                    'speech_url': 'http://example.com/speech_url_test.ogg'
                }
            })

        cmd = yield self.client.queue.get()
        self.assertEqual(cmd, EslCommand.from_dict({
            'type': 'sendmsg', 'name': 'playback',
            'arg': 'http://example.com/speech_url_test.ogg',
        }))
