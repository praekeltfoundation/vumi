# -*- test-case-name: vumi.transports.voice.tests.test_voice -*-

"""Transport that sends text as voice to a standard SIP client via
   the Freeswitch ESL interface. Keypad input from the phone is sent
   through as a vumi message
   """

from twisted.internet import reactor
from twisted.internet.protocol import ServerFactory
from twisted.internet.defer import inlineCallbacks, Deferred, gatherResults
from twisted.python import log

from vumi.transports import Transport
from vumi.message import TransportUserMessage
from vumi.config import ConfigServerEndpoint, ConfigInt, ConfigDict, ConfigText

import sys
import os.path
from . import freeswitchesl


class FreeSwitchESLProtocol(freeswitchesl.FreeSwitchEventProtocol):

    """Custom implementation of the Freeswitch event socket interface"""

    def __init__(self, vumi_transport):
        self.vumi_transport = vumi_transport
        freeswitchesl.FreeSwitchEventProtocol.__init__(self)
        self.request_hang_up = False

    @inlineCallbacks
    def connectionMade(self):
        log.msg("start connect")
        yield self.connect()
        log.msg("done connect")
        yield self.myevents()
        yield self.answer()
        yield self.vumi_transport.register_client(self)

    @inlineCallbacks
    def onDtmf(self, ev):
        print "DTMF:", ev.DTMF_Digit
        yield self.vumi_transport.handle_input(self, ev.DTMF_Digit)

    @inlineCallbacks
    def createAndStreamTextAsSpeech(self, engine, voice, message):
        os.system("%s -w /usr/src/freeswitch/pySource/media/test.wav \"%s\"" %
                  (engine, message))
        yield self.playback("/usr/src/freeswitch/pySource/media/test.wav")

    @inlineCallbacks
    def sendTextAsSpeech(self, engine, voice, message):
        yield self.set("tts_engine=" + engine)
        yield self.set("tts_voice=" + voice)
        yield self.execute("speak", message)

    def streamTextAsSpeech(self, message):
        finalmessage = message.replace("\n", " . ")
        log.msg("TTS: " + finalmessage + "\n")
        if (self.vumi_transport.config.tts_type == "local"):
            self.createAndStreamTextAsSpeech(
                self.vumi_transport.config.tts_engine,
                self.vumi_transport.config.tts_voice, finalmessage)
        else:
            self.sendTextAsSpeech(
                self.vumi_transport.config.tts_engine,
                self.vumi_transport.config.tts_voice, finalmessage)

    def getAddress(self):
        return self.uniquecallid

    def outputMessage(self, text):
        self.streamTextAsSpeech(text)

    def closeCall(self):
        self.request_hang_up = True

    def onChannelExecuteComplete(self, ev):
        log.msg("execute complete " + ev.variable_call_uuid)
        if self.request_hang_up:
            self.hangup()
            self.vumi_transport.deregister_client(self)

    def onChannelHangup(self, ev):
        self.vumi_transport.deregister_client(self)
        log.msg("User hung up")

    def unboundEvent(self, evdata, evname):
        pass


class VoiceServerTransportConfig(Transport.CONFIG_CLASS):

    """
    Configuration parameters for the voice transport
    """
    tts_type = ConfigText(
        "Either 'freeswitch' or 'local' to specify were tts is executed.",
        default="freeswitch", static=True)

    tts_engine = ConfigText(
        "Specify tts engine to use.", default="flite", static=True)

    tts_voice = ConfigText(
        "Specify tts voice to use.", default="kal", static=True)

    freeswitch_listenport = ConfigInt(
        "Port number that freeswitch will attempt to connect on",
        default=8084, static=True)


class VoiceServerTransport(Transport):

    """Transport for Freeswitch Voice Service.

    Voice transports may receive additional hints for how to handle
    outbound messages the ``voice`` section of ``helper_metadata``.
    The ``voice`` section may contain the following keys:

    * ``speech_url``: An HTTP URL from which a custom sound file to
      use for this message. If absent or ``None`` a text-to-speech
      engine will be used to generate suitable sound to play.
      Sound formats supported are: ``.wav``, ``.ogg`` and ``.mp3``.
      The format will be determined by the ``Content-Type`` returned
      by the URL, or by the file extension if the ``Content-Type``
      header is absent. The preferred format is ``.ogg``.

    * ``wait_for``: Gather response characters until the given
      DTMF character is encountered. Commonly either ``#`` or ``*``.
      If absent or ``None``, an inbound message is sent as soon as
      a single DTMF character arrives.

      If no input is seen for some time (configurable in the
      transport config) the voice transport will timeout the wait
      and send the characters entered so far.

      .. todo:

         Maybe ``wait_for`` should default to ``#``? It's not
         discoverable but at least it makes it possible to enter
         multi-digit numbers by default and it's probably simpler
         to add a bit of help text to an application that to
         update it to send ``helper_metadata``.

    Example ``helper_metadata``::

      "helper_metadata": {
          "voice": {
              "speech_url": "http://www.example.com/voice/ab34f611cdee.ogg",
              "wait_for": "#",
          },
      }
    """

    CONFIG_CLASS = VoiceServerTransportConfig

    @inlineCallbacks
    def setup_transport(self):
        self._clients = {}

        self.config = self.get_static_config()
        self._to_addr = "freeswitchvoice"
        self._transport_type = "voice"

        def protocol():
            return FreeSwitchESLProtocol(self)

        factory = ServerFactory()
        factory.protocol = protocol

        yield reactor.listenTCP(self.config.freeswitch_listenport, factory)

    def teardown_transport(self):
        pass

    def register_client(self, client):
        # We add our own Deferred to the client here because we only want to
        # fire it after we're finished with our own deregistration process.
        client.registration_d = Deferred()
        client_addr = client.getAddress()
        log.msg("Registering client connected from %r" % client_addr)
        self._clients[client_addr] = client
        self.send_inbound_message(client, None,
                                  TransportUserMessage.SESSION_NEW)
        log.msg("Register completed")

    def deregister_client(self, client):
        log.msg("Deregistering client.")
        self.send_inbound_message(
            client, None, TransportUserMessage.SESSION_CLOSE)
        del self._clients[client.getAddress()]
        client.registration_d.callback(None)

    def handle_input(self, client, text):
        self.send_inbound_message(client, text,
                                  TransportUserMessage.SESSION_RESUME)

    def send_inbound_message(self, client, text, session_event):
        self.publish_message(
            from_addr=client.getAddress(),
            to_addr=self._to_addr,
            session_event=session_event,
            content=text,
            transport_name=self.transport_name,
            transport_type=self._transport_type,
        )

    def handle_outbound_message(self, message):
        text = message['content']
        if text is None:
            text = u''
        text = u"\n".join(text.splitlines())

        client_addr = message['to_addr']
        client = self._clients.get(client_addr)

        text = text.encode('utf-8')
        client.outputMessage("%s\n" % text)

        if message['session_event'] == TransportUserMessage.SESSION_CLOSE:
            client.closeCall()
