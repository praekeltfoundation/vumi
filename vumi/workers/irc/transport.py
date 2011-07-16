from twisted.words.protocols import irc
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol, reactor
from twisted.python import log
from vumi.service import Worker
from vumi.message import Message
from datetime import datetime
import time, json



class VumiBot(irc.IRCClient):
    """A logging IRC bot."""
    nickname = 'twistedbot'

    def _publish_message(self, **kwargs):
        timestamp = datetime.utcnow()

        payload = {
            "nickname": kwargs.get('nickname', 'system'),
            "server": self.server,
            "channel": kwargs.get('channel', 'unknown'),
            "message_type": kwargs.get('message_type', 'message'),
            "message_content": kwargs.get('msg', ''),
            "timestamp": timestamp.isoformat()
        }

        self.publisher.publish_message(Message(**payload))

    def connectionMade(self):
        self.nickname = self.factory.nickname
        self.server = self.factory.network
        self.publisher = self.factory.publisher
        self.consumer = self.factory.consumer
        self.consumer.callback = self.consume_message
        irc.IRCClient.connectionMade(self)
        self._publish_message(message_type='system', msg="[%s connected at %s]" % (
                self.nickname, time.asctime(datetime.utcnow().timetuple())))

    def connectionLost(self, reason):
        irc.IRCClient.connectionLost(self, reason)
        self._publish_message(message_type='system', msg="[%s disconnected at %s]" % (
                self.nickname, time.asctime(datetime.utcnow().timetuple())))

    # callbacks for events

    def signedOn(self):
        """Called when bot has succesfully signed on to server."""
        log.msg("Channels: %r" % (self.factory.channels,))
        for channel in self.factory.channels:
            self.join(channel)

    def joined(self, channel):
        """This will get called when the bot joins the channel."""
        self._publish_message(message_type='system', msg="[%s has joined %s]" % (
                self.nickname, channel))

    def privmsg(self, user, channel, msg):
        """This will get called when the bot receives a message."""
        user = user.split('!', 1)[0]
        self._publish_message(nickname=user, channel=channel, msg=msg)

    def action(self, user, channel, msg):
        """This will get called when the bot sees someone do an action."""
        user = user.split('!', 1)[0]
        self._publish_message(message_type='action', nickname=user, channel=channel, msg=msg)

    # irc callbacks

    def irc_NICK(self, prefix, params):
        """Called when an IRC user changes their nickname."""
        old_nick = prefix.split('!')[0]
        new_nick = params[0]
        self._publish_message(message_type='nick_change', nickname=old_nick, msg=new_nick)


    # For fun, override the method that determines how a nickname is changed on
    # collisions. The default method appends an underscore.
    def alterCollidedNick(self, nickname):
        """
        Generate an altered version of a nickname that caused a collision in an
        effort to create an unused related name for subsequent registration.
        """
        return nickname + '^'

    def consume_message(self, message):
        log.msg('Consumed Message with %s' % (message.payload,))
        try:
            payload = message.payload
            msg_type = payload['message_type']
            if msg_type == 'message':
                self.msg(payload['channel'].encode('utf8'),
                         payload['message_content'].encode('utf8'))
        except Exception, e:
            log.msg("Oops: %r" % (e,))


class VumiBotFactory(protocol.ReconnectingClientFactory):
    """A factory for VumiBots.

    A new protocol instance will be created each time we connect to the server.
    """

    # the class of the protocol to build when new connection is made
    protocol = VumiBot

    def __init__(self, network, nickname, channels, publisher, consumer):
        self.network = network
        self.nickname = nickname
        self.channels = channels
        self.publisher = publisher
        self.consumer = consumer

    def buildProtocol(self, addr):
        self.resetDelay()
        p = self.protocol()
        p.factory = self
        return p


class IrcTransport(Worker):

    @inlineCallbacks
    def startWorker(self):
        network = self.config.get('network', 'irc.freenode.net')
        channels = self.config.get('channels', [])
        nickname = self.config.get('nickname', 'vumibot')
        inbound = self.config.get('inbound', 'irc.inbound')
        outbound = self.config.get('outbound', 'irc.outbound')
        self.publisher = yield self.publish_to(inbound)
        self.name = nickname
        self.consumer = yield self.consume(outbound, self.consume_message,
                                           '%s.%s' % (outbound, self.name))
        port = self.config.get('port', 6667)

        # create factory protocol and application
        f = VumiBotFactory(network, nickname, channels, self.publisher, self.consumer)

        # connect factory to this host and port
        reactor.connectTCP(network, port, f)

    def consume_message(self, message):
        log.msg('Consumed Message with %s, but have nowhere to send it. :-(' % message.payload)

    @inlineCallbacks
    def stopWorker(self):
        yield None
