"""IRC transport."""

from twisted.words.protocols import irc
from twisted.internet import protocol, reactor
from twisted.python import log

from vumi.transports import Transport


class IrcMessage(object):
    """Container for details of a message to or from an IRC user.

    :type sender: str
    :param sender:
        Who send the message (usually user!ident@hostmask).
    :type recipient: str
    :param recipient:
        User or channel recieving the message.
    :type content: str
    :param content:
        Contents of message.
    :type action: bool
    :param action:
        Whether the message is an action.
    """

    def __init__(self, sender, recipient, content, action=False):
        self.sender = sender
        self.recipient = recipient
        self.content = content
        self.action = action

    def __eq__(self, other):
        if isinstance(other, IrcMessage):
            return all(getattr(self, name) == getattr(other, name)
                       for name in ("sender", "recipient", "content",
                                    "action"))
        return False

    def channel(self):
        """Return the channel if the recipient is a channel.

        Otherwise return None.
        """
        if self.recipient[:1] in ('#', '&', '$'):
            return self.recipient
        return None

    def addressed_to(self, nickname):
        if not self.channel():
            return self.recipient.partition('!')[0] == nickname
        parts = self.content.split(None, 1)
        maybe_nickname = parts[0].rstrip(':,') if parts else ''
        return maybe_nickname.lower() == nickname.lower()


class VumiBotProtocol(irc.IRCClient):
    """An IRC bot that bridges IRC to Vumi."""

    def __init__(self, nickname, channels, irc_transport):
        self.nickname = nickname
        self.channels = channels
        self.irc_transport = irc_transport

    def publish_message(self, irc_msg):
        self.irc_transport.handle_inbound_irc_message(irc_msg)

    def consume_message(self, irc_msg):
        self.msg(irc_msg.recipient.encode('utf8'),
                 irc_msg.content.encode('utf8'))

    # connecting and disconnecting from server

    def connectionMade(self):
        irc.IRCClient.connectionMade(self)
        log.msg("Connected (nickname is: %s)" % (self.nickname,))

    def connectionLost(self, reason):
        irc.IRCClient.connectionLost(self, reason)
        log.msg("Disconnected (nickname was: %s)." % (self.nickname,))

    # callbacks for events

    def signedOn(self):
        """Called when bot has succesfully signed on to server."""
        log.msg("Attempting to join channels: %r" % (self.channels,))
        for channel in self.channels:
            self.join(channel)

    def joined(self, channel):
        """This will get called when the bot joins the channel."""
        log.msg("Joined %r" % (channel,))

    def privmsg(self, sender, recipient, message):
        """This will get called when the bot receives a message."""
        irc_msg = IrcMessage(sender, recipient, message)
        self.publish_message(irc_msg)

    def action(self, sender, recipient, message):
        """This will get called when the bot sees someone do an action."""
        irc_msg = IrcMessage(sender, recipient, message, action=True)
        self.publish_message(irc_msg)

    # irc callbacks

    def irc_NICK(self, prefix, params):
        """Called when an IRC user changes their nickname."""
        old_nick = prefix.partition('!')[0]
        new_nick = params[0]
        log.msg("Nick changed from %r to %r" % (old_nick, new_nick))

    # For fun, override the method that determines how a nickname is changed on
    # collisions. The default method appends an underscore.
    def alterCollidedNick(self, nickname):
        """
        Generate an altered version of a nickname that caused a collision in an
        effort to create an unused related name for subsequent registration.
        """
        return nickname + '^'


class VumiBotFactory(protocol.ReconnectingClientFactory):
    """A factory for :class:`VumiBotClient`s.

    A new protocol instance will be created each time we connect to
    the server.
    """

    # the class of the protocol to build when new connection is made
    protocol = VumiBotProtocol

    def __init__(self, nickname, channels, transport):
        self.nickname = nickname
        self.channels = channels
        self.transport = transport

    def buildProtocol(self, addr):
        self.resetDelay()
        return self.protocol(self.nickname, self.channels,
                             self.transport)


class IrcTransport(Transport):
    """IRC based transport.

    IRC transport options:

    :type network: str
    :param network:
        Host name of the IRC server to connect to.
    :type port: int
    :param port:
        Port of the IRC server to connect to. Default: 6667.
    :type channels: list
    :param channels:
        List of channels to join. Defaults: [].
    :type nickname: str
    :param nickname:
        IRC nickname for the transport IRC client to use. Default:
        vumibot.
    """

    def validate_config(self):
        self.network = self.config['network']
        self.port = int(self.config.get('port', 6667))
        self.channels = self.config.get('channels', [])
        self.nickname = self.config.get('nickname', 'vumibot')
        self.client = None

    def setup_transport(self):
        # TODO: clean-up factory constructor (arguments are a bit duplicated)
        factory = VumiBotFactory(self.nickname, self.channels,
                                 self)
        self.client = reactor.connectTCP(self.network, self.port, factory)

    def teardown_transport(self):
        if self.client is not None:
            self.client.disconnect()
            self.client.factory.stopTrying()

    def handle_inbound_irc_message(self, irc_msg):
        message_dict = {
            'to_addr': irc_msg.recipient,
            'from_addr': irc_msg.sender,
            'content': irc_msg.content,
            'transport_name': self.transport_name,
            'transport_type': self.config.get('transport_type', 'irc'),
            'transport_metadata': {
                'transport_nickname': self.client.something.nickname,
                'irc_server': "%s:%s" % (self.network, self.port),
                'irc_channel': irc_msg.channel(),
                },
            }
        self.publish_message(**message_dict)

    def handle_outbound_message(self, msg):
        irc_msg = IrcMessage(msg['to_addr'], msg['content'])
        self.client.something.consume_message(irc_msg)
