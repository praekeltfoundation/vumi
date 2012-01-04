"""IRC transport."""

from twisted.words.protocols import irc
from twisted.internet import protocol, reactor
from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.transports import Transport
from vumi.transports.failures import TemporaryFailure


class IrcMessage(object):
    """Container for details of a message to or from an IRC user.

    :type sender: str
    :param sender:
        Who sent the message (usually user!ident@hostmask).
    :type recipient: str
    :param recipient:
        User or channel recieving the message.
    :type content: str
    :param content:
        Contents of message.
    :type nickname: str
    :param nickname:
        Nickname used by the client that received the message.
        Optional.
    :type command: str
    :param command:
        IRC command that produced the message.
    """

    def __init__(self, sender, command, recipient, content, nickname=None):
        self.sender = self.canonicalize_recipient(sender)
        self.command = command
        self.recipient = self.canonicalize_recipient(recipient)
        self.content = content
        self.nickname = nickname

    def __eq__(self, other):
        if isinstance(other, IrcMessage):
            return all(getattr(self, name) == getattr(other, name)
                       for name in ("sender", "command", "recipient",
                                    "content", "nickname"))
        return False

    @staticmethod
    def canonicalize_recipient(recipient):
        """Convert a generic IRC address (with possible server parts)
        to a simple lowercase username or channel."""
        return recipient.partition('!')[0].lower()

    def channel(self):
        """Return the channel if the recipient is a channel.

        Otherwise return None.
        """
        if self.recipient[:1] in ('#', '&', '$'):
            return self.recipient
        return None

    def addressed_to(self, nickname):
        nickname = self.canonicalize_recipient(nickname)
        if not self.channel():
            return self.recipient == nickname
        parts = self.content.split(None, 1)
        maybe_nickname = parts[0].rstrip(':,') if parts else ''
        maybe_nickname = self.canonicalize_recipient(maybe_nickname)
        return maybe_nickname == nickname


class VumiBotProtocol(irc.IRCClient):
    """An IRC bot that bridges IRC to Vumi."""

    def __init__(self, nickname, channels, irc_transport):
        self.nickname = nickname
        self.channels = channels
        self.irc_transport = irc_transport

    def publish_message(self, irc_msg):
        self.irc_transport.handle_inbound_irc_message(irc_msg)

    def consume_message(self, irc_msg):
        recipient = irc_msg.recipient.encode('utf8')
        content = irc_msg.content.encode('utf8')
        if irc_msg.command == 'ACTION':
            self.describe(recipient, content)
        else:
            self.msg(recipient, content)

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
        irc_msg = IrcMessage(sender, 'PRIVMSG', recipient, message,
                             self.nickname)
        self.publish_message(irc_msg)

    def action(self, sender, recipient, message):
        """This will get called when the bot sees someone do an action."""
        irc_msg = IrcMessage(sender, 'ACTION', recipient, message,
                             self.nickname)
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

    def __init__(self, vumibot_args):
        self.vumibot_args = vumibot_args
        self.vumibot = None

    def buildProtocol(self, addr):
        self.resetDelay()
        self.vumibot = self.protocol(*self.vumibot_args)
        return self.vumibot


class IrcTransport(Transport):
    """IRC based transport.

    IRC transport options:

    :type network: str
    :param network:
        Host name of the IRC server to connect to.
    :type nickname: str
    :param nickname:
        IRC nickname for the transport IRC client to use.
    :type port: int
    :param port:
        Port of the IRC server to connect to. Default: 6667.
    :type channels: list
    :param channels:
        List of channels to join. Defaults: [].
    """

    def validate_config(self):
        self.network = self.config['network']
        self.nickname = self.config['nickname']
        self.port = int(self.config.get('port', 6667))
        self.channels = self.config.get('channels', [])
        self.client = None

    def setup_transport(self):
        factory = VumiBotFactory((self.nickname, self.channels,
                                 self))
        self.client = reactor.connectTCP(self.network, self.port, factory)

    def teardown_transport(self):
        if self.client is not None:
            self.client.factory.stopTrying()
            self.client.disconnect()

    def handle_inbound_irc_message(self, irc_msg):
        irc_server = "%s:%s" % (self.network, self.port)
        irc_channel = irc_msg.channel()
        message_dict = {
            'to_addr': irc_msg.recipient,
            'from_addr': irc_msg.sender,
            'content': irc_msg.content,
            'transport_name': self.transport_name,
            'transport_type': self.config.get('transport_type', 'irc'),
            'helper_metadata': {
                'irc': {
                    'transport_nickname': irc_msg.nickname,
                    'addressed_to_transport':
                        irc_msg.addressed_to(irc_msg.nickname),
                    'irc_server': irc_server,
                    'irc_channel': irc_channel,
                    'irc_command': irc_msg.command,
                    },
                },
            'transport_metadata': {
                'irc_channel': irc_channel,
                },
            }
        self.publish_message(**message_dict)

    @inlineCallbacks
    def handle_outbound_message(self, msg):
        vumibot = self.client.factory.vumibot
        if vumibot is None or self.client.state != 'connected':
            raise TemporaryFailure("IrcTransport not connected (state: %r)."
                                   % (self.client.state,))
        irc_metadata = msg['helper_metadata'].get('irc', {})
        transport_metadata = msg['transport_metadata']
        irc_command = irc_metadata.get('irc_command', 'PRIVMSG')
        irc_channel = irc_metadata.get('irc_channel',
                                       transport_metadata.get('irc_channel'))
        recipient = msg['to_addr'] if irc_channel is None else irc_channel
        irc_msg = IrcMessage(vumibot.nickname, irc_command, recipient,
                             msg['content'])
        vumibot.consume_message(irc_msg)
        # intentionally duplicate message id in sent_message_id since
        # IRC doesn't have its own message ids.
        yield self.publish_ack(user_message_id=msg['message_id'],
                               sent_message_id=msg['message_id'])
