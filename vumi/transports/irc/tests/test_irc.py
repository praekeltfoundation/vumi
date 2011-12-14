"""Tests for vumi.transports.irc.irc."""

from StringIO import StringIO

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import FileWrapper

from vumi.tests.utils import get_stubbed_worker, LogCatcher
from vumi.transports.irc.irc import IrcMessage, VumiBotProtocol
from vumi.transports.irc import IrcTransport


class TestIrcMessage(unittest.TestCase):

    def test_message(self):
        msg = IrcMessage('user!userfoo@example.com', '#bar', 'hello?')
        self.assertEqual(msg.sender, 'user!userfoo@example.com')
        self.assertEqual(msg.recipient, '#bar')
        self.assertEqual(msg.content, 'hello?')
        self.assertFalse(msg.action)

    def test_action(self):
        msg = IrcMessage('user!userfoo@example.com', '#bar', 'hello?',
                         action=True)
        self.assertTrue(msg.action)

    def test_channel(self):
        msg = IrcMessage('user!userfoo@example.com', '#bar', 'hello?')
        self.assertEqual(msg.channel(), '#bar')
        msg = IrcMessage('user!userfoo@example.com', 'user2!user2@example.com',
                         'hello?')
        self.assertEqual(msg.channel(), None)


class TestVumiBotProtocol(unittest.TestCase):

    nick = "testnick"
    channel = "#test1"

    def setUp(self):
        self.f = StringIO()
        self.t = FileWrapper(self.f)
        self.vb = VumiBotProtocol(self.nick, [self.channel], self)
        self.vb.makeConnection(self.t)
        self.recvd_messages = []

    def handle_inbound_irc_message(self, irc_msg):
        self.recvd_messages.append(irc_msg)

    def check(self, lines):
        connect_lines = [
            "NICK %s" % self.nick,
            "USER %s foo bar :None" % self.nick,  # TODO: remove foo and bar
            ]
        expected_lines = connect_lines + lines
        self.assertEqual(self.f.getvalue().splitlines(), expected_lines)

    def test_publish_message(self):
        msg = IrcMessage('user!userfoo@example.com', '#bar', 'hello?')
        self.vb.publish_message(msg)
        self.check([])
        [recvd_msg] = self.recvd_messages
        self.assertEqual(recvd_msg, msg)

    def test_consume_message(self):
        self.vb.consume_message(IrcMessage('user!userfoo@example.com', '#bar',
                                           'hello?'))
        self.check(["PRIVMSG #bar :hello?"])

    def test_connection_made(self):
        # just check that the connect messages made it through
        self.check([])

    def test_connection_lost(self):
        with LogCatcher() as logger:
            self.vb.connectionLost("test loss of connection")
            [log] = logger.logs
            self.assertEqual(log['message'][0],
                             'Disconnected (nickname was: %s).' % self.nick)
            self.assertEqual(logger.errors, [])

    def test_signed_on(self):
        self.vb.signedOn()
        self.check(['JOIN %s' % self.channel])

    def test_joined(self):
        with LogCatcher() as logger:
            self.vb.joined(self.channel)
            [log] = logger.logs
            self.assertEqual(log['message'][0], 'Joined %r' % self.channel)

    def test_privmsg(self):
        sender, recipient, text = (self.nick, "#zoo", "Hello zooites")
        self.vb.privmsg(sender, recipient, text)
        [recvd_msg] = self.recvd_messages
        self.assertEqual(recvd_msg, IrcMessage(sender, recipient,
                                               text, action=False))

    def test_action(self):
        sender, recipient, text = (self.nick, "#zoo", "waves at zooites")
        self.vb.action(sender, recipient, text)
        [recvd_msg] = self.recvd_messages
        self.assertEqual(recvd_msg, IrcMessage(sender, recipient,
                                               text, action=True))

    def test_irc_nick(self):
        with LogCatcher() as logger:
            self.vb.irc_NICK("oldnick!host", ["newnick"])
            [log] = logger.logs
            self.assertEqual(log['message'][0],
                             "Nick changed from 'oldnick' to 'newnick'")

    def test_alter_collided_nick(self):
        collided_nick = "commonnick"
        new_nick = self.vb.alterCollidedNick(collided_nick)
        self.assertEqual(new_nick, collided_nick + '^')


class TestIrcTransport(unittest.TestCase):

    @inlineCallbacks
    def setUp(self):
        config = {
            'transport_name': 'test_irc',
            'network': '127.0.0.1',
            'channels': [],
            'nickname': 'vumibottext',
            }
        self.worker = get_stubbed_worker(IrcTransport, config)
        self.broker = self.worker._amqp_client.broker
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()

    def test_stub(self):
        pass
