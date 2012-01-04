"""Tests for vumi.transports.irc.irc."""

from StringIO import StringIO

from twisted.trial import unittest
from twisted.internet.defer import (inlineCallbacks, returnValue,
                                    DeferredQueue)
from twisted.internet.protocol import FileWrapper

from vumi.tests.utils import LogCatcher
from vumi.transports.irc.irc import IrcMessage, VumiBotProtocol
from vumi.transports.irc import IrcTransport
from vumi.transports.tests.test_base import TransportTestCase


class TestIrcMessage(unittest.TestCase):

    def test_message(self):
        msg = IrcMessage('user!userfoo@example.com', 'PRIVMSG', '#bar',
                         'hello?')
        self.assertEqual(msg.sender, 'user')
        self.assertEqual(msg.command, 'PRIVMSG')
        self.assertEqual(msg.recipient, '#bar')
        self.assertEqual(msg.content, 'hello?')

    def test_action(self):
        msg = IrcMessage('user!userfoo@example.com', 'ACTION', '#bar',
                         'hello?')
        self.assertEqual(msg.command, 'ACTION')

    def test_channel(self):
        msg = IrcMessage('user!userfoo@example.com', 'PRIVMSG', '#bar',
                         'hello?')
        self.assertEqual(msg.channel(), '#bar')
        msg = IrcMessage('user!userfoo@example.com', 'PRIVMSG',
                         'user2!user2@example.com', 'hello?')
        self.assertEqual(msg.channel(), None)

    def test_nick(self):
        msg = IrcMessage('user!userfoo@example.com', 'PRIVMSG', '#bar',
                         'hello?', 'nicktest')
        self.assertEqual(msg.nickname, 'nicktest')

    def test_addressed_to(self):
        msg = IrcMessage('user!userfoo@example.com', 'PRIVMSG',
                         'otheruser!userfoo@example.com',
                         'hello?', 'nicktest')
        self.assertFalse(msg.addressed_to('user'))
        self.assertTrue(msg.addressed_to('otheruser'))

    def test_equality(self):
        msg1 = IrcMessage('user!userfoo@example.com', 'PRIVMSG', '#bar',
                         'hello?')
        msg2 = IrcMessage('user!userfoo@example.com', 'PRIVMSG', '#bar',
                         'hello?')
        self.assertTrue(msg1 == msg2)

    def test_inequality(self):
        msg1 = IrcMessage('user!userfoo@example.com', 'PRIVMSG', '#bar',
                         'hello?')
        self.assertFalse(msg1 == object())

    def test_canonicalize_recipient(self):
        canonical = IrcMessage.canonicalize_recipient
        self.assertEqual(canonical("user!userfoo@example.com"), "user")
        self.assertEqual(canonical("#channel"), "#channel")
        self.assertEqual(canonical("userfoo"), "userfoo")


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
            # foo and bar are twisted's mis-implementation of RFC 2812
            # Compare http://tools.ietf.org/html/rfc2812#section-3.1.3
            # and http://twistedmatrix.com/trac/browser/tags/releases/
            #     twisted-11.0.0/twisted/words/protocols/irc.py#L1552
            "USER %s foo bar :None" % self.nick,
            ]
        expected_lines = connect_lines + lines
        self.assertEqual(self.f.getvalue().splitlines(), expected_lines)

    def test_publish_message(self):
        msg = IrcMessage('user!userfoo@example.com', 'PRIVMSG', '#bar',
                         'hello?')
        self.vb.publish_message(msg)
        self.check([])
        [recvd_msg] = self.recvd_messages
        self.assertEqual(recvd_msg, msg)

    def test_consume_message_privmsg(self):
        self.vb.consume_message(IrcMessage('user!userfoo@example.com',
                                           'PRIVMSG', '#bar', 'hello?'))
        self.check(["PRIVMSG #bar :hello?"])

    def test_consume_message_action(self):
        self.vb.consume_message(IrcMessage('user!userfoo@example.com',
                                           'ACTION', '#bar', 'hello?'))
        self.check(["PRIVMSG #bar :\x01ACTION hello?\x01"])

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
        sender, command, recipient, text = (self.nick, 'PRIVMSG', "#zoo",
                                            "Hello zooites")
        self.vb.privmsg(sender, recipient, text)
        [recvd_msg] = self.recvd_messages
        self.assertEqual(recvd_msg,
                         IrcMessage(sender, command, recipient, text,
                                    self.vb.nickname))

    def test_action(self):
        sender, command, recipient, text = (self.nick, 'ACTION', "#zoo",
                                            "waves at zooites")
        self.vb.action(sender, recipient, text)
        [recvd_msg] = self.recvd_messages
        self.assertEqual(recvd_msg,
                         IrcMessage(sender, command, recipient, text,
                                    self.vb.nickname))

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


from twisted.internet.protocol import ServerFactory
from twisted.internet import reactor
from twisted.words.protocols.irc import IRC


class StubbyIrcProtocol(IRC):

    def __init__(self, events):
        self.events = events

    def irc_unknown(self, prefix, command, params):
        self.events.put((prefix, command, params))


class StubbyIrcServer(ServerFactory):

    protocol = StubbyIrcProtocol

    def __init__(self, *args, **kw):
        # ServerFactory.__init__(self, *args, **kw)
        self.client = None
        self.events = DeferredQueue()

    def buildProtocol(self, addr):
        self.client = self.protocol(self.events)
        return self.client

    @inlineCallbacks
    def filter_events(self, command_type):
        while True:
            ev = yield self.events.get()
            if ev[1] == command_type:
                returnValue(ev)


class TestIrcTransport(TransportTestCase):

    timeout = 5

    transport_name = 'test_irc_transport'
    transport_class = IrcTransport
    nick = 'vumibottest'

    @inlineCallbacks
    def setUp(self):
        super(TestIrcTransport, self).setUp()

        self.irc_server = StubbyIrcServer()
        self.irc_connector = yield reactor.listenTCP(0, self.irc_server)
        addr = self.irc_connector.getHost()
        self.server_addr = "%s:%s" % (addr.host, addr.port)

        self.config = {
            'transport_name': self.transport_name,
            'network': addr.host,
            'port': addr.port,
            'channels': [],
            'nickname': self.nick,
            }
        self.transport = yield self.get_transport(self.config, start=True)
        # wait for transport to connect
        yield self.irc_server.filter_events("NICK")

    @inlineCallbacks
    def tearDown(self):
        yield self.irc_connector.stopListening()
        super(TestIrcTransport, self).tearDown()

    @inlineCallbacks
    def test_handle_inbound(self):
        sender, recipient, text = "user!ident@host", "#zoo", "Hello gooites"
        self.irc_server.client.privmsg(sender, recipient, text)
        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['to_addr'], "#zoo")
        self.assertEqual(msg['from_addr'], "user")
        self.assertEqual(msg['content'], text)
        self.assertEqual(msg['helper_metadata'], {
            'irc': {
                'transport_nickname': self.nick,
                'addressed_to_transport': False,
                'irc_server': self.server_addr,
                'irc_channel': '#zoo',
                'irc_command': 'PRIVMSG',
                }
            })
        self.assertEqual(msg['transport_metadata'], {
            'irc_channel': '#zoo',
            })

    @inlineCallbacks
    def test_handle_outbound_message_while_disconnected(self):
        yield self.irc_connector.stopListening()
        self.transport.client.disconnect()

        msg = self.mkmsg_out()
        yield self.dispatch(msg)
        [error] = self.get_dispatched_failures()
        self.assertTrue(error['reason'].strip().endswith(
            "TemporaryFailure: IrcTransport not connected"
            " (state: 'disconnected')."))

    @inlineCallbacks
    def test_handle_outbound_message(self):
        msg = self.mkmsg_out(to_addr="#vumitest", content='hello world')
        yield self.dispatch(msg)

        event = yield self.irc_server.filter_events('PRIVMSG')
        self.assertEqual(event, ('', 'PRIVMSG',
                                 ['#vumitest', 'hello world']))

        [smsg] = self.get_dispatched_events()
        self.assertEqual(self.mkmsg_ack(sent_message_id=msg['message_id']),
                         smsg)
