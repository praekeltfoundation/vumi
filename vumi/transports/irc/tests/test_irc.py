"""Tests for vumi.transports.irc.irc."""

from StringIO import StringIO

from twisted.internet.defer import (inlineCallbacks, returnValue,
                                    DeferredQueue, Deferred)
from twisted.internet.protocol import FileWrapper

from vumi.tests.utils import LogCatcher
from vumi.transports.failures import FailureMessage, TemporaryFailure
from vumi.transports.irc.irc import IrcMessage, VumiBotProtocol
from vumi.transports.irc import IrcTransport
from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.helpers import VumiTestCase


class TestIrcMessage(VumiTestCase):

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


class TestVumiBotProtocol(VumiTestCase):

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
            [logmsg] = logger.messages()
            self.assertEqual(logmsg,
                             'Disconnected (nickname was: %s).' % self.nick)
            self.assertEqual(logger.errors, [])

    def test_signed_on(self):
        self.vb.signedOn()
        self.check(['JOIN %s' % self.channel])

    def test_joined(self):
        with LogCatcher() as logger:
            self.vb.joined(self.channel)
            [logmsg] = logger.messages()
            self.assertEqual(logmsg, 'Joined %r' % self.channel)

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
            [logmsg] = logger.messages()
            self.assertEqual(logmsg,
                             "Nick changed from 'oldnick' to 'newnick'")

    def test_alter_collided_nick(self):
        collided_nick = "commonnick"
        new_nick = self.vb.alterCollidedNick(collided_nick)
        self.assertEqual(new_nick, collided_nick + '^')


from twisted.internet.protocol import ServerFactory
from twisted.internet import reactor
from twisted.words.protocols.irc import IRC


class StubbyIrcServerProtocol(IRC):
    hostname = '127.0.0.1'

    def irc_unknown(self, prefix, command, params):
        self.factory.events.put((prefix, command, params))

    def connectionLost(self, reason):
        IRC.connectionLost(self, reason)
        self.factory.finished_d.callback(None)


class StubbyIrcServer(ServerFactory):
    protocol = StubbyIrcServerProtocol

    def startFactory(self):
        self.server = None
        self.events = DeferredQueue()
        self.finished_d = Deferred()

    def buildProtocol(self, addr):
        self.server = ServerFactory.buildProtocol(self, addr)
        self.server.factory = self
        return self.server

    @inlineCallbacks
    def filter_events(self, command_type):
        while True:
            ev = yield self.events.get()
            if ev[1] == command_type:
                returnValue(ev)


class TestIrcTransport(VumiTestCase):

    nick = 'vumibottest'

    @inlineCallbacks
    def setUp(self):
        self.irc_server = StubbyIrcServer()
        self.add_cleanup(lambda: self.irc_server.finished_d)
        self.tx_helper = self.add_helper(TransportHelper(IrcTransport))
        self.irc_connector = yield reactor.listenTCP(
            0, self.irc_server, interface='127.0.0.1')
        self.add_cleanup(self.irc_connector.stopListening)
        addr = self.irc_connector.getHost()
        self.server_addr = "%s:%s" % (addr.host, addr.port)

        self.transport = yield self.tx_helper.get_transport({
            'network': addr.host,
            'port': addr.port,
            'channels': [],
            'nickname': self.nick,
        })
        # wait for transport to connect
        yield self.irc_server.filter_events("NICK")

    def dispatch_outbound_irc(self, *args, **kw):
        helper_metadata = kw.setdefault('helper_metadata', {'irc': {}})
        irc_command = kw.pop('irc_command', None)
        if irc_command is not None:
            helper_metadata['irc']['irc_command'] = irc_command
        return self.tx_helper.make_dispatch_outbound(*args, **kw)

    def assert_inbound_message(self, msg, to_addr, from_addr, channel, content,
                               addressed_to_transport, irc_command):
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['to_addr'], to_addr)
        self.assertEqual(msg['from_addr'], from_addr)
        self.assertEqual(msg['group'], channel)
        self.assertEqual(msg['content'], content)
        self.assertEqual(msg['helper_metadata'], {
            'irc': {
                'transport_nickname': self.nick,
                'addressed_to_transport': addressed_to_transport,
                'irc_server': self.server_addr,
                'irc_channel': channel,
                'irc_command': irc_command,
                }
            })
        self.assertEqual(msg['transport_metadata'], {
            'irc_channel': channel,
            })

    def assert_ack_for(self, msg, ack):
        to_payload = lambda m: dict(
            (k, v) for k, v in m.payload.iteritems()
            if k not in ('event_id', 'timestamp', 'transport_type'))
        self.assertEqual(to_payload(self.tx_helper.make_ack(msg)),
                         to_payload(ack))

    def send_irc_message(self, content, recipient, sender="user!ident@host"):
        self.irc_server.server.privmsg(sender, recipient, content)

    @inlineCallbacks
    def test_handle_inbound_to_channel(self):
        text = "Hello gooites"
        self.send_irc_message(text, "#zoo")
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assert_inbound_message(msg,
                                    to_addr=None,
                                    from_addr="user",
                                    channel="#zoo",
                                    content=text,
                                    addressed_to_transport=False,
                                    irc_command="PRIVMSG")

    @inlineCallbacks
    def test_handle_inbound_to_channel_directed(self):
        self.send_irc_message("%s: Hi" % (self.nick,), "#zoo")
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assert_inbound_message(msg,
                                    to_addr=self.nick,
                                    from_addr="user",
                                    channel="#zoo",
                                    content="Hi",
                                    addressed_to_transport=True,
                                    irc_command="PRIVMSG")

    @inlineCallbacks
    def test_handle_inbound_to_user(self):
        self.send_irc_message("Hi there", "%s!bot@host" % (self.nick,))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assert_inbound_message(msg,
                                    to_addr=self.nick,
                                    from_addr="user",
                                    channel=None,
                                    content="Hi there",
                                    addressed_to_transport=True,
                                    irc_command="PRIVMSG")

    @inlineCallbacks
    def test_handle_inbound_channel_notice(self):
        sender, recipient, text = "user!ident@host", "#zoo", "Hello gooites"
        self.irc_server.server.notice(sender, recipient, text)
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['to_addr'], None)
        self.assertEqual(msg['from_addr'], "user")
        self.assertEqual(msg['group'], "#zoo")
        self.assertEqual(msg['content'], text)
        self.assertEqual(msg['helper_metadata'], {
            'irc': {
                'transport_nickname': self.nick,
                'addressed_to_transport': False,
                'irc_server': self.server_addr,
                'irc_channel': '#zoo',
                'irc_command': 'NOTICE',
                }
            })
        self.assertEqual(msg['transport_metadata'], {
            'irc_channel': '#zoo',
            })

    @inlineCallbacks
    def test_handle_inbound_user_notice(self):
        sender, recipient, text = "user!ident@host", "bot", "Hello gooites"
        self.irc_server.server.notice(sender, recipient, text)
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['to_addr'], "bot")
        self.assertEqual(msg['from_addr'], "user")
        self.assertEqual(msg['group'], None)
        self.assertEqual(msg['content'], text)
        self.assertEqual(msg['helper_metadata'], {
            'irc': {
                'transport_nickname': self.nick,
                'addressed_to_transport': False,
                'irc_server': self.server_addr,
                'irc_channel': None,
                'irc_command': 'NOTICE',
                }
            })
        self.assertEqual(msg['transport_metadata'], {
            'irc_channel': None,
            })

    @inlineCallbacks
    def test_handle_outbound_message_while_disconnected(self):
        yield self.irc_connector.stopListening()
        self.transport.factory.vumibot.connectionLost("testing disconnect")

        expected_error = "IrcTransport not connected."

        yield self.dispatch_outbound_irc("outbound")
        [error] = self.tx_helper.get_dispatched_failures()
        self.assertTrue(error['reason'].strip().endswith(expected_error))

        [error] = self.flushLoggedErrors(TemporaryFailure)
        failure = error.value
        self.assertEqual(failure.failure_code, FailureMessage.FC_TEMPORARY)
        self.assertEqual(str(failure), expected_error)

    @inlineCallbacks
    def test_handle_outbound_to_channel_old(self):
        msg = yield self.dispatch_outbound_irc(
            "hello world", to_addr="#vumitest")

        event = yield self.irc_server.filter_events('PRIVMSG')
        self.assertEqual(event, ('', 'PRIVMSG',
                                 ['#vumitest', 'hello world']))

        [smsg] = self.tx_helper.get_dispatched_events()
        self.assert_ack_for(msg, smsg)

    @inlineCallbacks
    def test_handle_outbound_to_channel(self):
        msg = yield self.dispatch_outbound_irc(
            "hello world", to_addr=None, group="#vumitest")

        event = yield self.irc_server.filter_events('PRIVMSG')
        self.assertEqual(event, ('', 'PRIVMSG',
                                 ['#vumitest', 'hello world']))

        [smsg] = self.tx_helper.get_dispatched_events()
        self.assert_ack_for(msg, smsg)

    @inlineCallbacks
    def test_handle_outbound_to_channel_directed(self):
        msg = yield self.dispatch_outbound_irc(
            "hello world", to_addr="user", group="#vumitest")

        event = yield self.irc_server.filter_events('PRIVMSG')
        self.assertEqual(event, ('', 'PRIVMSG',
                                 ['#vumitest', 'user: hello world']))

        [smsg] = self.tx_helper.get_dispatched_events()
        self.assert_ack_for(msg, smsg)

    @inlineCallbacks
    def test_handle_outbound_to_user(self):
        msg = yield self.dispatch_outbound_irc(
            "hello world", to_addr="user", group=None)

        event = yield self.irc_server.filter_events('PRIVMSG')
        self.assertEqual(event, ('', 'PRIVMSG',
                                 ['user', 'hello world']))

        [smsg] = self.tx_helper.get_dispatched_events()
        self.assert_ack_for(msg, smsg)

    @inlineCallbacks
    def test_handle_outbound_action_to_channel(self):
        msg = yield self.dispatch_outbound_irc(
            "waves", to_addr=None, group="#vumitest", irc_command="ACTION")

        event = yield self.irc_server.filter_events('PRIVMSG')
        self.assertEqual(event, ('', 'PRIVMSG',
                                 ['#vumitest', '\x01ACTION waves\x01']))

        [smsg] = self.tx_helper.get_dispatched_events()
        self.assert_ack_for(msg, smsg)

    @inlineCallbacks
    def test_handle_outbound_action_to_channel_directed(self):
        msg = yield self.dispatch_outbound_irc(
            "waves", to_addr="user", group="#vumitest", irc_command='ACTION')

        event = yield self.irc_server.filter_events('PRIVMSG')
        self.assertEqual(event, ('', 'PRIVMSG',
                                 ['#vumitest', '\x01ACTION waves\x01']))

        [smsg] = self.tx_helper.get_dispatched_events()
        self.assert_ack_for(msg, smsg)

    @inlineCallbacks
    def test_handle_outbound_action_to_user(self):
        msg = yield self.dispatch_outbound_irc(
            "waves", to_addr="user", group=None, irc_command='ACTION')

        event = yield self.irc_server.filter_events('PRIVMSG')
        self.assertEqual(event, ('', 'PRIVMSG',
                                 ['user', '\x01ACTION waves\x01']))

        [smsg] = self.tx_helper.get_dispatched_events()
        self.assert_ack_for(msg, smsg)
