"""Tests for vumi.transports.irc.irc."""

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.tests.utils import get_stubbed_worker
from vumi.transports.irc.irc import IrcMessage, VumiBotClient
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


class TestVumiBotClient(unittest.TestCase):
    def setUp(self):
        client = VumiBotClient
        print client


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
