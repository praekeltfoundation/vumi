"""Tests for vumi.demos.words."""

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.web.static import Data

from vumi.tests.utils import get_stubbed_worker
from vumi.demos.words import (SimpleAppWorker, EchoWorker, ReverseWorker,
                              WordCountWorker)
from vumi.message import TransportUserMessage


class EchoTestApp(SimpleAppWorker):
    """Test worker that echos calls to process_message."""
    def process_message(self, data):
        return 'echo:%s' % data


class TestSimpleAppWorker(unittest.TestCase):
    @inlineCallbacks
    def setUp(self):
        self.transport_name = 'test_transport'
        self.worker = get_stubbed_worker(EchoTestApp, {
                'transport_name': self.transport_name})
        self.broker = self.worker._amqp_client.broker
        yield self.worker.startWorker()

    @inlineCallbacks
    def send(self, content, session_event=None):
        msg = TransportUserMessage(content=content,
                                   session_event=session_event,
                                   from_addr='+1234', to_addr='+134567',
                                   transport_name='test',
                                   transport_type='fake',
                                   transport_metadata={})
        self.broker.publish_message('vumi', '%s.inbound' % self.transport_name,
                                    msg)
        yield self.broker.kick_delivery()

    @inlineCallbacks
    def recv(self, n=0):
        msgs = yield self.broker.wait_messages('vumi', '%s.outbound'
                                                % self.transport_name, n)

        def reply_code(msg):
            if msg['session_event'] == TransportUserMessage.SESSION_CLOSE:
                return 'end'
            return 'reply'

        returnValue([(reply_code(msg), msg['content']) for msg in msgs])

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()

    @inlineCallbacks
    def test_help(self):
        yield self.send(None, TransportUserMessage.SESSION_NEW)
        reply, = yield self.recv(1)
        self.assertEqual(reply[0], 'reply')
        self.assertEqual(reply[1], 'Enter text:')

    @inlineCallbacks
    def test_content_text(self):
        yield self.send("test", TransportUserMessage.SESSION_NEW)
        reply, = yield self.recv(1)
        self.assertEqual(reply[0], 'reply')
        self.assertEqual(reply[1], 'echo:test')

    def test_base_process_message(self):
        worker = get_stubbed_worker(SimpleAppWorker, {
                'transport_name': self.transport_name})
        self.assertRaises(NotImplementedError, worker.process_message, 'foo')


class TestEchoWorker(unittest.TestCase):

    def setUp(self):
        self.worker = get_stubbed_worker(EchoWorker, {
            'transport_name': 'test_echoworker'})

    def test_process_message(self):
        self.assertEqual(self.worker.process_message("foo"), "foo")

    def test_help(self):
        self.assertEqual(self.worker.get_help(), "Enter text to echo:")


class TestReverseWorker(unittest.TestCase):

    def setUp(self):
        self.worker = get_stubbed_worker(ReverseWorker, {
            'transport_name': 'test_reverseworker'})

    def test_process_message(self):
        self.assertEqual(self.worker.process_message("foo"), "oof")

    def test_help(self):
        self.assertEqual(self.worker.get_help(), "Enter text to reverse:")


class TestWordCountWorker(unittest.TestCase):

    def setUp(self):
        self.worker = get_stubbed_worker(WordCountWorker, {
            'transport_name': 'test_wordcountworker'})

    def test_process_message(self):
        self.assertEqual(self.worker.process_message("foo bar"),
                         "2 words, 7 chars")

    def test_singular(self):
        self.assertEqual(self.worker.process_message("f"),
                         "1 word, 1 char")

    def test_help(self):
        self.assertEqual(self.worker.get_help(), "Enter text to return word"
                         " and character counts for:")
