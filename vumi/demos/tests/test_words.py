# -*- coding: utf-8 -*-
"""Tests for vumi.demos.words."""

from twisted.internet.defer import inlineCallbacks

from vumi.demos.words import (SimpleAppWorker, EchoWorker, ReverseWorker,
                              WordCountWorker)
from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.application.tests.helpers import ApplicationHelper
from vumi.tests.utils import LogCatcher


class EchoTestApp(SimpleAppWorker):
    """Test worker that echos calls to process_message."""
    def process_message(self, data):
        return 'echo:%s' % data


class TestSimpleAppWorker(VumiTestCase):
    @inlineCallbacks
    def setUp(self):
        self.app_helper = self.add_helper(ApplicationHelper(None))
        self.worker = yield self.app_helper.get_application({}, EchoTestApp)

    @inlineCallbacks
    def test_help(self):
        yield self.app_helper.make_dispatch_inbound(
            None, session_event=TransportUserMessage.SESSION_NEW)
        [reply] = self.app_helper.get_dispatched_outbound()
        self.assertEqual(reply['session_event'], None)
        self.assertEqual(reply['content'], 'Enter text:')

    @inlineCallbacks
    def test_content_text(self):
        yield self.app_helper.make_dispatch_inbound(
            "test", session_event=TransportUserMessage.SESSION_NEW)
        [reply] = self.app_helper.get_dispatched_outbound()
        self.assertEqual(reply['session_event'], None)
        self.assertEqual(reply['content'], 'echo:test')

    @inlineCallbacks
    def test_base_process_message(self):
        worker = yield self.app_helper.get_application({}, SimpleAppWorker)
        self.assertRaises(NotImplementedError, worker.process_message, 'foo')


class TestEchoWorker(VumiTestCase):
    @inlineCallbacks
    def setUp(self):
        self.app_helper = self.add_helper(ApplicationHelper(None))
        self.worker = yield self.app_helper.get_application({}, EchoWorker)

    def test_process_message(self):
        self.assertEqual(self.worker.process_message("foo"), "foo")

    def test_help(self):
        self.assertEqual(self.worker.get_help(), "Enter text to echo:")

    @inlineCallbacks
    def test_echo_non_ascii(self):
        content = u'Zoë destroyer of Ascii'
        with LogCatcher() as log:
            yield self.app_helper.make_dispatch_inbound(content)
            [reply] = self.app_helper.get_dispatched_outbound()
            self.assertEqual(
                log.messages(),
                ['User message: Zo\xc3\xab destroyer of Ascii'])


class TestReverseWorker(VumiTestCase):
    @inlineCallbacks
    def setUp(self):
        self.app_helper = self.add_helper(ApplicationHelper(None))
        self.worker = yield self.app_helper.get_application({}, ReverseWorker)

    def test_process_message(self):
        self.assertEqual(self.worker.process_message("foo"), "oof")

    def test_help(self):
        self.assertEqual(self.worker.get_help(), "Enter text to reverse:")


class TestWordCountWorker(VumiTestCase):
    @inlineCallbacks
    def setUp(self):
        self.app_helper = self.add_helper(ApplicationHelper(None))
        self.worker = yield self.app_helper.get_application(
            {}, WordCountWorker)

    def test_process_message(self):
        self.assertEqual(self.worker.process_message("foo bar"),
                         "2 words, 7 chars")

    def test_singular(self):
        self.assertEqual(self.worker.process_message("f"),
                         "1 word, 1 char")

    def test_help(self):
        self.assertEqual(self.worker.get_help(), "Enter text to return word"
                         " and character counts for:")
