"""Tests for vumi.sentry."""

import logging

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.python.failure import Failure

from vumi.tests.utils import MockHttpServer, LogCatcher
from vumi.sentry import quiet_get_page, SentryLogObserver


class TestQuietGetPage(TestCase):

    @inlineCallbacks
    def setUp(self):
        self.mock_http = MockHttpServer(self._handle_request)
        yield self.mock_http.start()

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_http.stop()

    def _handle_request(self, request):
        request.setResponseCode(http.OK)
        request.do_not_log = True
        return "Hello"

    @inlineCallbacks
    def test_request(self):
        with LogCatcher() as lc:
            result = yield quiet_get_page(self.mock_http.url)
            self.assertEqual(lc.logs, [])
        self.assertEqual(result, "Hello")


class DummySentryClient(object):
    def __init__(self):
        self.exceptions = []
        self.messages = []

    def captureMessage(self, *args, **kwargs):
        self.messages.append((args, kwargs))

    def captureException(self, *args, **kwargs):
        self.exceptions.append((args, kwargs))


class TestSentryLogObserver(TestCase):
    def setUp(self):
        self.client = DummySentryClient()
        self.obs = SentryLogObserver(self.client)

    def test_level_for_event(self):
        for expected_level, event in [
            (logging.WARN, {'logLevel': logging.WARN}),
            (logging.ERROR, {'isError': 1}),
            (logging.INFO, {}),
        ]:
            self.assertEqual(self.obs.level_for_event(event), expected_level)

    def test_logger_for_event(self):
        self.assertEqual(self.obs.logger_for_event({'system': 'foo,bar'}),
                         'foo,bar')
        self.assertEqual(self.obs.logger_for_event({}), 'unknown')

    def test_log_failure(self):
        e = ValueError("foo error")
        f = Failure(e)
        self.obs({'failure': f, 'system': 'test.log'})
        self.assertEqual(self.client.exceptions, [
            (((type(e), e, None),),
             {'data': {'level': 20, 'logger': 'test.log'}}),
        ])

    def test_log_message(self):
        self.obs({'message': ["a"], 'system': 'test.log'})
        self.assertEqual(self.client.messages, [
            (('a',),
             {'data': {'level': 20, 'logger': 'test.log'}})
        ])


class TestRavenUtilityFunctions(TestCase):
    pass
