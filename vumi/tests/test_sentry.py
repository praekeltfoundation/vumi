"""Tests for vumi.sentry."""

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks
from twisted.web import http

from vumi.tests.utils import MockHttpServer, LogCatcher
from vumi.sentry import quiet_get_page


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


class TestSentryLogObserver(TestCase):
    pass


class TestRavenUtilityFunctions(TestCase):
    pass
