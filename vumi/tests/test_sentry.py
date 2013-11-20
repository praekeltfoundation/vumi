"""Tests for vumi.sentry."""

import logging
import base64
import json
import sys
import traceback

from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.web import http
from twisted.python.failure import Failure
from twisted.python.log import LogPublisher

from vumi.tests.utils import MockHttpServer, LogCatcher
from vumi.sentry import (quiet_get_page, SentryLogObserver, vumi_raven_client,
                         SentryLoggerService)
from vumi.tests.helpers import VumiTestCase, import_skip


class TestQuietGetPage(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.mock_http = MockHttpServer(self._handle_request)
        self.add_cleanup(self.mock_http.stop)
        yield self.mock_http.start()

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
        self.teardowns = 0

    def captureMessage(self, *args, **kwargs):
        self.messages.append((args, kwargs))

    def captureException(self, *args, **kwargs):
        self.exceptions.append((args, kwargs))

    def teardown(self):
        self.teardowns += 1


class TestSentryLogObserver(VumiTestCase):
    def setUp(self):
        self.client = DummySentryClient()
        self.obs = SentryLogObserver(self.client, 'test', "worker-1")

    def test_level_for_event(self):
        for expected_level, event in [
            (logging.WARN, {'logLevel': logging.WARN}),
            (logging.ERROR, {'isError': 1}),
            (logging.INFO, {}),
        ]:
            self.assertEqual(self.obs.level_for_event(event), expected_level)

    def test_logger_for_event(self):
        self.assertEqual(self.obs.logger_for_event({'system': 'foo,bar'}),
                         'test.foo.bar')
        self.assertEqual(self.obs.logger_for_event({}), 'test')

    def test_log_failure(self):
        e = ValueError("foo error")
        f = Failure(e)
        self.obs({'failure': f, 'system': 'foo', 'isError': 1})
        self.assertEqual(self.client.exceptions, [
            (((type(e), e, None),),
             {'data': {'level': 40, 'logger': 'test.foo'},
              'tags': {'worker-id': 'worker-1'}}),
        ])

    def test_log_traceback(self):
        try:
            raise ValueError("foo")
        except ValueError:
            f = Failure(*sys.exc_info())
        self.obs({'failure': f, 'isError': 1})
        [call_args] = self.client.exceptions
        exc_info = call_args[0][0]
        tb = ''.join(traceback.format_exception(*exc_info))
        self.assertTrue('raise ValueError("foo")' in tb)

    def test_log_warning(self):
        self.obs({'message': ["a"], 'system': 'foo',
                  'logLevel': logging.WARN})
        self.assertEqual(self.client.messages, [
            (('a',),
             {'data': {'level': 30, 'logger': 'test.foo'},
              'tags': {'worker-id': 'worker-1'}})
        ])

    def test_log_info(self):
        self.obs({'message': ["a"], 'system': 'test.log'})
        self.assertEqual(self.client.messages, [])  # should be filtered out


class TestSentryLoggerSerivce(VumiTestCase):

    def setUp(self):
        import vumi.sentry
        self.client = DummySentryClient()
        self.patch(vumi.sentry, 'vumi_raven_client', lambda dsn: self.client)
        self.logger = LogPublisher()
        self.service = SentryLoggerService("http://example.com/",
                                           "test.logger",
                                           "worker-1",
                                           logger=self.logger)

    @inlineCallbacks
    def test_logging(self):
        yield self.service.startService()
        self.logger.msg("Hello", logLevel=logging.WARN)
        self.assertEqual(self.client.messages, [
            (("Hello",),
             {'data': {'level': 30, 'logger': 'test.logger'},
              'tags': {'worker-id': 'worker-1'}})
        ])
        del self.client.messages[:]
        yield self.service.stopService()
        self.logger.msg("Foo", logLevel=logging.WARN)
        self.assertEqual(self.client.messages, [])

    @inlineCallbacks
    def test_stop_not_running(self):
        yield self.service.stopService()
        self.assertFalse(self.service.running)

    @inlineCallbacks
    def test_start_stop(self):
        self.assertFalse(self.service.registered())
        self.assertEqual(self.client.teardowns, 0)
        yield self.service.startService()
        self.assertTrue(self.service.registered())
        yield self.service.stopService()
        self.assertFalse(self.service.registered())
        self.assertEqual(self.client.teardowns, 1)


class TestRavenUtilityFunctions(VumiTestCase):

    def setUp(self):
        try:
            import raven
            raven  # To keep pyflakes happy.
        except ImportError, e:
            import_skip(e, 'raven')

    def mk_sentry_dsn(self):
        proj_user = "4c96ae4ca518483192dd9917c03847c4"
        proj_key = "05d9515b5c504cc7bf180597fd6f67"
        proj_no = 2
        host, port = "example.com", "30000"
        dsn = "http://%s:%s@%s:%s/%s" % (proj_user, proj_key, host, port,
                                         proj_no)
        return dsn

    def parse_call(self, sentry_call):
        args, kwargs = sentry_call
        postdata = kwargs['postdata']
        return json.loads(base64.b64decode(postdata).decode('zlib'))

    def test_vumi_raven_client_capture_message(self):
        import vumi.sentry
        dsn = self.mk_sentry_dsn()
        call_history = []

        def fake_get_page(*args, **kw):
            call_history.append((args, kw))
            return Deferred()

        self.patch(vumi.sentry, 'quiet_get_page', fake_get_page)
        client = vumi_raven_client(dsn)
        client.captureMessage("my message")
        [sentry_call] = call_history
        sentry_data = self.parse_call(sentry_call)
        self.assertEqual(sentry_data['message'], "my message")
