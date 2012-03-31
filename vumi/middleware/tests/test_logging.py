"""Tests from vumi.middleware.logging."""

from twisted.trial.unittest import TestCase

from vumi.middleware.logging import LoggingMiddleware
from vumi.tests.utils import LogCatcher


class DummyMessage(object):
    def __init__(self, json):
        self._json = json

    def to_json(self):
        return self._json


class LoggingMiddlewareTestCase(TestCase):

    def mklogger(self, config):
        worker = object()
        mw = LoggingMiddleware("test_logger", config, worker)
        mw.setup_middleware()
        return mw

    def test_default_config(self):
        mw = self.mklogger({})
        with LogCatcher() as lc:
            mw.handle_inbound(DummyMessage("inbound"), "endpoint")
            mw.handle_outbound(DummyMessage("outbound"), "endpoint")
            mw.handle_event(DummyMessage("event"), "endpoint")
            mw.handle_failure(DummyMessage("failure"), "endpoint")
            logs = lc.logs
        self.assertEqual([log['logLevel'] for log in logs],
                         [20, 20, 20, 40])
        self.assertEqual([log['message'][0] for log in logs], [
            "Processed inbound message for endpoint: inbound",
            "Processed outbound message for endpoint: outbound",
            "Processed event message for endpoint: event",
            "'Processed failure message for endpoint: failure'",
            ])

    def test_custom_log_level(self):
        mw = self.mklogger({'log_level': 'warning'})
        with LogCatcher() as lc:
            mw.handle_inbound(DummyMessage("inbound"), "endpoint")
            logs = lc.logs
        self.assertEqual([log['logLevel'] for log in logs], [30])
        self.assertEqual([log['message'][0] for log in logs], [
            "Processed inbound message for endpoint: inbound",
            ])

    def test_custom_failure_log_level(self):
        mw = self.mklogger({'failure_log_level': 'info'})
        with LogCatcher() as lc:
            mw.handle_failure(DummyMessage("failure"), "endpoint")
            logs = lc.logs
        self.assertEqual([log['logLevel'] for log in logs], [20])
        self.assertEqual([log['message'][0] for log in logs], [
            "Processed failure message for endpoint: failure",
            ])
