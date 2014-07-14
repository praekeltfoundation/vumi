"""Tests from vumi.middleware.logging."""

from vumi.middleware.logging import LoggingMiddleware
from vumi.tests.utils import LogCatcher
from vumi.tests.helpers import VumiTestCase


class DummyMessage(object):
    def __init__(self, json):
        self._json = json

    def to_json(self):
        return self._json


class TestLoggingMiddleware(VumiTestCase):

    def mklogger(self, config):
        worker = object()
        mw = LoggingMiddleware("test_logger", config, worker)
        mw.setup_middleware()
        return mw

    def test_default_config(self):
        mw = self.mklogger({})
        with LogCatcher() as lc:
            for handler, rkey in [
                    (mw.handle_inbound, "inbound"),
                    (mw.handle_outbound, "outbound"),
                    (mw.handle_event, "event"),
                    (mw.handle_failure, "failure")]:
                msg = DummyMessage(rkey)
                result = handler(msg, "dummy_connector")
                self.assertEqual(result, msg)
            logs = lc.logs
        self.assertEqual([log['logLevel'] for log in logs],
                         [20, 20, 20, 40])
        self.assertEqual([log['message'][0] for log in logs], [
            "Processed inbound message for dummy_connector: inbound",
            "Processed outbound message for dummy_connector: outbound",
            "Processed event message for dummy_connector: event",
            "'Processed failure message for dummy_connector: failure'",
            ])

    def test_custom_log_level(self):
        mw = self.mklogger({'log_level': 'warning'})
        with LogCatcher() as lc:
            msg = DummyMessage("inbound")
            result = mw.handle_inbound(msg, "dummy_connector")
            self.assertEqual(result, msg)
            logs = lc.logs
        self.assertEqual([log['logLevel'] for log in logs], [30])
        self.assertEqual([log['message'][0] for log in logs], [
            "Processed inbound message for dummy_connector: inbound",
            ])

    def test_custom_failure_log_level(self):
        mw = self.mklogger({'failure_log_level': 'info'})
        with LogCatcher() as lc:
            msg = DummyMessage("failure")
            result = mw.handle_failure(msg, "dummy_connector")
            self.assertEqual(result, msg)
            logs = lc.logs
        self.assertEqual([log['logLevel'] for log in logs], [20])
        self.assertEqual([log['message'][0] for log in logs], [
            "Processed failure message for dummy_connector: failure",
            ])
