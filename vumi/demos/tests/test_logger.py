"""Tests for vumi.demos.logger."""

from twisted.internet.defer import inlineCallbacks

from vumi.application.tests.test_base import ApplicationTestCase

from vumi.demos.logger import LoggerWorker


class TestLoggerWorker(ApplicationTestCase):

    application_class = LoggerWorker

    @inlineCallbacks
    def setUp(self):
        super(TestLoggerWorker, self).setUp()
        # TODO: setup dummy log server
        self.worker = yield self.get_application({
            'log_server': 'TODO',
            })

    def test_inbound_channel_msg(self):
        pass

    def test_inbound_channel_action(self):
        pass
