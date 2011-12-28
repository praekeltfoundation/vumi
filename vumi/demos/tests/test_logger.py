"""Tests for vumi.demos.logger."""

import json

from twisted.internet.defer import inlineCallbacks

from vumi.application.tests.test_base import ApplicationTestCase
from vumi.tests.utils import MockHttpServer

from vumi.demos.logger import LoggerWorker


class TestLoggerWorker(ApplicationTestCase):

    application_class = LoggerWorker

    @inlineCallbacks
    def setUp(self):
        super(TestLoggerWorker, self).setUp()
        self.mock_log = MockHttpServer(self.handle_request)
        self.mock_log_calls = []
        yield self.mock_log.start()
        self.worker = yield self.get_application({
            'log_server': self.mock_log.url,
            })

    def handle_request(self, request):
        content = json.loads(request.content.read())
        self.mock_log_calls.append((request, content))
        self.assertEqual(request.getHeader('content-type'), 'application/json')
        return "OK"

    @inlineCallbacks
    def tearDown(self):
        super(TestLoggerWorker, self).tearDown()
        yield self.mock_log.stop()

    @inlineCallbacks
    def test_inbound_private_msg(self):
        msg = self.mkmsg_in(content="a message")
        yield self.dispatch(msg)
        self.assertEqual(self.mock_log_calls, [])

    @inlineCallbacks
    def test_inbound_channel_msg(self):
        msg = self.mkmsg_in(content="a message",
                            from_addr="userfoo!user@example.com",
                            transport_metadata={
                                'irc_channel': "#bar",
                                'irc_command': 'PRIVMSG',
                                },
                            )
        yield self.dispatch(msg)
        [(request, content)] = self.mock_log_calls
        self.assertEqual(content, {
            'message_type': 'message',
            'msg': "a message",
            'nickname': "userfoo",
            'channel': "#bar",
            })

    @inlineCallbacks
    def test_inbound_channel_action(self):
        msg = self.mkmsg_in(content="an action",
                            from_addr="userfoo!user@example.com",
                            transport_metadata={
                                'irc_channel': "#bar",
                                'irc_command': 'ACTION',
                                },
                            )
        yield self.dispatch(msg)
        [(request, content)] = self.mock_log_calls
        self.assertEqual(content, {
            'message_type': 'action',
            'msg': "* userfoo an action",
            'channel': "#bar",
            })
