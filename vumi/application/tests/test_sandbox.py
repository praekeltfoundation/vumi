"""Tests for vumi.application.sandbox."""

from twisted.internet.defer import inlineCallbacks

from vumi.message import TransportEvent

from vumi.application.tests.test_base import ApplicationTestCase
from vumi.application.sandbox import Sandbox


class SandboxTestCase(ApplicationTestCase):

    application_class = Sandbox

    def setup_app(self, executable, args=None):
        return self.get_application({
            'executable': executable,
            'args': args if args is not None else [],
            'path': '/tmp',  # TODO: somewhere temporary for test
            'timeout': '1',
            })

    @inlineCallbacks
    def test_process_in_sandbox(self):
        app = yield self.setup_app('/bin/echo', ['-n', '{}'])
        event = TransportEvent(event_type='ack', user_message_id=1,
                               sent_message_id=1)
        status = yield app.process_in_sandbox(event)
        self.assertEqual(status, 0)
