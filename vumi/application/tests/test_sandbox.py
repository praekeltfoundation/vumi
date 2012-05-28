"""Tests for vumi.application.sandbox."""

from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ProcessTerminated

from vumi.message import TransportEvent

from vumi.application.tests.test_base import ApplicationTestCase
from vumi.application.sandbox import Sandbox, SandboxError


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
                               sent_message_id=1, sandbox_id='sandbox1')
        status = yield app.process_event_in_sandbox(event)
        [sandbox_err] = self.flushLoggedErrors(SandboxError)
        self.assertEqual(str(sandbox_err.value).split(' [')[0],
                         "Resource fallback: unknown command 'unknown'"
                         " received from sandbox 'sandbox1'")
        # There is are two possible conditions here:
        # 1) The process is killed and terminates with signal 9.
        # 2) The process exits normally before it can be killed and returns
        #    exit status 0.
        if status is None:
            [kill_err] = self.flushLoggedErrors(ProcessTerminated)
            self.assertTrue('process ended by signal' in str(kill_err.value))
        else:
            self.assertEqual(status, 0)

    # TODO: test error logging
