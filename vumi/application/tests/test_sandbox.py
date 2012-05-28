"""Tests for vumi.application.sandbox."""

import json

from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ProcessTerminated

from vumi.message import TransportEvent
from vumi.application.tests.test_base import ApplicationTestCase
from vumi.application.sandbox import Sandbox, SandboxCommand, SandboxError
from vumi.tests.utils import FakeRedis


class SandboxTestCase(ApplicationTestCase):

    application_class = Sandbox

    def setup_app(self, executable, args, extra_config=None):
        config = {
            'executable': executable,
            'args': args,
            'path': '/tmp',  # TODO: somewhere temporary for test
            'timeout': '1',
            }
        if extra_config is not None:
            config.update(extra_config)
        return self.get_application(config)

    @inlineCallbacks
    def test_echo_from_sandbox(self):
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

    @inlineCallbacks
    def test_resource_setup(self):
        r_server = FakeRedis()
        json_data = SandboxCommand(cmd='db.set', key='foo',
                                   value={'a': 1, 'b': 2}).to_json()
        app = yield self.setup_app('/bin/echo', [json_data], {
            'sandbox': {
                'db': {
                    'cls': 'vumi.application.sandbox.RedisResource',
                    'redis': r_server,
                    'r_prefix': 'test',
                    }
                }
            })
        event = TransportEvent(event_type='ack', user_message_id=1,
                               sent_message_id=1, sandbox_id='sandbox1')
        status = yield app.process_event_in_sandbox(event)
        self.assertEqual(status, 0)
        self.assertEqual(sorted(r_server.keys()),
                         ['test:count:sandbox1',
                          'test:sandboxes:sandbox1:foo'])
        self.assertEqual(r_server.get('test:count:sandbox1'), '1')
        self.assertEqual(r_server.get('test:sandboxes:sandbox1:foo'),
                         json.dumps({'a': 1, 'b': 2}))

    # TODO: test error logging
