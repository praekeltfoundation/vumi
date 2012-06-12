"""Tests for vumi.application.sandbox."""

import os
import sys
import json
import pkg_resources
from collections import defaultdict

from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ProcessTerminated
from twisted.trial.unittest import TestCase, SkipTest

from vumi.message import TransportUserMessage, TransportEvent
from vumi.application.tests.test_base import ApplicationTestCase
from vumi.application.sandbox import (Sandbox, SandboxCommand, SandboxError,
                                      RedisResource, OutboundResource,
                                      JsSandboxResource, LoggingResource)
from vumi.tests.utils import FakeRedis, LogCatcher


class SandboxTestCaseBase(ApplicationTestCase):

    application_class = Sandbox

    def setup_app(self, executable, args, extra_config=None):
        tmp_path = self.mktemp()
        os.mkdir(tmp_path)
        config = {
            'executable': executable,
            'args': args,
            'path': tmp_path,
            'timeout': '10',
            }
        if extra_config is not None:
            config.update(extra_config)
        return self.get_application(config)


class SandboxTestCase(SandboxTestCaseBase):

    def setup_app(self, python_code, extra_config=None):
        return super(SandboxTestCase, self).setup_app(
            sys.executable, ['-c', python_code],
            extra_config=extra_config)

    @inlineCallbacks
    def test_bad_command_from_sandbox(self):
        app = yield self.setup_app(
            "import sys, time\n"
            "sys.stdout.write('{}\\n')\n"
            "sys.stdout.flush()\n"
            "time.sleep(5)\n"
            )
        event = TransportEvent(event_type='ack', user_message_id=1,
                               sent_message_id=1, sandbox_id='sandbox1')
        status = yield app.process_event_in_sandbox(event)
        [sandbox_err] = self.flushLoggedErrors(SandboxError)
        self.assertEqual(str(sandbox_err.value).split(' [')[0],
                         "Resource fallback: unknown command 'unknown'"
                         " received from sandbox 'sandbox1'")
        self.assertEqual(status, None)
        [kill_err] = self.flushLoggedErrors(ProcessTerminated)
        self.assertTrue('process ended by signal' in str(kill_err.value))

    @inlineCallbacks
    def test_stderr_from_sandbox(self):
        app = yield self.setup_app(
            "import sys\n"
            "sys.stderr.write('err\\n')\n"
            )
        event = TransportEvent(event_type='ack', user_message_id=1,
                               sent_message_id=1, sandbox_id='sandbox1')
        status = yield app.process_event_in_sandbox(event)
        self.assertEqual(status, 0)
        [sandbox_err] = self.flushLoggedErrors(SandboxError)
        self.assertEqual(str(sandbox_err.value).split(' [')[0], "err")

    @inlineCallbacks
    def test_resource_setup(self):
        r_server = FakeRedis()
        json_data = SandboxCommand(cmd='db.set', key='foo',
                                   value={'a': 1, 'b': 2}).to_json()
        app = yield self.setup_app(
            "import sys\n"
            "sys.stdout.write(%r)\n" % json_data,
            {'sandbox': {
                'db': {
                    'cls': 'vumi.application.sandbox.RedisResource',
                    'redis': r_server,
                    'r_prefix': 'test',
                },
            }})
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

    @inlineCallbacks
    def test_outbound_reply_from_sandbox(self):
        msg = TransportUserMessage(to_addr="1", from_addr="2",
                                   transport_name="test",
                                   transport_type="sphex",
                                   sandbox_id='sandbox1')
        json_data = SandboxCommand(cmd='outbound.reply_to',
                                   content='Hooray!',
                                   in_reply_to=msg['message_id']).to_json()
        app = yield self.setup_app(
            "import sys\n"
            "sys.stdout.write(%r)\n" % json_data,
            {'sandbox': {
                    'outbound': {
                        'cls': 'vumi.application.sandbox.OutboundResource',
                    },
             }})
        status = yield app.process_message_in_sandbox(msg)
        self.assertEqual(status, 0)
        [reply] = self.get_dispatched_messages()
        self.assertEqual(reply['content'], "Hooray!")
        self.assertEqual(reply['session_event'], None)

    # TODO: test process killed if it writes too much.

    # TODO: def consume_user_message(self, msg):
    # TODO: def close_session(self, msg):
    # TODO: def consume_ack(self, event):
    # TODO: def consume_delivery_report(self, event):


class NodeJsSandboxTestCase(SandboxTestCaseBase):

    possible_nodejs_executables = [
        '/usr/local/bin/node',
        '/usr/bin/node',
        ]

    def setUp(self):
        super(NodeJsSandboxTestCase, self).setUp()
        for path in self.possible_nodejs_executables:
            if os.path.isfile(path):
                self.nodejs_executable = path
                break
        else:
            raise SkipTest("No node.js executable found.")
        self.sandboxer_js = pkg_resources.resource_filename('vumi.application',
                                                            'sandboxer.js')

    def setup_app(self, javascript_code, extra_config=None):
        extra_config = extra_config or {}
        sandbox_config = extra_config.setdefault('sandbox', {})
        sandbox_config.update({
                'log': {
                    'cls': 'vumi.application.sandbox.LoggingResource',
                    },
                 'js': {
                    'cls': 'vumi.application.sandbox.JsSandboxResource',
                    'javascript': javascript_code,
                    },
            })
        return super(NodeJsSandboxTestCase, self).setup_app(
            self.nodejs_executable, [self.sandboxer_js],
            extra_config=extra_config)

    @inlineCallbacks
    def test_js_sandboxer(self):
        msg = TransportUserMessage(to_addr="1", from_addr="2",
                                   transport_name="test",
                                   transport_type="sphex",
                                   sandbox_id='sandbox1')
        app_js = pkg_resources.resource_filename('vumi.application',
                                                 'app.js')
        javascript = file(app_js).read()
        app = yield self.setup_app(javascript)

        with LogCatcher() as lc:
            status = yield app.process_message_in_sandbox(msg)
            failures = [log['failure'].value for log in lc.errors]
            msgs = [log['message'][0] for log in lc.logs if log['message']]
        self.assertEqual(failures, [])
        self.assertEqual(status, 0)
        self.assertEqual(msgs, [
            'Starting sandbox ...',
            'Loading sandboxed code ...',
            'From init!',
            'From command: inbound-message',
            'Log successful: true',
            'Done.',
            ])


class DummyAppWorker(object):

    class DummyApi(object):
        def __init__(self, sandbox_id):
            self.sandbox_id = sandbox_id

    class DummyProtocol(object):
        def __init__(self, api):
            self.api = api

    sandbox_api_cls = DummyApi
    sandbox_protocol_cls = DummyProtocol

    def __init__(self):
        self.mock_calls = defaultdict(list)

    def create_sandbox_api(self, sandbox_id):
        return self.sandbox_api_cls(sandbox_id)

    def create_sandbox_protocol(self, api):
        return self.sandbox_protocol_cls(api)

    def __getattr__(self, name):
        def mock_method(*args, **kw):
            self.mock_calls[name].append((args, kw))
        return mock_method


class ResourceTestCaseBase(TestCase):

    app_worker_cls = DummyAppWorker
    resource_cls = None
    resource_name = 'test_resource'
    sandbox_id = 'test_id'

    def setUp(self):
        self.app_worker = self.app_worker_cls()
        self.resource = None
        self.api = self.app_worker.create_sandbox_api(self.sandbox_id)
        self.sandbox = self.app_worker.create_sandbox_protocol(self.api)

    def tearDown(self):
        if self.resource is not None:
            self.resource.teardown()

    def create_resource(self, config):
        resource = self.resource_cls(self.resource_name,
                                     self.app_worker,
                                     config)
        resource.setup()
        self.resource = resource

    def dispatch_command(self, cmd, **kwargs):
        if self.resource is None:
            raise ValueError("Create a resource before"
                             " calling dispatch_command")
        msg = SandboxCommand(cmd=cmd, **kwargs)
        return self.resource.dispatch_request(self.api, self.sandbox, msg)


class TestRedisResource(ResourceTestCaseBase):

    resource_cls = RedisResource

    def setUp(self):
        super(TestRedisResource, self).setUp()
        self.r_server = FakeRedis()
        self.create_resource({
            'r_prefix': 'test',
            'redis': self.r_server,
            })

    def tearDown(self):
        super(TestRedisResource, self).tearDown()
        self.r_server.teardown()

    def test_handle_set(self):
        reply = self.dispatch_command('set', key='foo', value='bar')
        self.assertEqual(reply['success'], True)
        self.assertEqual(self.r_server.get('test:sandboxes:test_id:foo'),
                         json.dumps('bar'))
        self.assertEqual(self.r_server.get('test:count:test_id'), '1')

    def test_handle_get(self):
        self.r_server.set('test:sandboxes:test_id:foo', json.dumps('bar'))
        reply = self.dispatch_command('get', key='foo')
        self.assertEqual(reply['success'], True)
        self.assertEqual(reply['value'], 'bar')

    def test_handle_delete(self):
        self.r_server.set('test:sandboxes:test_id:foo', json.dumps('bar'))
        self.r_server.set('test:count:test_id', '1')
        reply = self.dispatch_command('delete', key='foo')
        self.assertEqual(reply['success'], True)
        self.assertEqual(reply['existed'], True)
        self.assertEqual(self.r_server.get('test:sandboxes:test_id:foo'), None)
        self.assertEqual(self.r_server.get('test:count:test_id'), '0')


class TestOutboundResource(ResourceTestCaseBase):

    resource_cls = OutboundResource

    def setUp(self):
        super(TestOutboundResource, self).setUp()
        self.create_resource({})

    def test_handle_reply_to(self):
        self.api.get_inbound_message = lambda msg_id: msg_id
        reply = self.dispatch_command('reply_to', content='hello',
                                      continue_session=True,
                                      in_reply_to='msg1')
        self.assertEqual(reply, None)
        self.assertEqual(self.app_worker.mock_calls['reply_to'],
                         [(('msg1', 'hello'), {'continue_session': True})])

    def test_handle_reply_to_group(self):
        self.api.get_inbound_message = lambda msg_id: msg_id
        reply = self.dispatch_command('reply_to_group', content='hello',
                                      continue_session=True,
                                      in_reply_to='msg1')
        self.assertEqual(reply, None)
        self.assertEqual(self.app_worker.mock_calls['reply_to_group'],
                         [(('msg1', 'hello'), {'continue_session': True})])

    def test_handle_send_to(self):
        reply = self.dispatch_command('send_to', content='hello',
                                      to_addr='1234',
                                      tag='default')
        self.assertEqual(reply, None)
        self.assertEqual(self.app_worker.mock_calls['send_to'],
                         [(('1234', 'hello'), {'tag': 'default'})])


class TestJsSandboxResource(ResourceTestCaseBase):

    resource_cls = JsSandboxResource

    def setUp(self):
        super(TestJsSandboxResource, self).setUp()
        self.create_resource({
            'javascript': 'testscript',
            })

    def test_sandbox_init(self):
        msgs = []
        self.sandbox.send = lambda msg: msgs.append(msg)
        self.resource.sandbox_init(self.api, self.sandbox)
        self.assertEqual(msgs, [SandboxCommand(cmd='initialize',
                                               cmd_id=msgs[0]['cmd_id'],
                                               javascript='testscript')])


class TestLoggingResource(ResourceTestCaseBase):

    resource_cls = LoggingResource

    def setUp(self):
        super(TestLoggingResource, self).setUp()
        self.create_resource({})

    def test_handle_info(self):
        with LogCatcher() as lc:
            reply = self.dispatch_command('info', msg='foo')
            msgs = [log['message'][0] for log in lc.logs if log['message']]
        self.assertEqual(reply['success'], True)
        self.assertEqual(msgs, ['foo'])
