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
from vumi.tests.utils import LogCatcher, PersistenceMixin


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

    def mk_event(self, **kw):
        msg_kw = {
            'event_type': 'ack', 'user_message_id': '1',
            'sent_message_id': '1', 'sandbox_id': 'sandbox1',
        }
        msg_kw.update(kw)
        return TransportEvent(**msg_kw)

    def mk_msg(self, **kw):
        msg_kw = {
            'to_addr': "1", 'from_addr': "2",
            'transport_name': "test", 'transport_type': "sphex",
            'sandbox_id': 'sandbox1',
        }
        msg_kw.update(kw)
        return TransportUserMessage(**msg_kw)


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
        status = yield app.process_event_in_sandbox(self.mk_event())
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
        status = yield app.process_event_in_sandbox(self.mk_event())
        self.assertEqual(status, 0)
        [sandbox_err] = self.flushLoggedErrors(SandboxError)
        self.assertEqual(str(sandbox_err.value).split(' [')[0], "err")

    @inlineCallbacks
    def test_resource_setup(self):
        r_server = yield self.get_redis_manager()
        json_data = SandboxCommand(cmd='db.set', key='foo',
                                   value={'a': 1, 'b': 2}).to_json()
        app = yield self.setup_app(
            "import sys\n"
            "sys.stdout.write(%r)\n" % json_data,
            {'sandbox': {
                'db': {
                    'cls': 'vumi.application.sandbox.RedisResource',
                    'redis_manager': {
                        'FAKE_REDIS': r_server,
                        'key_prefix': r_server._key_prefix,
                    },
                },
            }})
        status = yield app.process_event_in_sandbox(self.mk_event())
        self.assertEqual(status, 0)
        self.assertEqual(sorted((yield r_server.keys())),
                         ['count#sandbox1',
                          'sandboxes#sandbox1#foo'])
        self.assertEqual((yield r_server.get('count#sandbox1')), '1')
        self.assertEqual((yield r_server.get('sandboxes#sandbox1#foo')),
                         json.dumps({'a': 1, 'b': 2}))

    @inlineCallbacks
    def test_outbound_reply_from_sandbox(self):
        msg = self.mk_msg()
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

    @inlineCallbacks
    def test_recv_limit(self):
        recv_limit = 1000
        app = yield self.setup_app(
            "import sys, time\n"
            "sys.stderr.write(%r)\n"
            "sys.stdout.write('\\n')\n"
            "time.sleep(5)\n"
            % ("a" * (recv_limit - 1) + "\\n"),
            {'recv_limit': str(recv_limit)})
        status = yield app.process_message_in_sandbox(self.mk_msg())
        self.assertEqual(status, None)
        [kill_err] = self.flushLoggedErrors(ProcessTerminated)
        self.assertTrue('process ended by signal' in str(kill_err.value))

    @inlineCallbacks
    def echo_check(self, handler_name, msg, expected_cmd):
        app = yield self.setup_app(
            "import sys, json\n"
            "cmd = sys.stdin.readline()\n"
            "log = {'cmd': 'log.info', 'cmd_id': '1',\n"
            "       'reply': False, 'msg': cmd}\n"
            "sys.stdout.write(json.dumps(log) + '\\n')\n",
            {'sandbox': {
                'log': {'cls': 'vumi.application.sandbox.LoggingResource'},
            }},
        )
        with LogCatcher() as lc:
            status = yield getattr(app, handler_name)(msg)
            [cmd_json] = lc.messages()

        self.assertEqual(status, 0)
        echoed_cmd = json.loads(cmd_json)
        self.assertEqual(echoed_cmd['cmd'], expected_cmd)
        echoed_cmd['msg']['timestamp'] = msg['timestamp']
        self.assertEqual(echoed_cmd['msg'], msg.payload)

    def test_consume_user_message(self):
        return self.echo_check('consume_user_message', self.mk_msg(),
                               'inbound-message')

    def test_close_session(self):
        return self.echo_check('close_session', self.mk_msg(),
                               'inbound-message')

    def test_consume_ack(self):
        return self.echo_check('consume_ack', self.mk_event(),
                               'inbound-event')

    def test_consume_delivery_report(self):
        return self.echo_check('consume_delivery_report', self.mk_event(),
                               'inbound-event')


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
        app_js = pkg_resources.resource_filename('vumi.application.tests',
                                                 'app.js')
        javascript = file(app_js).read()
        app = yield self.setup_app(javascript)

        with LogCatcher() as lc:
            status = yield app.process_message_in_sandbox(self.mk_msg())
            failures = [log['failure'].value for log in lc.errors]
            msgs = lc.messages()
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
        def __init__(self):
            pass

        def set_sandbox(self, sandbox):
            self.sandbox = sandbox
            self.sandbox_id = sandbox.sandbox_id

    class DummyProtocol(object):
        def __init__(self, sandbox_id, api):
            self.sandbox_id = sandbox_id
            self.api = api
            api.set_sandbox(self)

    sandbox_api_cls = DummyApi
    sandbox_protocol_cls = DummyProtocol

    def __init__(self):
        self.mock_calls = defaultdict(list)

    def create_sandbox_api(self):
        return self.sandbox_api_cls()

    def create_sandbox_protocol(self, sandbox_id, api):
        return self.sandbox_protocol_cls(sandbox_id, api)

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
        self.api = self.app_worker.create_sandbox_api()
        self.sandbox = self.app_worker.create_sandbox_protocol(self.sandbox_id,
                                                               self.api)

    @inlineCallbacks
    def tearDown(self):
        if self.resource is not None:
            yield self.resource.teardown()

    @inlineCallbacks
    def create_resource(self, config):
        resource = self.resource_cls(self.resource_name,
                                     self.app_worker,
                                     config)
        yield resource.setup()
        self.resource = resource

    def dispatch_command(self, cmd, **kwargs):
        if self.resource is None:
            raise ValueError("Create a resource before"
                             " calling dispatch_command")
        msg = SandboxCommand(cmd=cmd, **kwargs)
        return self.resource.dispatch_request(self.api, msg)


class TestRedisResource(ResourceTestCaseBase, PersistenceMixin):

    resource_cls = RedisResource

    @inlineCallbacks
    def setUp(self):
        super(TestRedisResource, self).setUp()
        yield self._persist_setUp()
        self.r_server = yield self.get_redis_manager()
        yield self.create_resource({
            'redis_manager': {
                'FAKE_REDIS': self.r_server,
                'key_prefix': self.r_server._key_prefix,
            }})

    @inlineCallbacks
    def tearDown(self):
        yield super(TestRedisResource, self).tearDown()
        yield self._persist_tearDown()

    @inlineCallbacks
    def test_handle_set(self):
        reply = yield self.dispatch_command('set', key='foo', value='bar')
        self.assertEqual(reply['success'], True)
        self.assertEqual((yield
                          self.r_server.get('sandboxes#test_id#foo')),
                         json.dumps('bar'))
        self.assertEqual((yield self.r_server.get('count#test_id')), '1')

    @inlineCallbacks
    def test_handle_get(self):
        yield self.r_server.set('sandboxes#test_id#foo', json.dumps('bar'))
        reply = yield self.dispatch_command('get', key='foo')
        self.assertEqual(reply['success'], True)
        self.assertEqual(reply['value'], 'bar')

    @inlineCallbacks
    def test_handle_delete(self):
        yield self.r_server.set('sandboxes#test_id#foo',
                                json.dumps('bar'))
        yield self.r_server.set('count#test_id', '1')
        reply = yield self.dispatch_command('delete', key='foo')
        self.assertEqual(reply['success'], True)
        self.assertEqual(reply['existed'], True)
        self.assertEqual((yield
                          self.r_server.get('sandboxes#test_id#foo')),
                         None)
        self.assertEqual((yield self.r_server.get('count#test_id')), '0')


class TestOutboundResource(ResourceTestCaseBase):

    resource_cls = OutboundResource

    @inlineCallbacks
    def setUp(self):
        super(TestOutboundResource, self).setUp()
        yield self.create_resource({})

    @inlineCallbacks
    def test_handle_reply_to(self):
        self.api.get_inbound_message = lambda msg_id: msg_id
        reply = yield self.dispatch_command('reply_to', content='hello',
                                            continue_session=True,
                                            in_reply_to='msg1')
        self.assertEqual(reply, None)
        self.assertEqual(self.app_worker.mock_calls['reply_to'],
                         [(('msg1', 'hello'), {'continue_session': True})])

    @inlineCallbacks
    def test_handle_reply_to_group(self):
        self.api.get_inbound_message = lambda msg_id: msg_id
        reply = yield self.dispatch_command('reply_to_group', content='hello',
                                            continue_session=True,
                                            in_reply_to='msg1')
        self.assertEqual(reply, None)
        self.assertEqual(self.app_worker.mock_calls['reply_to_group'],
                         [(('msg1', 'hello'), {'continue_session': True})])

    @inlineCallbacks
    def test_handle_send_to(self):
        reply = yield self.dispatch_command('send_to', content='hello',
                                            to_addr='1234',
                                            tag='default')
        self.assertEqual(reply, None)
        self.assertEqual(self.app_worker.mock_calls['send_to'],
                         [(('1234', 'hello'), {'tag': 'default'})])


class TestJsSandboxResource(ResourceTestCaseBase):

    resource_cls = JsSandboxResource

    @inlineCallbacks
    def setUp(self):
        super(TestJsSandboxResource, self).setUp()
        yield self.create_resource({
            'javascript': 'testscript',
        })

    def test_sandbox_init(self):
        msgs = []
        self.api.sandbox_send = lambda msg: msgs.append(msg)
        self.resource.sandbox_init(self.api)
        self.assertEqual(msgs, [SandboxCommand(cmd='initialize',
                                               cmd_id=msgs[0]['cmd_id'],
                                               javascript='testscript')])


class TestLoggingResource(ResourceTestCaseBase):

    resource_cls = LoggingResource

    @inlineCallbacks
    def setUp(self):
        super(TestLoggingResource, self).setUp()
        yield self.create_resource({})

    @inlineCallbacks
    def test_handle_info(self):
        with LogCatcher() as lc:
            reply = yield self.dispatch_command('info', msg='foo')
            msgs = lc.messages()
        self.assertEqual(reply['success'], True)
        self.assertEqual(msgs, ['foo'])
