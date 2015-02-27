"""Tests for vumi.application.sandbox."""

import os
import sys
import json
import resource
import pkg_resources
import logging
from datetime import datetime


from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.internet.error import ProcessTerminated

from vumi.application.sandbox.worker import (
    Sandbox, SandboxApi, SandboxCommand, SandboxResources,
    JsSandboxResource, JsSandbox, JsFileSandbox)
from vumi.application.sandbox import SandboxResource, LoggingResource
from vumi.application.sandbox.tests.utils import DummyAppWorker
from vumi.application.sandbox.resources.tests.utils import (
    ResourceTestCaseBase)
from vumi.application.tests.helpers import (
    ApplicationHelper, find_nodejs_or_skip_test)
from vumi.tests.utils import LogCatcher
from vumi.tests.helpers import VumiTestCase


class MockResource(SandboxResource):
    def __init__(self, name, app_worker, **handlers):
        super(MockResource, self).__init__(name, app_worker, {})
        for name, handler in handlers.iteritems():
            setattr(self, "handle_%s" % name, handler)


class ListLoggingResource(LoggingResource):
    def __init__(self, name, app_worker, config):
        super(ListLoggingResource, self).__init__(name, app_worker, config)
        self.msgs = []

    def log(self, api, msg, level):
        self.msgs.append((level, msg))


class SandboxTestCaseBase(VumiTestCase):

    application_class = Sandbox

    def setUp(self):
        self.app_helper = self.add_helper(
            ApplicationHelper(self.application_class))

    def setup_app(self, executable=None, args=None, extra_config=None):
        tmp_path = self.mktemp()
        os.mkdir(tmp_path)
        config = {
            'path': tmp_path,
            'timeout': '10',
        }
        if executable is not None:
            config['executable'] = executable
        if args is not None:
            config['args'] = args
        if extra_config is not None:
            config.update(extra_config)
        return self.app_helper.get_application(config)


class TestSandbox(SandboxTestCaseBase):

    def setup_app(self, python_code, extra_config=None):
        return super(TestSandbox, self).setup_app(
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
        with LogCatcher(log_level=logging.ERROR) as lc:
            status = yield app.process_event_in_sandbox(
                self.app_helper.make_ack(sandbox_id='sandbox1'))
            [msg] = lc.messages()
        self.assertTrue(msg.startswith(
            "Resource fallback received unknown command 'unknown'"
            " from sandbox 'sandbox1'. Killing sandbox."
            " [Full command: <Message payload=\"{"
        ))
        self.assertEqual(status, None)
        [kill_err] = self.flushLoggedErrors(ProcessTerminated)
        self.assertTrue('process ended by signal' in str(kill_err.value))

    @inlineCallbacks
    def test_stderr_from_sandbox(self):
        app = yield self.setup_app(
            "import sys\n"
            "sys.stderr.write('err\\n')\n"
        )
        with LogCatcher(log_level=logging.ERROR) as lc:
            status = yield app.process_event_in_sandbox(
                self.app_helper.make_ack(sandbox_id='sandbox1'))
            msgs = lc.messages()
        self.assertEqual(status, 0)
        self.assertEqual(msgs, ["err"])

    @inlineCallbacks
    def test_stderr_from_sandbox_with_multiple_lines(self):
        app = yield self.setup_app(
            "import sys\n"
            "sys.stderr.write('err1\\nerr2\\nerr3')\n"
        )
        with LogCatcher(log_level=logging.ERROR) as lc:
            status = yield app.process_event_in_sandbox(
                self.app_helper.make_ack(sandbox_id='sandbox1'))
            msgs = lc.messages()
        self.assertEqual(status, 0)
        self.assertEqual(msgs, ["err1\nerr2\nerr3"])

    @inlineCallbacks
    def test_bad_rlimit(self):
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        # This irreversibly sets limits for the current process.
        # 10k file handles should be enough for everyone, right?
        hard = min(hard, 10000)
        soft = min(soft, hard)
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))

        app = yield self.setup_app(
            "import sys\n"
            "import resource\n"
            "rlimit_nofile = resource.getrlimit(resource.RLIMIT_NOFILE)\n"
            "sys.stderr.write('%s %s\\n' % rlimit_nofile)\n",
            {'rlimits': {'RLIMIT_NOFILE': [soft, hard * 2]}})
        with LogCatcher(log_level=logging.ERROR) as lc:
            status = yield app.process_event_in_sandbox(
                self.app_helper.make_ack(sandbox_id='sandbox1'))
            msgs = lc.messages()
        self.assertEqual(status, 0)
        self.assertEqual(msgs, ["%s %s" % (soft, hard)])

    @inlineCallbacks
    def test_resource_setup(self):
        r_server = yield self.app_helper.get_redis_manager()
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
        status = yield app.process_event_in_sandbox(
            self.app_helper.make_ack(sandbox_id='sandbox1'))
        self.assertEqual(status, 0)
        self.assertEqual(sorted((yield r_server.keys())),
                         ['count#sandbox1',
                          'sandboxes#sandbox1#foo'])
        self.assertEqual((yield r_server.get('count#sandbox1')), '1')
        self.assertEqual((yield r_server.get('sandboxes#sandbox1#foo')),
                         json.dumps({'a': 1, 'b': 2}))

    @inlineCallbacks
    def test_outbound_reply_from_sandbox(self):
        msg = self.app_helper.make_inbound("foo", sandbox_id='sandbox1')
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
        [reply] = self.app_helper.get_dispatched_outbound()
        self.assertEqual(reply['content'], "Hooray!")
        self.assertEqual(reply['session_event'], None)

    @inlineCallbacks
    def test_recv_limit(self):
        recv_limit = 1000
        send_out = "a" * 500
        send_err = "a" * 501
        app = yield self.setup_app(
            "import sys, time\n"
            "sys.stdout.write(%r)\n"
            "sys.stdout.flush()\n"
            "sys.stderr.write(%r)\n"
            "sys.stderr.flush()\n"
            "time.sleep(5)\n"
            % (send_out, send_err),
            {'recv_limit': str(recv_limit)})
        with LogCatcher(log_level=logging.ERROR) as lc:
            status = yield app.process_message_in_sandbox(
                self.app_helper.make_inbound("foo", sandbox_id='sandbox1'))
            msgs = lc.messages()
        self.assertEqual(status, None)
        self.assertEqual(msgs[0],
                         "Sandbox 'sandbox1' killed for producing too much"
                         " data on stderr and stdout.")
        self.assertEqual(len(msgs), 2)  # 2nd message is the bad command log
        [kill_err] = self.flushLoggedErrors(ProcessTerminated)
        self.assertTrue('process ended by signal' in str(kill_err.value))

    @inlineCallbacks
    def test_env_variable(self):
        app = yield self.setup_app(
            "import sys, os, json\n"
            "test_value = os.environ['TEST_VAR']\n"
            "log = {'cmd': 'log.info', 'cmd_id': '1',\n"
            "       'reply': False, 'msg': test_value}\n"
            "sys.stdout.write(json.dumps(log) + '\\n')\n",
            {'env': {'TEST_VAR': 'success'},
             'sandbox': {
                 'log': {'cls': 'vumi.application.sandbox.LoggingResource'},
             }},
        )
        with LogCatcher() as lc:
            status = yield app.process_message_in_sandbox(
                self.app_helper.make_inbound("foo", sandbox_id='sandbox1'))
            [value_str] = lc.messages()
        self.assertEqual(status, 0)
        self.assertEqual(value_str, "success")

    @inlineCallbacks
    def test_python_path_set(self):
        app = yield self.setup_app(
            "import sys, json\n"
            "path = ':'.join(sys.path)\n"
            "log = {'cmd': 'log.info', 'cmd_id': '1',\n"
            "       'reply': False, 'msg': path}\n"
            "sys.stdout.write(json.dumps(log) + '\\n')\n",
            {'env': {'PYTHONPATH': '/pp1:/pp2'},
             'sandbox': {
                 'log': {'cls': 'vumi.application.sandbox.LoggingResource'},
             }},
        )
        with LogCatcher() as lc:
            status = yield app.process_message_in_sandbox(
                self.app_helper.make_inbound("foo", sandbox_id='sandbox1'))
            [path_str] = lc.messages()
        self.assertEqual(status, 0)
        path = path_str.split(':')
        self.assertTrue('/pp1' in path)
        self.assertTrue('/pp2' in path)

    @inlineCallbacks
    def test_python_path_unset(self):
        app = yield self.setup_app(
            "import sys, json\n"
            "path = ':'.join(sys.path)\n"
            "log = {'cmd': 'log.info', 'cmd_id': '1',\n"
            "       'reply': False, 'msg': path}\n"
            "sys.stdout.write(json.dumps(log) + '\\n')\n",
            {'env': {},
             'sandbox': {
                 'log': {'cls': 'vumi.application.sandbox.LoggingResource'},
             }},
        )
        with LogCatcher() as lc:
            status = yield app.process_message_in_sandbox(
                self.app_helper.make_inbound("foo", sandbox_id='sandbox1'))
            [path_str] = lc.messages()
        self.assertEqual(status, 0)
        path = path_str.split(':')
        self.assertTrue('/pp1' not in path)
        self.assertTrue('/pp2' not in path)

    @inlineCallbacks
    def test_custom_logging_resource(self):
        app = yield self.setup_app(
            "import sys, json\n"
            "log = {'cmd': 'foo.info', 'cmd_id': '1',\n"
            "       'reply': False, 'msg': 'log info'}\n"
            "sys.stdout.write(json.dumps(log) + '\\n')\n",
            {'env': {},
             'logging_resource': 'foo',
             'sandbox': {
                 'foo': {'cls': '%s.ListLoggingResource' % __name__},
             }},
        )
        with LogCatcher() as lc:
            status = yield app.process_message_in_sandbox(
                self.app_helper.make_inbound("foo", sandbox_id='sandbox1'))
            msgs = lc.messages()
        self.assertEqual(status, 0)
        logging_resource = app.resources.resources['foo']
        self.assertEqual(logging_resource.msgs, [
            (logging.INFO, 'log info')
        ])
        self.assertEqual(msgs, [])

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
        msg = self.app_helper.make_inbound("foo", sandbox_id='sandbox1')
        return self.echo_check('consume_user_message', msg, 'inbound-message')

    def test_close_session(self):
        msg = self.app_helper.make_inbound("foo", sandbox_id='sandbox1')
        return self.echo_check('close_session', msg, 'inbound-message')

    def test_consume_ack(self):
        msg = self.app_helper.make_ack(sandbox_id='sandbox1')
        return self.echo_check('consume_ack', msg, 'inbound-event')

    def test_consume_nack(self):
        msg = self.app_helper.make_nack(sandbox_id='sandbox1')
        return self.echo_check('consume_nack', msg, 'inbound-event')

    def test_consume_delivery_report(self):
        msg = self.app_helper.make_delivery_report(sandbox_id='sandbox1')
        return self.echo_check('consume_delivery_report', msg, 'inbound-event')

    @inlineCallbacks
    def event_dispatch_check(self, event):
        yield self.setup_app(
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
            yield self.app_helper.dispatch_event(event)
            [cmd_json] = lc.messages()

        if not cmd_json.startswith('{'):
            self.fail(cmd_json)
        echoed_cmd = json.loads(cmd_json)
        self.assertEqual(echoed_cmd['cmd'], 'inbound-event')
        echoed_cmd['msg']['timestamp'] = event['timestamp']
        self.assertEqual(echoed_cmd['msg'], event.payload)

    def test_event_dispatch_default(self):
        return self.event_dispatch_check(
            self.app_helper.make_ack(sandbox_id='sandbox1'))

    def test_event_dispatch_non_default(self):
        ack = self.app_helper.make_ack(sandbox_id='sandbox1')
        ack.set_routing_endpoint('foo')
        return self.event_dispatch_check(ack)

    def test_sandbox_command_does_not_parse_timestamps(self):
        # We should serialise datetime objects correctly.
        timestamp = datetime(2014, 07, 18, 15, 0, 0)
        json_cmd = SandboxCommand(cmd='foo', timestamp=timestamp).to_json()
        # We should not parse timestamp-like strings into datetime objects.
        cmd = SandboxCommand.from_json(json_cmd)
        self.assertEqual(cmd['timestamp'], "2014-07-18 15:00:00.000000")


class JsSandboxTestMixin(object):

    @inlineCallbacks
    def test_js_sandboxer(self):
        app_js = pkg_resources.resource_filename(
            'vumi.application.sandbox.tests', 'app.js')
        javascript = file(app_js).read()
        app = yield self.setup_app(javascript)

        with LogCatcher() as lc:
            status = yield app.process_message_in_sandbox(
                self.app_helper.make_inbound("foo", sandbox_id='sandbox1'))
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

    @inlineCallbacks
    def test_js_sandboxer_with_app_context(self):
        app_js = pkg_resources.resource_filename(
            'vumi.application.sandbox.tests', 'app_requires_path.js')
        javascript = file(app_js).read()
        app = yield self.setup_app(javascript, extra_config={
            "app_context": "{path: require('path')}",
        })

        with LogCatcher() as lc:
            status = yield app.process_message_in_sandbox(
                self.app_helper.make_inbound("foo", sandbox_id='sandbox1'))
            failures = [log['failure'].value for log in lc.errors]
            msgs = lc.messages()
        self.assertEqual(failures, [])
        self.assertEqual(status, 0)
        self.assertEqual(msgs, [
            'Starting sandbox ...',
            'Loading sandboxed code ...',
            'From init!',
            'We have access to path!',
            'Done.',
        ])

    @inlineCallbacks
    def test_js_sandboxer_with_delayed_requests(self):
        app_js = pkg_resources.resource_filename(
            'vumi.application.sandbox.tests', 'app_delayed_requests.js')
        javascript = file(app_js).read()
        app = yield self.setup_app(javascript, extra_config={
            "app_context": "{setImmediate: setImmediate}",
        })

        with LogCatcher() as lc:
            status = yield app.process_message_in_sandbox(
                self.app_helper.make_inbound("foo", sandbox_id='sandbox1'))
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


class TestJsSandbox(SandboxTestCaseBase, JsSandboxTestMixin):

    application_class = JsSandbox

    def setUp(self):
        self._node_path = find_nodejs_or_skip_test(self.application_class)
        super(TestJsSandbox, self).setUp()

    def setup_app(self, javascript_code, extra_config=None):
        extra_config = extra_config or {}
        extra_config.update({
            'javascript': javascript_code,
            'executable': self._node_path,
        })
        return super(TestJsSandbox, self).setup_app(
            extra_config=extra_config)


class TestJsFileSandbox(SandboxTestCaseBase, JsSandboxTestMixin):

    application_class = JsFileSandbox

    def setUp(self):
        self._node_path = find_nodejs_or_skip_test(self.application_class)
        super(TestJsFileSandbox, self).setUp()

    def setup_app(self, javascript, extra_config=None):
        tmp_file_name = self.mktemp()
        tmp_file = open(tmp_file_name, 'w')
        tmp_file.write(javascript)
        tmp_file.close()

        extra_config = extra_config or {}
        extra_config.update({
            'javascript_file': tmp_file_name,
            'executable': self._node_path,
        })

        return super(TestJsFileSandbox, self).setup_app(
            extra_config=extra_config)


class TestSandboxApi(VumiTestCase):
    def setUp(self):
        self.sent_messages = DeferredQueue()
        self.patch(SandboxApi, 'sandbox_send',
                   staticmethod(lambda msg: self.sent_messages.put(msg)))
        self.app = DummyAppWorker()
        self.resources = SandboxResources(self.app, {})
        self.api = SandboxApi(self.resources, self.app)

    @inlineCallbacks
    def test_request_dispatching_for_uncaught_exceptions(self):
        def handle_use(api, command):
            raise Exception('Something bad happened')
        self.resources.add_resource(
            'bad_resource',
            MockResource('bad_resource', self.app, use=handle_use))

        command = SandboxCommand(cmd='bad_resource.use')
        self.api.dispatch_request(command)
        msg = yield self.sent_messages.get()

        self.assertEqual(msg['cmd'], 'bad_resource.use')
        self.assertEqual(msg['cmd_id'], command['cmd_id'])
        self.assertTrue(msg['reply'])
        self.assertFalse(msg['success'])
        self.assertEqual(msg['reason'], u'Something bad happened')

        logged_error = self.flushLoggedErrors()[0]
        self.assertEqual(str(logged_error.value), 'Something bad happened')
        self.assertEqual(logged_error.type, Exception)


class JsDummyAppWorker(DummyAppWorker):
    def javascript_for_api(self, api):
        return 'testscript'

    def app_context_for_api(self, api):
        return 'appcontext'


class TestJsSandboxResource(ResourceTestCaseBase):

    resource_cls = JsSandboxResource

    app_worker_cls = JsDummyAppWorker

    @inlineCallbacks
    def setUp(self):
        super(TestJsSandboxResource, self).setUp()
        yield self.create_resource({})

    def test_sandbox_init(self):
        msgs = []
        self.api.sandbox_send = lambda msg: msgs.append(msg)
        self.resource.sandbox_init(self.api)
        self.assertEqual(msgs, [SandboxCommand(cmd='initialize',
                                               cmd_id=msgs[0]['cmd_id'],
                                               javascript='testscript',
                                               app_context='appcontext')])
