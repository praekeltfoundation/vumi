"""Tests for vumi.application.sandbox."""

import os
import sys
import json
import resource
import pkg_resources
import logging
from collections import defaultdict

from OpenSSL.SSL import (
    VERIFY_PEER, VERIFY_FAIL_IF_NO_PEER_CERT, VERIFY_NONE)

from twisted.internet.defer import (
    inlineCallbacks, fail, succeed, DeferredQueue)
from twisted.internet.error import ProcessTerminated
from twisted.trial.unittest import SkipTest

from vumi.application.sandbox import (
    Sandbox, SandboxApi, SandboxCommand, SandboxResources,
    SandboxResource, RedisResource, OutboundResource, JsSandboxResource,
    LoggingResource, HttpClientResource, JsSandbox, JsFileSandbox,
    HttpClientContextFactory)
from vumi.application.tests.helpers import ApplicationHelper
from vumi.tests.utils import LogCatcher
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


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
        self.app_helper = ApplicationHelper(self.application_class)
        self.add_cleanup(self.app_helper.cleanup)

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


class JsSandboxTestMixin(object):

    @inlineCallbacks
    def test_js_sandboxer(self):
        app_js = pkg_resources.resource_filename('vumi.application.tests',
                                                 'app.js')
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
        app_js = pkg_resources.resource_filename('vumi.application.tests',
                                                 'app_requires_path.js')
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


class TestJsSandbox(SandboxTestCaseBase, JsSandboxTestMixin):

    application_class = JsSandbox

    def setUp(self):
        if JsSandbox.find_nodejs() is None:
            raise SkipTest("No node.js executable found.")
        super(TestJsSandbox, self).setUp()

    def setup_app(self, javascript_code, extra_config=None):
        extra_config = extra_config or {}
        extra_config.update({
            'javascript': javascript_code,
        })
        return super(TestJsSandbox, self).setup_app(
            extra_config=extra_config)


class TestJsFileSandbox(SandboxTestCaseBase, JsSandboxTestMixin):

    application_class = JsFileSandbox

    def setUp(self):
        if JsSandbox.find_nodejs() is None:
            raise SkipTest("No node.js executable found.")
        super(TestJsFileSandbox, self).setUp()

    def setup_app(self, javascript, extra_config=None):
        tmp_file_name = self.mktemp()
        tmp_file = open(tmp_file_name, 'w')
        tmp_file.write(javascript)
        tmp_file.close()

        extra_config = extra_config or {}
        extra_config.update({
            'javascript_file': tmp_file_name,
        })

        return super(TestJsFileSandbox, self).setup_app(
            extra_config=extra_config)


class DummyAppWorker(object):

    class DummyApi(object):
        def __init__(self):
            self.logs = []

        def set_sandbox(self, sandbox):
            self.sandbox = sandbox
            self.sandbox_id = sandbox.sandbox_id

        def log(self, message, level):
            self.logs.append((level, message))

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


class ResourceTestCaseBase(VumiTestCase):

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
    def create_resource(self, config):
        if self.resource is not None:
            # clean-up any existing resource so
            # .create_resource can be called multiple times.
            yield self.resource.teardown()
        resource = self.resource_cls(self.resource_name,
                                     self.app_worker,
                                     config)
        self.add_cleanup(resource.teardown)
        yield resource.setup()
        self.resource = resource

    def dispatch_command(self, cmd, **kwargs):
        if self.resource is None:
            raise ValueError("Create a resource before"
                             " calling dispatch_command")
        msg = SandboxCommand(cmd=cmd, **kwargs)
        # round-trip message to get something more similar
        # to what would be returned by a real sandbox when
        # msgs are loaded from JSON.
        msg = SandboxCommand.from_json(msg.to_json())
        return self.resource.dispatch_request(self.api, msg)


class TestRedisResource(ResourceTestCaseBase):

    resource_cls = RedisResource

    @inlineCallbacks
    def setUp(self):
        super(TestRedisResource, self).setUp()
        self.persistence_helper = PersistenceHelper()
        self.add_cleanup(self.persistence_helper.cleanup)
        self.r_server = yield self.persistence_helper.get_redis_manager()
        yield self.create_resource({})

    def create_resource(self, config):
        config.setdefault('redis_manager', {
            'FAKE_REDIS': self.r_server,
            'key_prefix': self.r_server._key_prefix,
        })
        return super(TestRedisResource, self).create_resource(config)

    def check_reply(self, reply, success=True, **kw):
        self.assertEqual(reply['success'], success)
        for key, expected_value in kw.iteritems():
            self.assertEqual(reply[key], expected_value)

    @inlineCallbacks
    def create_metric(self, metric, value, total_count=1):
        metric_key = 'sandboxes#test_id#' + metric
        count_key = 'count#test_id'
        yield self.r_server.set(metric_key, value)
        yield self.r_server.set(count_key, total_count)

    @inlineCallbacks
    def check_metric(self, metric, value, total_count):
        metric_key = 'sandboxes#test_id#' + metric
        count_key = 'count#test_id'
        self.assertEqual((yield self.r_server.get(metric_key)), value)
        self.assertEqual((yield self.r_server.get(count_key)),
                         str(total_count))

    def assert_api_log(self, expected_level, expected_message):
        [log_entry] = self.api.logs
        level, message = log_entry
        self.assertEqual(level, expected_level)
        self.assertEqual(message, expected_message)

    @inlineCallbacks
    def test_handle_set(self):
        reply = yield self.dispatch_command('set', key='foo', value='bar')
        self.check_reply(reply, success=True)
        yield self.check_metric('foo', json.dumps('bar'), 1)

    @inlineCallbacks
    def test_handle_set_soft_limit_reached(self):
        yield self.create_metric('foo', 'a', total_count=80)
        reply = yield self.dispatch_command('set', key='bar', value='bar')
        self.check_reply(reply, success=True)
        self.assert_api_log(
            logging.WARNING,
            'Redis soft limit of 80 keys reached for sandbox test_id. '
            'Once the hard limit of 100 is reached no more keys can '
            'be written.'
        )

    @inlineCallbacks
    def test_handle_set_hard_limit_reached(self):
        yield self.create_metric('foo', 'a', total_count=100)
        reply = yield self.dispatch_command('set', key='bar', value='bar')
        self.check_reply(reply, success=False, reason='Too many keys')
        yield self.check_metric('bar', None, 100)
        self.assert_api_log(
            logging.ERROR,
            'Redis hard limit of test_id keys reached for sandbox 100. '
            'No more keys can be written.'
        )

    @inlineCallbacks
    def test_keys_per_user_fallback_hard_limit(self):
        yield self.create_resource({
            'keys_per_user': 10,
        })
        yield self.create_metric('foo', 'a', total_count=10)
        reply = yield self.dispatch_command('set', key='bar', value='bar')
        self.check_reply(reply, success=False, reason='Too many keys')
        self.assert_api_log(
            logging.ERROR,
            'Redis hard limit of test_id keys reached for sandbox 10. '
            'No more keys can be written.'
        )

    @inlineCallbacks
    def test_keys_per_user_fallback_soft_limit(self):
        yield self.create_resource({
            'keys_per_user': 10,
        })
        yield self.create_metric('foo', 'a', total_count=8)
        reply = yield self.dispatch_command('set', key='bar', value='bar')
        self.check_reply(reply, success=True)
        self.assert_api_log(
            logging.WARNING,
            'Redis soft limit of 8 keys reached for sandbox test_id. '
            'Once the hard limit of 10 is reached no more keys can '
            'be written.'
        )

    @inlineCallbacks
    def test_handle_get(self):
        yield self.create_metric('foo', json.dumps('bar'))
        reply = yield self.dispatch_command('get', key='foo')
        self.check_reply(reply, success=True, value='bar')

    @inlineCallbacks
    def test_handle_get_for_unknown_key(self):
        reply = yield self.dispatch_command('get', key='foo')
        self.check_reply(reply, success=True, value=None)

    @inlineCallbacks
    def test_handle_delete(self):
        self.create_metric('foo', json.dumps('bar'))
        yield self.r_server.set('count#test_id', '1')
        reply = yield self.dispatch_command('delete', key='foo')
        self.check_reply(reply, success=True, existed=True)
        yield self.check_metric('foo', None, 0)

    @inlineCallbacks
    def test_handle_incr_default_amount(self):
        reply = yield self.dispatch_command('incr', key='foo')
        self.check_reply(reply, success=True, value=1)
        yield self.check_metric('foo', '1', 1)

    @inlineCallbacks
    def test_handle_incr_create(self):
        reply = yield self.dispatch_command('incr', key='foo', amount=2)
        self.check_reply(reply, success=True, value=2)
        yield self.check_metric('foo', '2', 1)

    @inlineCallbacks
    def test_handle_incr_existing(self):
        self.create_metric('foo', '2')
        reply = yield self.dispatch_command('incr', key='foo', amount=2)
        self.check_reply(reply, success=True, value=4)
        yield self.check_metric('foo', '4', 1)

    @inlineCallbacks
    def test_handle_incr_existing_non_int(self):
        self.create_metric('foo', 'a')
        reply = yield self.dispatch_command('incr', key='foo', amount=2)
        self.check_reply(reply, success=False)
        self.assertTrue(reply['reason'])
        yield self.check_metric('foo', 'a', 1)

    @inlineCallbacks
    def test_handle_incr_soft_limit_reached(self):
        yield self.create_metric('foo', 'a', total_count=80)
        reply = yield self.dispatch_command('incr', key='bar', amount=2)
        self.check_reply(reply, success=True)
        [limit_warning] = self.api.logs
        level, message = limit_warning
        self.assertEqual(level, logging.WARNING)
        self.assertEqual(
            message,
            'Redis soft limit of 80 keys reached for sandbox test_id. '
            'Once the hard limit of 100 is reached no more keys can '
            'be written.')

    @inlineCallbacks
    def test_handle_incr_hard_limit_reached(self):
        yield self.create_metric('foo', 'a', total_count=100)
        reply = yield self.dispatch_command('incr', key='bar', amount=2)
        self.check_reply(reply, success=False, reason='Too many keys')
        yield self.check_metric('bar', None, 100)
        [limit_error] = self.api.logs
        level, message = limit_error
        self.assertEqual(level, logging.ERROR)
        self.assertEqual(
            message,
            'Redis hard limit of test_id keys reached for sandbox 100. '
            'No more keys can be written.')


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
                         [(('1234', 'hello'), {'endpoint': 'default'})])


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


class TestLoggingResource(ResourceTestCaseBase):

    resource_cls = LoggingResource

    @inlineCallbacks
    def setUp(self):
        super(TestLoggingResource, self).setUp()
        yield self.create_resource({})

    @inlineCallbacks
    def check_logs(self, cmd_name, msg, log_level, **kw):
        with LogCatcher(log_level=log_level) as lc:
            reply = yield self.dispatch_command(cmd_name, msg=msg, **kw)
            msgs = lc.messages()
        self.assertEqual(reply['success'], True)
        self.assertEqual(msgs, [msg])

    def test_handle_debug(self):
        return self.check_logs('debug', 'foo', logging.DEBUG)

    def test_handle_info(self):
        return self.check_logs('info', 'foo', logging.INFO)

    def test_handle_warning(self):
        return self.check_logs('warning', 'foo', logging.WARNING)

    def test_handle_error(self):
        return self.check_logs('error', 'foo', logging.ERROR)

    def test_handle_critical(self):
        return self.check_logs('critical', 'foo', logging.CRITICAL)

    def test_handle_log(self):
        return self.check_logs('log', 'foo', logging.ERROR,
                               level=logging.ERROR)

    def test_handle_log_defaults_to_info(self):
        return self.check_logs('log', 'foo', logging.INFO)

    def test_with_unicode(self):
        with LogCatcher() as lc:
            reply = yield self.dispatch_command('log', msg=u'Zo\u00eb')
            msgs = lc.messages()
        self.assertEqual(reply['success'], True)
        self.assertEqual(msgs, ['Zo\xc3\xab'])


class TestHttpClientResource(ResourceTestCaseBase):

    resource_cls = HttpClientResource

    class DummyResponse(object):
        pass

    @inlineCallbacks
    def setUp(self):
        super(TestHttpClientResource, self).setUp()
        yield self.create_resource({})
        import vumi.application.sandbox
        self.patch(vumi.application.sandbox,
                   'http_request_full', self.dummy_http_request)
        self._next_http_request_result = None
        self._http_requests = []

    def dummy_http_request(self, *args, **kw):
        self._http_requests.append((args, kw))
        return self._next_http_request_result

    def http_request_fail(self, error):
        self._next_http_request_result = fail(error)

    def http_request_succeed(self, body, code=200):
        response = self.DummyResponse()
        response.delivered_body = body
        response.code = code
        self._next_http_request_result = succeed(response)

    def assert_not_unicode(self, arg):
        self.assertFalse(isinstance(arg, unicode))

    def get_context_factory(self):
        return self._context_factory

    def assert_http_request(self, url, method='GET', headers={}, data=None,
                            timeout=None, data_limit=None):
        timeout = (timeout if timeout is not None
                   else self.resource.timeout)
        data_limit = (data_limit if data_limit is not None
                      else self.resource.data_limit)
        args = (url,)
        kw = dict(method=method, headers=headers, data=data,
                  timeout=timeout, data_limit=data_limit)
        [(actual_args, actual_kw)] = self._http_requests
        self._context_factory = actual_kw.pop('context_factory')
        self.assertTrue(isinstance(self._context_factory,
                                   HttpClientContextFactory))
        self.assertEqual((actual_args, actual_kw), (args, kw))

        self.assert_not_unicode(actual_args[0])
        self.assert_not_unicode(actual_kw.get('data'))
        for key, values in actual_kw.get('headers', {}).items():
            self.assert_not_unicode(key)
            for value in values:
                self.assert_not_unicode(value)

    @inlineCallbacks
    def test_handle_get(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('get',
                                            url='http://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('http://www.example.com', method='GET')

    @inlineCallbacks
    def test_handle_post(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('post',
                                            url='http://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('http://www.example.com', method='POST')

    @inlineCallbacks
    def test_handle_head(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('head',
                                            url='http://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('http://www.example.com', method='HEAD')

    @inlineCallbacks
    def test_handle_delete(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('delete',
                                            url='http://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('http://www.example.com', method='DELETE')

    @inlineCallbacks
    def test_handle_put(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('put',
                                            url='http://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('http://www.example.com', method='PUT')

    @inlineCallbacks
    def test_failed_get(self):
        self.http_request_fail(ValueError("HTTP request failed"))
        reply = yield self.dispatch_command('get',
                                            url='http://www.example.com')
        self.assertFalse(reply['success'])
        self.assertEqual(reply['reason'], "HTTP request failed")
        self.assert_http_request('http://www.example.com', method='GET')

    @inlineCallbacks
    def test_null_url(self):
        reply = yield self.dispatch_command('get')
        self.assertFalse(reply['success'])
        self.assertEqual(reply['reason'], "No URL given")

    @inlineCallbacks
    def test_https_request(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command('get',
                                            url='https://www.example.com')
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('https://www.example.com', method='GET')

        ctxt = self.get_context_factory()
        self.assertEqual(ctxt.verify_options, None)

    @inlineCallbacks
    def test_https_request_verify_none(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command(
            'get', url='https://www.example.com',
            verify_options=['VERIFY_NONE'])
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('https://www.example.com', method='GET')

        ctxt = self.get_context_factory()
        self.assertEqual(ctxt.verify_options, VERIFY_NONE)

    @inlineCallbacks
    def test_https_request_verify_peer_or_fail(self):
        self.http_request_succeed("foo")
        reply = yield self.dispatch_command(
            'get', url='https://www.example.com',
            verify_options=['VERIFY_PEER', 'VERIFY_FAIL_IF_NO_PEER_CERT'])
        self.assertTrue(reply['success'])
        self.assertEqual(reply['body'], "foo")
        self.assert_http_request('https://www.example.com', method='GET')

        ctxt = self.get_context_factory()
        self.assertEqual(
            ctxt.verify_options,
            VERIFY_PEER | VERIFY_FAIL_IF_NO_PEER_CERT)
