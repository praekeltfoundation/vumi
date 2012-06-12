# -*- test-case-name: vumi.application.tests.test_sandbox -*-

"""An application for sandboxing message processing."""

import sys
import resource
import os
import signal
import json
from uuid import uuid4

from twisted.internet import reactor
from twisted.internet.protocol import ProcessProtocol
from twisted.internet.defer import Deferred
from twisted.internet.error import ProcessDone
from twisted.python.failure import Failure

from vumi.application.base import ApplicationWorker
from vumi.message import Message
from vumi.errors import ConfigError
from vumi.utils import load_class_by_string, redis_from_config
from vumi import log


class MultiDeferred(object):
    """A callable that returns new deferreds each time and
    then fires them all together."""

    NOT_FIRED = object()

    def __init__(self):
        self._result = self.NOT_FIRED
        self._deferreds = []

    def callback(self, result):
        self._result = result
        for d in self._deferreds:
            d.callback(result)
        self._deferreds = []

    def get(self):
        d = Deferred()
        if self.fired():
            d.callback(self._result)
        else:
            self._deferreds.append(d)
        return d

    def fired(self):
        return self._result is not self.NOT_FIRED


class SandboxError(Exception):
    """An error occurred inside the sandbox."""


class SandboxRlimiter(object):
    """This reads rlimits in from stdin, applies them and then execs a
    new executable.

    It's necessary because Twisted's spawnProcess has no equivalent of
    the `preexec_fn` argument to :class:`subprocess.POpen`.

    See http://twistedmatrix.com/trac/ticket/4159.
    """
    def __init__(self, argv, env):
        start = argv.index('--') + 1
        self._executable = argv[start]
        self._args = [self._executable] + argv[start + 1:]
        self._env = env

    def _apply_rlimits(self):
        data = sys.stdin.readline()
        rlimits = json.loads(data) if data.strip() else {}
        for rlimit, (soft, hard) in rlimits.iteritems():
            resource.setrlimit(int(rlimit), (soft, hard))

    def _reset_signals(self):
        # reset all signal handlers to their defaults
        for i in range(1, signal.NSIG):
            if signal.getsignal(i) == signal.SIG_IGN:
                signal.signal(i, signal.SIG_DFL)

    def _sanitize_fds(self):
        # close everything except stdin, stdout and stderr
        maxfds = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
        os.closerange(3, maxfds)

    def execute(self):
        self._restore_python_path(os.environ)
        self._apply_rlimits()
        self._sanitize_fds()
        self._reset_signals()
        os.execvpe(self._executable, self._args, self._env)

    _SANDBOXED_PYTHONPATH_ = "_SANDBOXED_PYTHONPATH_"

    @classmethod
    def _override_python_path(cls, env):
        """Override PYTHONPATH so that SandboxRlimiter can be found."""
        if 'PYTHONPATH' in env:
            env[cls._SANDBOXED_PYTHONPATH_] = env['PYTHONPATH']
        env['PYTHONPATH'] = ':'.join(sys.path)

    @classmethod
    def _restore_python_path(cls, env):
        """Remove PYTHONPATH override."""
        if 'PYTHONPATH' in env:
            del env['PYTHONPATH']
        if cls._SANDBOXED_PYTHONPATH_ in env:
            env['PYTHONPATH'] = env.pop(cls._SANDBOXED_PYTHONPATH_)

    @classmethod
    def spawn(cls, protocol, executable, **kwargs):
        # spawns a SandboxRlimiter, connectionMade then passes the rlimits
        # through to stdin and the SandboxRlimiter applies them
        args = kwargs.pop('args', [])
        # the -u for unbuffered I/O is important (otherwise the process
        # execed will be very confused about where its stdin data has
        # gone)
        args = [sys.executable, '-u', '-m', __name__, '--'] + args
        env = kwargs.pop('env', {})
        cls._override_python_path(env)
        reactor.spawnProcess(protocol, sys.executable, args=args, env=env,
                             **kwargs)


class SandboxProtocol(ProcessProtocol):

    def __init__(self, api, rlimits, timeout, recv_limit):
        self.api = api
        self.rlimits = rlimits
        self._started = MultiDeferred()
        self._done = MultiDeferred()
        self.exit_reason = None
        self.timeout_task = reactor.callLater(timeout, self.kill)
        self.recv_limit = recv_limit
        self.recv_bytes = 0
        self.chunk = ''
        self.error_chunk = ''

    @staticmethod
    def rlimiter(args, env):
        return SandboxRlimiter(args, env)

    def spawn(self, executable, **kwargs):
        SandboxRlimiter.spawn(self, executable, **kwargs)

    def done(self):
        """Returns a deferred that will be called when the process ends."""
        return self._done.get()

    def started(self):
        """Returns a deferred that will be called once the process starts."""
        return self._started.get()

    def kill(self):
        """Kills the underlying process."""
        if self.transport.pid is not None:
            self.transport.signalProcess('KILL')

    def send(self, command):
        """Writes the command to the processes' stdin."""
        self.transport.write(command.to_json())
        self.transport.write("\n")

    def check_recv(self, nbytes):
        self.recv_bytes += nbytes
        if self.recv_bytes <= self.recv_limit:
            return True
        else:
            self.kill()
            return False

    def _send_rlimits(self):
        self.transport.write(json.dumps(self.rlimits))
        self.transport.write("\n")

    def connectionMade(self):
        self._send_rlimits()
        self._started.callback(self)

    def _process_data(self, chunk, data):
        if not self.check_recv(len(data)):
            return [chunk]  # skip the data if it's too big
        line_parts = data.split("\n")
        line_parts[0] = chunk + line_parts[0]
        return line_parts

    def _parse_command(self, line):
        try:
            return SandboxCommand.from_json(line)
        except Exception, e:
            return SandboxCommand(cmd="unknown", line=line, exception=e)

    def outReceived(self, data):
        lines = self._process_data(self.chunk, data)
        for i in range(len(lines) - 1):
            self.api.dispatch_request(self, self._parse_command(lines[i]))
        self.chunk = lines[-1]

    def outConnectionLost(self):
        if self.chunk:
            line, self.chunk = self.chunk, ""
            self.api.dispatch_request(self, self._parse_command(line))

    def errReceived(self, data):
        lines = self._process_data(self.error_chunk, data)
        for i in range(len(lines) - 1):
            log.error(Failure(SandboxError(lines[i])))
        self.error_chunk = lines[-1]

    def errConnectionLost(self):
        if self.error_chunk:
            log.error(Failure(SandboxError(self.error_chunk)))
            self.error_chunk = ""

    def processEnded(self, reason):
        if self.timeout_task.active():
            self.timeout_task.cancel()
        if isinstance(reason.value, ProcessDone):
            result = reason.value.status
        else:
            result = reason
        if not self._started.fired():
            self._started.callback(Failure(
                SandboxError("Process failed to start.")))
        self._done.callback(result)


class SandboxResources(object):
    """Class for holding resources common to a set of sandboxes."""

    def __init__(self, app_worker, config):
        self.app_worker = app_worker
        self.config = config
        self.resources = {}

    def validate_config(self):
        for name, config in self.config.iteritems():
            cls = load_class_by_string(config.pop('cls'))
            self.resources[name] = cls(name, self.app_worker, config)

    def setup_resources(self):
        for resource in self.resources.itervalues():
            resource.setup()

    def teardown_resources(self):
        for resource in self.resources.itervalues():
            resource.teardown()


class SandboxResource(object):
    """Base class for sandbox resources."""

    def __init__(self, name, app_worker, config):
        self.name = name
        self.app_worker = app_worker
        self.config = config

    def setup(self):
        pass

    def teardown(self):
        pass

    def sandbox_init(self, api, sandbox):
        pass

    def reply(self, command, **kwargs):
        return SandboxCommand(cmd=command['cmd'], reply=True,
                              cmd_id=command['cmd_id'], **kwargs)

    def log_error(self, error_msg):
        log.error(Failure(SandboxError(error_msg)))

    def dispatch_request(self, api, sandbox, command):
        handler_name = 'handle_%s' % (command['cmd'],)
        handler = getattr(self, handler_name, self.unknown_request)
        return handler(api, sandbox, command)

    def unknown_request(self, api, sandbox, command):
        self.log_error("Resource %s: unknown command %r received from"
                       " sandbox %r [%r]" % (self.name, command['cmd'],
                                             api.sandbox_id, command))
        sandbox.kill()  # it's a harsh world


class RedisResource(SandboxResource):
    def setup(self):
        self.r_prefix = self.config.get('r_prefix')
        self.r_server = redis_from_config(self.config.get('redis', {}))
        self.keys_per_user = self.config.get('keys_per_user', 100)

    def _count_key(self, sandbox_id):
        return ":".join([self.r_prefix, "count", sandbox_id])

    def _sandboxed_key(self, sandbox_id, key):
        return ":".join([self.r_prefix, "sandboxes", sandbox_id, key])

    def check_keys(self, sandbox_id, key):
        if self.r_server.exists(key):
            return True
        count_key = self._count_key(sandbox_id)
        if self.r_server.incr(count_key, 1) > self.keys_per_user:
            self.r_server.incr(count_key, -1)
            return False
        return True

    def handle_set(self, api, sandbox, command):
        key = self._sandboxed_key(api.sandbox_id, command.get('key'))
        if not self.check_keys(api.sandbox_id, key):
            return command.reply("Too many keys")
        value = command.get('value')
        self.r_server.set(key, json.dumps(value))
        return self.reply(command, success=True)

    def handle_get(self, api, sandbox, command):
        key = self._sandboxed_key(api.sandbox_id, command.get('key'))
        return self.reply(command, success=True,
                          value=json.loads(self.r_server.get(key)))

    def handle_delete(self, api, sandbox, command):
        key = self._sandboxed_key(api.sandbox_id, command.get('key'))
        existed = bool(self.r_server.delete(key))
        if existed:
            count_key = self._count_key(api.sandbox_id)
            self.r_server.incr(count_key, -1)
        return self.reply(command, success=True,
                          existed=existed)


class OutboundResource(SandboxResource):

    def handle_reply_to(self, api, sandbox, command):
        content = command['content']
        continue_session = command.get('continue_session', True)
        orig_msg = api.get_inbound_message(command['in_reply_to'])
        self.app_worker.reply_to(orig_msg, content,
                                 continue_session=continue_session)

    def handle_reply_to_group(self, api, sandbox, command):
        content = command['content']
        continue_session = command.get('continue_session', True)
        orig_msg = api.get_inbound_message(command['in_reply_to'])
        self.app_worker.reply_to_group(orig_msg, content,
                                       continue_session=continue_session)

    def handle_send_to(self, api, sandbox, command):
        content = command['content']
        to_addr = command['to_addr']
        tag = command.get('tag', 'default')
        self.app_worker.send_to(to_addr, content, tag=tag)


class JsSandboxResource(SandboxResource):
    def setup(self):
        self.javascript = self.config.get('javascript')

    def sandbox_init(self, api, sandbox):
        sandbox.send(SandboxCommand(cmd="initialize",
                                    javascript=self.javascript))


class LoggingResource(SandboxResource):

    def handle_info(self, api, sandbox, command):
        log.info(command['msg'])
        return self.reply(command, success=True)


class SandboxApi(object):
    """A sandbox API instance for a particular sandbox run."""

    def __init__(self, sandbox_id, resources):
        self.sandbox_id = sandbox_id
        self.resources = resources
        self.fallback_resource = SandboxResource("fallback", None, {})
        self._inbound_messages = {}

    # TODO: refactor this all to just include the sandbox in the API object?

    def sandbox_init(self, sandbox):
        for resource in self.resources.resources.values():
            resource.sandbox_init(self, sandbox)

    def sandbox_inbound_message(self, sandbox, msg):
        self._inbound_messages[msg['message_id']] = msg
        sandbox.send(SandboxCommand(cmd="inbound-message", msg=msg.payload))

    def sandbox_inbound_event(self, sandbox, event):
        sandbox.send(SandboxCommand(cmd="inbound-event", msg=event.payload))

    def sandbox_reply(self, sandbox, reply):
        sandbox.send(reply)

    def get_inbound_message(self, message_id):
        return self._inbound_messages.get(message_id)

    def dispatch_request(self, sandbox, command):
        resource_name, sep, rest = command['cmd'].partition('.')
        if not sep:
            resource_name, rest = '', resource_name
        command['cmd'] = rest
        resource = self.resources.resources.get(resource_name,
                                                self.fallback_resource)
        reply = resource.dispatch_request(self, sandbox, command)
        if reply is not None:
            reply['cmd'] = '%s%s%s' % (resource_name, sep, rest)
            self.sandbox_reply(sandbox, reply)


class SandboxCommand(Message):
    @staticmethod
    def generate_id():
        return uuid4().get_hex()

    def process_fields(self, fields):
        fields = super(SandboxCommand, self).process_fields(fields)
        fields.setdefault('cmd', 'unknown')
        fields.setdefault('cmd_id', self.generate_id())
        fields.setdefault('reply', False)
        return fields

    def validate_fields(self):
        super(SandboxCommand, self).validate_fields()
        self.assert_field_present(
            'cmd',
            'cmd_id',
            'reply',
            )


class Sandbox(ApplicationWorker):
    """
    Configuration options:

    :param str executable:
        Full path to the executable to run in the sandbox.
    :param list args:
        List of arguments to pass to the executable (not including
        the path of the executable itself).
    :param str path:
        Current working directory to run the executable in.
    :param int timeout:
        Length of time the subprocess is given to process
        a message.
    :param int recv_limit:
        Maximum number of bytes that will be read from a sandboxed
        process' stdout and stderr combined.
    :param dict sandbox:
        Dictionary of resources to provide to the sandbox.
        Keys are the names of resources (as seen inside the sandbox).
        Values are dictionaries which must contain a `cls` key that
        gives the full name of the class that provides the resource.
        Other keys are additional configuration for that resource.
    :param dict rlimits:
        Dictionary of resource limits to be applied to sandboxed
        processes. Defaults are fairly restricted. Keys maybe
        names or values of the RLIMIT constants in
        :module:`resource`. Values should be appropriate integers.
    """

    KB, MB = 1024, 1024 * 1024
    DEFAULT_RLIMITS = {
        resource.RLIMIT_CORE: (1 * MB, 1 * MB),
        resource.RLIMIT_CPU: (60, 60),
        resource.RLIMIT_FSIZE: (1 * MB, 1 * MB),
        resource.RLIMIT_DATA: (10 * MB, 10 * MB),
        resource.RLIMIT_STACK: (1 * MB, 1 * MB),
        resource.RLIMIT_RSS: (10 * MB, 10 * MB),
        resource.RLIMIT_NPROC: (1, 1),
        resource.RLIMIT_NOFILE: (10, 10),
        resource.RLIMIT_MEMLOCK: (64 * KB, 64 * KB),
        resource.RLIMIT_AS: (50 * MB, 50 * MB),
        }

    def validate_config(self):
        self.executable = self.config.get("executable")
        self.args = [self.executable] + self.config.get("args", [])
        self.path = self.config.get("path", None)
        self.timeout = int(self.config.get("timeout", "60"))
        self.recv_limit = int(self.config.get("recv_limit", 1024 * 1024))
        self.resources = self.create_sandbox_resources(
            self.config.get('sandbox', {}))
        self.resources.validate_config()
        self.rlimits = self.DEFAULT_RLIMITS.copy()
        self.rlimits.update(self._convert_rlimits(
            self.config.get('rlimits', {})))

    def _convert_rlimits(self, rlimits_config):
        rlimits = dict((getattr(resource, key, key), value) for key, value in
                       rlimits_config.iteritems())
        for key in rlimits.iterkeys():
            if not isinstance(key, (int, long)):
                raise ConfigError("Unknown resource limit key %r" % (key,))
        return rlimits

    def setup_application(self):
        return self.resources.setup_resources()

    def teardown_application(self):
        return self.resources.teardown_resources()

    def create_sandbox_resources(self, config):
        return SandboxResources(self, config)

    def create_sandbox_api(self, sandbox_id):
        return SandboxApi(sandbox_id, self.resources)

    def create_sandbox_protocol(self, api):
        return SandboxProtocol(api, self.rlimits, self.timeout,
                               self.recv_limit)

    def sandbox_id_for_message(self, msg):
        """Sub-classes should override this to retrieve an appropriate id."""
        return msg['sandbox_id']

    def sandbox_id_for_event(self, event):
        """Sub-classes should override this to retrieve an appropriate id."""
        return event['sandbox_id']

    def _process_in_sandbox(self, api, api_callback):
        sandbox_protocol = self.create_sandbox_protocol(api)
        sandbox_protocol.spawn(self.executable, args=self.args,
                               env={}, path=self.path)

        def on_start(_result):
            api.sandbox_init(sandbox_protocol)
            api_callback(sandbox_protocol)
            d = sandbox_protocol.done()
            d.addErrback(log.error)
            return d

        d = sandbox_protocol.started()
        d.addCallbacks(on_start, log.error)
        return d

    def process_message_in_sandbox(self, msg):
        sandbox_id = self.sandbox_id_for_message(msg)
        api = self.create_sandbox_api(sandbox_id)
        return self._process_in_sandbox(api,
                lambda sandbox: api.sandbox_inbound_message(sandbox, msg))

    def process_event_in_sandbox(self, event):
        sandbox_id = self.sandbox_id_for_event(event)
        api = self.create_sandbox_api(sandbox_id)
        return self._process_in_sandbox(api,
                lambda sandbox: api.sandbox_inbound_event(sandbox, event))

    def consume_user_message(self, msg):
        return self.process_message_in_sandbox(msg)

    def close_session(self, msg):
        return self.process_message_in_sandbox(msg)

    def consume_ack(self, event):
        return self.process_event_in_sandbox(event)

    def consume_delivery_report(self, event):
        return self.process_event_in_sandbox(event)


if __name__ == "__main__":
    rlimiter = SandboxProtocol.rlimiter(sys.argv, os.environ)
    rlimiter.execute()
