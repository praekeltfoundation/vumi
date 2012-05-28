# -*- test-case-name: vumi.application.tests.test_sandbox -*-

"""An application for sandboxing message processing."""

import sys
import resource
import os
import json

from twisted.internet import reactor
from twisted.internet.protocol import ProcessProtocol
from twisted.internet.defer import Deferred
from twisted.internet.error import ProcessDone
from twisted.python.failure import Failure

from vumi.application.base import ApplicationWorker
from vumi.message import Message
from vumi.errors import ConfigError
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
        for rlimit, value in rlimits.iteritems():
            # TODO: allow soft and hard limits to be different
            resource.setrlimit(int(rlimit), (value, value))

    def execute(self):
        self._apply_rlimits()
        os.execvpe(self._executable, self._args, self._env)


class SandboxProtocol(ProcessProtocol):

    def __init__(self, api, rlimits, timeout, recv_limit=1024 * 1024):
        self.api = api
        self.rlimits = rlimits
        self._started = MultiDeferred()
        self._done = MultiDeferred()
        self.exit_reason = None
        self.timeout_task = reactor.callLater(timeout, self.kill)
        self.recv_limit = recv_limit
        self.recv_bytes = 0
        self.chunk = ''

    @staticmethod
    def rlimiter(args, env):
        return SandboxRlimiter(args, env)

    def spawn(self, executable, **kwargs):
        # spawns a SandboxRlimiter, connectionMade then passes the rlimits
        # through to stdin and the SandboxRlimiter applies them
        kwargs['args'] = ['-m', __name__, '--'] + kwargs.get('args', [])
        reactor.spawnProcess(self, sys.executable, **kwargs)

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

    def outReceived(self, data):
        if not self.check_recv(len(data)):
            return
        pos = data.find("\n")
        if pos == -1:
            self.chunk += data
        else:
            # TODO: check for errors when parsing JSON
            command = SandboxCommand.from_json(self.chunk + data[:pos])
            self.api.dispatch_request(command)
            self.chunk = data[pos + 1:]

    def errReceived(self, data):
        if not self.check_recv(len(data)):
            return
        # TODO: this needs to be a Failure instance and have some context
        log.error(data)

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

    # TODO: maybe add some simple resource options?

    def __init__(self, config):
        # for use by sub-classes that want more advanced config
        self.config = config

    def validate_config(self):
        pass

    def setup_resources(self):
        pass

    def teardown_resources(self):
        pass


class SandboxApi(object):
    """A sandbox API instance for a particular sandbox run."""

    def __init__(self, resources):
        self.resources = resources

    def sandbox_init(self, sandbox):
        sandbox.send(SandboxCommand("initialize"))

    def sandbox_vumi_message(self, sandbox, msg):
        sandbox.send(SandboxCommand("vumi-message", msg=msg.payload))

    def dispatch_request(self, sandbox, command):
        handler_name = 'handle_%s' % (command['cmd'],)
        handler = getattr(self, handler_name, self.unknown_command)
        return handler(sandbox, command)

    # TODO: add commands

    def unknown_command(self, sandbox, command):
        # TODO: log.error expects a Failure instance
        log.error("Sandbox %r sent unknown command %r" % (sandbox, command))
        sandbox.kill()


class SandboxCommand(Message):
    def __init__(self, cmd, **kw):
        # TODO: add IDs for replies
        super(SandboxCommand, self).__init__(cmd=cmd, **kw)


class Sandbox(ApplicationWorker):
    """
    Configuration options:

    :param int timeout:
        Length of time the subprocess is given to process
        a message.
    """

    MB = 1024 * 1024
    DEFAULT_RLIMITS = {
        resource.RLIMIT_CORE: 1 * MB,
        resource.RLIMIT_CPU: 60,
        resource.RLIMIT_FSIZE: 1 * MB,
        resource.RLIMIT_DATA: 10 * MB,
        resource.RLIMIT_STACK: 1 * MB,
        resource.RLIMIT_RSS: 10 * MB,
        resource.RLIMIT_NPROC: 1,
        resource.RLIMIT_NOFILE: 10,
        #resource.RLIMIT_MEMLOCK: 10 * MB,  # this is higher than the default
        #resource.RLIMIT_VMEM: 10 * MB,
        resource.RLIMIT_AS: 10 * MB,
        }

    def validate_config(self):
        self.executable = self.config.get("executable")
        self.args = [self.executable] + self.config.get("args", [])
        self.path = self.config.get("path", None)
        self.timeout = int(self.config.get("timeout", "60"))
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
        return SandboxResources(config)

    def create_sandbox_api(self, msg):
        return SandboxApi(self.resources)

    def create_sandbox_protocol(self, api):
        return SandboxProtocol(api, self.rlimits, self.timeout)

    def process_in_sandbox(self, msg):
        api = self.create_sandbox_api(msg)
        sandbox_protocol = self.create_sandbox_protocol(api)
        sandbox_protocol.spawn(self.executable, args=self.args,
                               env={}, path=self.path)

        def on_start(_result):
            api.sandbox_init(sandbox_protocol)
            api.sandbox_vumi_message(sandbox_protocol, msg)
            return sandbox_protocol.done()

        d = sandbox_protocol.started()
        d.addCallback(on_start)
        return d

    def consume_user_message(self, msg):
        return self.process_in_sandbox(msg)

    def close_session(self, msg):
        return self.process_in_sandbox(msg)

    def consume_ack(self, event):
        return self.process_in_sandbox(event)

    def consume_delivery_report(self, event):
        return self.process_in_sandbox(event)


if __name__ == "__main__":
    rlimiter = SandboxProtocol.rlimiter(sys.argv, os.environ)
    rlimiter.execute()
