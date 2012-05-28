# -*- test-case-name: vumi.application.tests.test_sandbox -*-

"""An application for sandboxing message processing."""

from twisted.internet import reactor
from twisted.internet.protocol import ProcessProtocol
from twisted.internet.defer import Deferred
from twisted.internet.error import ProcessDone
from twisted.python.failure import Failure

from vumi.application.base import ApplicationWorker
from vumi.message import Message
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


class SandboxProtocol(ProcessProtocol):

    def __init__(self, api, timeout, recv_limit=1024 * 1024):
        self.api = api
        self._started = MultiDeferred()
        self._done = MultiDeferred()
        self.exit_reason = None
        self.timeout_task = reactor.callLater(timeout, self.kill)
        self.recv_limit = recv_limit
        self.recv_bytes = 0
        self.chunk = ''

    def spawn(self, *args, **kwargs):
        # TODO: spawn process which sets resource limits and then calls
        #       executable instead
        reactor.spawnProcess(self, *args, **kwargs)

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

    def connectionMade(self):
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

    def validate_config(self):
        self.executable = self.config.get("executable")
        self.args = [self.executable] + self.config.get("args", [])
        self.path = self.config.get("path", None)
        self.timeout = int(self.config.get("timeout", "60"))
        self.resources = self.create_sandbox_resources(
            self.config.get('sandbox', {}))
        self.resources.validate_config()

    def setup_application(self):
        return self.resources.setup_resources()

    def teardown_application(self):
        return self.resources.teardown_resources()

    def create_sandbox_resources(self, config):
        return SandboxResources(config)

    def create_sandbox_api(self, msg):
        return SandboxApi(self.resources)

    def create_sandbox_protocol(self, api):
        return SandboxProtocol(api, self.timeout)

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
