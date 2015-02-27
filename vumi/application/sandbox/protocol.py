import logging

from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet.protocol import ProcessProtocol
from twisted.internet.error import ProcessDone
from twisted.python.failure import Failure

from vumi.application.sandbox.rlimiter import SandboxRlimiter
from vumi.application.sandbox.utils import SandboxError
from vumi.application.sandbox.resources.utils import SandboxCommand
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


class SandboxProtocol(ProcessProtocol):
    """A protocol for communicating over stdin and stdout with a sandboxed
    process.

    The sandbox process is created by calling :meth:`spawn`. This:

    * Spawns a new Python process that applies the supplied rlimits.
    * The spawned process then `execs` the supplied executable.

    Once a spawned process starts, the parent process communicates with
    it over `stdin`, `stdout` and `stderr` reading and writing a stream
    of newline separated JSON commands that are parsed and formatted by
    :class:`SandboxCommand`.

    Incoming commands are dispatched to :class:`SandboxResource` instances
    via the supplied :class:`SandboxApi`.
    """

    def __init__(self, sandbox_id, api, executable, spawn_kwargs,
                 rlimits, timeout, recv_limit):
        self.sandbox_id = sandbox_id
        self.api = api
        self.executable = executable
        self.spawn_kwargs = spawn_kwargs
        self.rlimits = rlimits
        self._started = MultiDeferred()
        self._done = MultiDeferred()
        self._pending_requests = []
        self.exit_reason = None
        self.timeout_task = reactor.callLater(timeout, self.kill)
        self.recv_limit = recv_limit
        self.recv_bytes = 0
        self.chunk = ''
        self.error_chunk = ''
        self.error_lines = []
        api.set_sandbox(self)

    def spawn(self):
        SandboxRlimiter.spawn(
            reactor, self, self.executable, self.rlimits, **self.spawn_kwargs)

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
            self.api.log("Sandbox %r killed for producing too much data on"
                         " stderr and stdout." % (self.sandbox_id),
                         level=logging.ERROR)
            return False

    def connectionMade(self):
        self._started.callback(self)

    def _process_data(self, chunk, data):
        if not self.check_recv(len(data)):
            return ['']  # skip the data if it's too big
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
            d = self.api.dispatch_request(self._parse_command(lines[i]))
            self._pending_requests.append(d)
        self.chunk = lines[-1]

    def outConnectionLost(self):
        if self.chunk:
            line, self.chunk = self.chunk, ""
            d = self.api.dispatch_request(self._parse_command(line))
            self._pending_requests.append(d)

    def errReceived(self, data):
        lines = self._process_data(self.error_chunk, data)
        for i in range(len(lines) - 1):
            self.error_lines.append(lines[i])
        self.error_chunk = lines[-1]

    def errConnectionLost(self):
        if self.error_chunk:
            self.error_lines.append(self.error_chunk)
            self.error_chunk = ""

    def _process_request_results(self, results):
        for success, result in results:
            if not success:
                # errors here are bugs in Vumi and thus should always
                # be logged via Twisted too.
                log.error(result)
                # we log them again in a simplified form via the sandbox
                # api so that the sandbox owner gets to see them too
                self.api.log(result.getErrorMessage(), logging.ERROR)

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
        if self.error_lines:
            self.api.log("\n".join(self.error_lines), logging.ERROR)
            self.error_lines = []
        requests_done = DeferredList(self._pending_requests)
        requests_done.addCallback(self._process_request_results)
        requests_done.addCallback(lambda _r: self._done.callback(result))
