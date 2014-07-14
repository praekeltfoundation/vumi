# -*- test-case-name: vumi.application.tests.test_sandbox_rlimiter -*-

"""NOTE:

    This module is also used as a standalone Python program that is executed by
    the sandbox machinery. It must never, ever import non-stdlib modules.
"""

import os
import sys
import json
import signal
import resource


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
        data = os.environ[self._SANDBOX_RLIMITS_]
        rlimits = json.loads(data) if data.strip() else {}
        for rlimit, (soft, hard) in rlimits.iteritems():
            # Cap our rlimits to the maximum allowed.
            rsoft, rhard = resource.getrlimit(int(rlimit))
            soft = min(soft, rsoft)
            hard = min(hard, rhard)
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
        self._apply_rlimits()
        self._restore_child_env(os.environ)
        self._sanitize_fds()
        self._reset_signals()
        os.execvpe(self._executable, self._args, self._env)

    _SANDBOX_RLIMITS_ = "_SANDBOX_RLIMITS_"

    @classmethod
    def _override_child_env(cls, env, rlimits):
        """Put RLIMIT config in the env."""

        env[cls._SANDBOX_RLIMITS_] = json.dumps(rlimits)

    @classmethod
    def _restore_child_env(cls, env):
        """Remove RLIMIT config."""
        del env[cls._SANDBOX_RLIMITS_]

    @classmethod
    def script_name(cls):
        # we need to pass Python the actual filename of this script
        # (rather than using -m __name__) so that is doesn't import
        # Twisted's reactor (since that causes errors when we close
        # all the file handles if using certain reactors).
        script_name = __file__
        if script_name.endswith('.pyc') or script_name.endswith('.pyo'):
            script_name = script_name[:-len('.pyc')] + '.py'
        return script_name

    @classmethod
    def spawn(cls, reactor, protocol, executable, rlimits, **kwargs):
        # spawns a SandboxRlimiter, connectionMade then passes the rlimits
        # through to stdin and the SandboxRlimiter applies them
        args = kwargs.pop('args', [])
        # the -u for unbuffered I/O is important (otherwise the process
        # execed will be very confused about where its stdin data has
        # gone)
        args = [sys.executable, '-u', cls.script_name(), '--'] + args
        env = kwargs.pop('env', {})
        cls._override_child_env(env, rlimits)
        reactor.spawnProcess(protocol, sys.executable, args=args, env=env,
                             **kwargs)


if __name__ == "__main__":
    rlimiter = SandboxRlimiter(sys.argv, os.environ)
    rlimiter.execute()
