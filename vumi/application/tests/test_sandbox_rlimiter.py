"""Tests for vumi.application.sandbox_rlimiter."""

from twisted.trial.unittest import TestCase

from vumi.application import sandbox_rlimiter
from vumi.application.sandbox_rlimiter import SandboxRlimiter


class SandboxRlimiterTestCase(TestCase):
    def test_script_name_dot_py(self):
        self.patch(sandbox_rlimiter, '__file__', 'foo.py')
        self.assertEqual(SandboxRlimiter.script_name(), 'foo.py')

    def test_script_name_dot_pyc(self):
        self.patch(sandbox_rlimiter, '__file__', 'foo.pyc')
        self.assertEqual(SandboxRlimiter.script_name(), 'foo.py')

    def test_script_name_dot_pyo(self):
        self.patch(sandbox_rlimiter, '__file__', 'foo.pyo')
        self.assertEqual(SandboxRlimiter.script_name(), 'foo.py')
