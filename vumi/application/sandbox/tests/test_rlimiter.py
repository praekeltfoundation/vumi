"""Tests for vumi.application.sandbox_rlimiter."""

from vumi.application.sandbox import rlimiter
from vumi.application.sandbox.rlimiter import SandboxRlimiter
from vumi.tests.helpers import VumiTestCase


class TestSandboxRlimiter(VumiTestCase):
    def test_script_name_dot_py(self):
        self.patch(rlimiter, '__file__', 'foo.py')
        self.assertEqual(SandboxRlimiter.script_name(), 'foo.py')

    def test_script_name_dot_pyc(self):
        self.patch(rlimiter, '__file__', 'foo.pyc')
        self.assertEqual(SandboxRlimiter.script_name(), 'foo.py')

    def test_script_name_dot_pyo(self):
        self.patch(rlimiter, '__file__', 'foo.pyo')
        self.assertEqual(SandboxRlimiter.script_name(), 'foo.py')
