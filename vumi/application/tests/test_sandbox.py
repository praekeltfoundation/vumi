"""Tests for vumi.application.sandbox."""

from vumi.application.tests.test_base import ApplicationTestCase
from vumi.application.sandbox import Sandbox


class SandboxTestCase(ApplicationTestCase):

    application_class = Sandbox
