
from twisted.trial import unittest
from vumi.workers.smpp.service import *

class DummySMSCTests(unittest.TestCase):
    def test_launch(self):
        """A test that should always pass"""
        self.assertEqual(0, 0)
