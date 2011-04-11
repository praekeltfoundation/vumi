
from twisted.trial import unittest

class PassingTest(unittest.TestCase):
    def testPasser(self):
        """A test that should always pass"""
        self.assertEqual(0, 0)

