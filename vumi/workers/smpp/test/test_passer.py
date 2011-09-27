
from twisted.trial import unittest


class TestTests(unittest.TestCase):
    def test_passer(self):
        """A test that should always pass"""
        self.assertEqual(0, 0)
