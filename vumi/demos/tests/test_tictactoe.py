from twisted.trial import unittest

from vumi.demos.tictactoe import TicTacToeGame, TicTacToeWorker


class TestTicTacToeGame(unittest.TestCase):
    def test_fail(self):
        raise unittest.SkipTest("No tests for %r." % TicTacToeGame)


class TestTicTacToeWorker(unittest.TestCase):
    def test_fail(self):
        raise unittest.SkipTest("No tests for %r." % TicTacToeGame)
