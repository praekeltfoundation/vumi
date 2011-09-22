from twisted.trial import unittest

from vumi.demos.tictactoe import TicTacToeGame, TicTacToeWorker


class TestTicTacToeGame(unittest.TestCase):
    def test_fail(self):
        self.fail("No tests for %r." % TicTacToeGame)


class TestTicTacToeWorker(unittest.TestCase):
    def test_fail(self):
        self.fail("No tests for %r." % TicTacToeWorker)
