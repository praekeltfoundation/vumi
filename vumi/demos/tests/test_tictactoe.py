from twisted.trial import unittest

from vumi.demos.tictactoe import TicTacToeGame, TicTacToeWorker


class TestTicTacToeGame(unittest.TestCase):
    def get_game(self, moves=()):
        game = TicTacToeGame('pX')
        game.set_player_O('pO')
        for sid, x, y in moves:
            game.move(sid, x, y)
        return game

    def test_game_init(self):
        game = TicTacToeGame('pX')
        self.assertEquals('pX', game.player_X)
        self.assertEquals(None, game.player_O)
        self.assertEquals([[' '] * 3] * 3, game.board)

        game.set_player_O('pO')
        self.assertEquals('pX', game.player_X)
        self.assertEquals('pO', game.player_O)
        self.assertEquals([[' '] * 3] * 3, game.board)

    def test_move(self):
        game = self.get_game()
        expected_board = [[' ', ' ', ' '] for _i in range(3)]

        self.assertEqual((True, 'pO'), game.move('pX', 0, 0))
        expected_board[0][0] = 'X'
        self.assertEqual(expected_board, game.board)

        self.assertEqual((True, 'pX'), game.move('pO', 1, 0))
        expected_board[0][1] = 'O'
        self.assertEqual(expected_board, game.board)

        self.assertEqual((False, 'pO'), game.move('pX', 0, 0))
        self.assertEqual(expected_board, game.board)

    def test_draw_board(self):
        game = self.get_game(moves=[('pX', 0, 0), ('pO', 1, 0), ('pX', 1, 2)])
        self.assertEqual("+---+---+---+\n"
                         "| X | O |   |\n"
                         "+---+---+---+\n"
                         "|   |   |   |\n"
                         "+---+---+---+\n"
                         "|   | X |   |\n"
                         "+---+---+---+",
                         game.draw_board())

    def test_check_draw(self):
        game = self.get_game()
        for y, x in [(y, x) for x in range(3) for y in range(3)]:
            self.assertEqual(False, game.check_draw())
            game.move('pX' if x + y % 2 == 0 else 'pO', x, y)
        self.assertEqual(True, game.check_draw())

    def test_check_win(self):
        game = self.get_game()
        for i, (y, x) in enumerate([(0, 0), (1, 0),
                                    (1, 2), (1, 1),
                                    (0, 2), (0, 1),
                                    (2, 2)]):
            self.assertEqual(False, game.check_win())
            game.move('pX' if i % 2 == 0 else 'pO', x, y)
        self.assertEqual('X', game.check_win())


class TestTicTacToeWorker(unittest.TestCase):
    def test_fail(self):
        raise unittest.SkipTest("No tests for %r." % TicTacToeGame)
