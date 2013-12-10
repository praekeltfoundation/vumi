from twisted.internet.defer import inlineCallbacks

from vumi.demos.tictactoe import TicTacToeGame, TicTacToeWorker
from vumi.message import TransportUserMessage
from vumi.application.tests.helpers import ApplicationHelper
from vumi.tests.helpers import VumiTestCase


class TestTicTacToeGame(VumiTestCase):
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


class TestTicTacToeWorker(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.app_helper = self.add_helper(ApplicationHelper(TicTacToeWorker))
        self.worker = yield self.app_helper.get_application({})

    def dispatch_start_message(self, from_addr):
        return self.app_helper.make_dispatch_inbound(
            None, from_addr=from_addr,
            session_event=TransportUserMessage.SESSION_NEW)

    @inlineCallbacks
    def test_new_sessions(self):
        self.assertEquals({}, self.worker.games)
        self.assertEquals(None, self.worker.open_game)

        user1 = '+27831234567'
        user2 = '+27831234568'

        yield self.dispatch_start_message(user1)
        self.assertNotEquals(None, self.worker.open_game)
        game = self.worker.open_game
        self.assertEquals({user1: game}, self.worker.games)

        yield self.dispatch_start_message(user2)
        self.assertEquals(None, self.worker.open_game)
        self.assertEquals({user1: game, user2: game}, self.worker.games)

        [msg] = self.app_helper.get_dispatched_outbound()
        self.assertTrue(msg['content'].startswith('+---+---+---+'))

    @inlineCallbacks
    def test_moves(self):
        user1 = '+27831234567'
        user2 = '+27831234568'

        yield self.dispatch_start_message(user1)
        game = self.worker.open_game
        yield self.dispatch_start_message(user2)
        self.assertEquals(1, len(self.app_helper.get_dispatched_outbound()))

        yield self.app_helper.make_dispatch_inbound('1', from_addr=user1)
        self.assertEquals(2, len(self.app_helper.get_dispatched_outbound()))

        yield self.app_helper.make_dispatch_inbound('2', from_addr=user2)
        self.assertEquals(3, len(self.app_helper.get_dispatched_outbound()))

        self.assertEqual('X', game.board[0][0])
        self.assertEqual('O', game.board[0][1])

    @inlineCallbacks
    def test_full_game(self):
        user1 = '+27831234567'
        user2 = '+27831234568'
        yield self.dispatch_start_message(user1)
        game = self.worker.open_game
        yield self.dispatch_start_message(user2)

        for user, content in [
                (user1, '1'), (user2, '4'),
                (user1, '2'), (user2, '5'),
                (user1, '3')]:
            yield self.app_helper.make_dispatch_inbound(
                content, from_addr=user)

        self.assertEqual('X', game.check_win())
        [end1, end2] = self.app_helper.get_dispatched_outbound()[-2:]
        self.assertEqual(user1, end1["to_addr"])
        self.assertEqual("You won!", end1["content"])
        self.assertEqual(user2, end2["to_addr"])
        self.assertEqual("You lost!", end2["content"])
