# -*- test-case-name: vumi.demos.tests.test_tictactoe -*-

"""Simple Tic Tac Toe game."""

from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.application import ApplicationWorker


class TicTacToeGame(object):
    def __init__(self, player_X):
        self.board = [
            [' ', ' ', ' '],
            [' ', ' ', ' '],
            [' ', ' ', ' '],
            ]
        self.player_X = player_X
        self.player_O = None

    def set_player_O(self, player_O):
        self.player_O = player_O

    def draw_line(self):
        return "+---+---+---+"

    def draw_row(self, row):
        return '| %s | %s | %s |' % tuple(row)

    def draw_board(self):
        return '\n'.join(['\n'.join([self.draw_line(), self.draw_row(r)])
                          for r in self.board] + [self.draw_line()])

    def _move(self, val, x, y):
        if self.board[y][x] != ' ':
            return False
        self.board[y][x] = val
        return True

    def move(self, sid, x, y):
        if sid == self.player_X:
            return self._move('X', x, y), self.player_O
        if sid == self.player_O:
            return self._move('O', x, y), self.player_X

    def check_line(self, v1, v2, v3):
        if (v1 != ' ') and (v1 == v2) and (v1 == v3):
            return v1
        return False

    def check_win(self):
        for l in [((0, 0), (0, 1), (0, 2)),
                  ((1, 0), (1, 1), (1, 2)),
                  ((2, 0), (2, 1), (2, 2)),
                  ((0, 0), (1, 0), (2, 0)),
                  ((0, 1), (1, 1), (2, 1)),
                  ((0, 2), (1, 2), (2, 2)),
                  ((0, 0), (1, 1), (2, 2)),
                  ((0, 2), (1, 1), (2, 0))]:
            ll = [self.board[y][x] for x, y in l]
            result = self.check_line(*ll)
            if result:
                return result
        return False

    def check_draw(self):
        for x in [0, 1, 2]:
            for y in [0, 1, 2]:
                if self.board[y][x] == ' ':
                    return False
        return True


class TicTacToeWorker(ApplicationWorker):

    @inlineCallbacks
    def startWorker(self):
        """docstring for startWorker"""
        yield super(TicTacToeWorker, self).startWorker()
        self.games = {}
        self.open_game = None
        self.messages = {}

    def reply(self, player, content, continue_session=True):
        orig = self.messages.pop(player, None)
        if orig is None:
            log.msg("Can't reply to %r, no stored message." % player)
            return
        return self.reply_to(orig, content, continue_session=continue_session)

    def end(self, player, content):
        return self.reply(player, content, continue_session=False)

    def new_session(self, msg):
        log.msg("New session:", msg)
        log.msg("Open game:", self.open_game)
        log.msg("Games:", self.games)
        user_id = msg.user()
        self.messages[user_id] = msg
        if self.open_game:
            game = self.open_game
            game.set_player_O(user_id)
            self.open_game = None
            self.reply(game.player_X, game.draw_board())
        else:
            game = TicTacToeGame(user_id)
            self.open_game = game
        self.games[user_id] = game

    def close_session(self, msg):
        log.msg("Close session:", msg)
        user_id = msg.user()
        game = self.games.get(user_id)
        if game:
            if self.open_game == game:
                self.open_game = None
            for uid in (game.player_X, game.player_O):
                if uid is not None:
                    self.games.pop(uid, None)
                    self.end(uid, "Other side timed out.")

    def consume_user_message(self, msg):
        log.msg("Resume session:", msg)
        user_id = msg.user()
        self.messages[user_id] = msg
        if user_id not in self.games:
            return
        game = self.games[user_id]
        move = self.parse_move(msg['content'])
        if move is None:
            self.end(game.player_X, "Cheerio.")
            self.end(game.player_O, "Cheerio.")
            return
        log.msg("Move:", move)
        resp, other_uid = game.move(user_id, *move)

        if game.check_win():
            self.end(user_id, "You won!")
            self.end(other_uid, "You lost!")
            return

        if game.check_draw():
            self.end(user_id, "Draw. :-(")
            self.end(other_uid, "Draw. :-(")
            return

        self.reply(other_uid, game.draw_board())

    def parse_move(self, move):
        moves = {
            '1': (0, 0), '2': (1, 0), '3': (2, 0),
            '4': (0, 1), '5': (1, 1), '6': (2, 1),
            '7': (0, 2), '8': (1, 2), '9': (2, 2),
            }
        if move[0] in moves:
            return moves[move[0]]
        return None
