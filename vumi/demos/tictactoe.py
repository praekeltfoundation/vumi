# -*- test-case-name: vumi.demos.tests.test_tictactoe -*-

"""Simple Tic Tac Toe game."""

from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.application import ApplicationWorker
from vumi.utils import safe_routing_key


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
        self.games = {}
        self.open_game = None
        self.publisher = yield self.publish_to(
            'ussd.outbound.%(transport_name)s' % self.config)
        self.consumer = yield self.consume('ussd.inbound.%s.%s' % (
            self.config['transport_name'],
            safe_routing_key(self.config['ussd_code'])
        ), self.consume_message)

    def new_session(self, data):
        log.msg("New session:", data)
        log.msg("Open game:", self.open_game)
        log.msg("Games:", self.games)
        session_id = data['transport_session_id']
        if self.open_game:
            game = self.open_game
            game.set_player_O(session_id)
            self.open_game = None
            self.reply(game.player_X, game.draw_board())
        else:
            game = TicTacToeGame(session_id)
            self.open_game = game
        self.games[session_id] = game

    def close_session(self, data):
        log.msg("Close session:", data)
        game = self.games.get(data['transport_session_id'])
        if game:
            if self.open_game == game:
                self.open_game = None
            for sid in (game.player_X, game.player_O):
                if sid is not None:
                    self.games.pop(sid, None)
                    self.end(sid, "Other side timed out.")

    def resume_session(self, data):
        log.msg("Resume session:", data)
        session_id = data['transport_session_id']
        if session_id not in self.games:
            return
        game = self.games[session_id]
        move = self.parse_move(data['message'])
        if move is None:
            self.end(game.player_X, "Cheerio.")
            self.end(game.player_O, "Cheerio.")
            return
        log.msg("Move:", move)
        resp, other_sid = game.move(session_id, *move)

        if game.check_win():
            self.end(session_id, "You won!")
            self.end(other_sid, "You lost!")
            return

        if game.check_draw():
            self.end(session_id, "Draw. :-(")
            self.end(other_sid, "Draw. :-(")
            return

        self.reply(other_sid, game.draw_board())

    def parse_move(self, move):
        moves = {
            '1': (0, 0), '2': (1, 0), '3': (2, 0),
            '4': (0, 1), '5': (1, 1), '6': (2, 1),
            '7': (0, 2), '8': (1, 2), '9': (2, 2),
            }
        if move[0] in moves:
            return moves[move[0]]
        return None
