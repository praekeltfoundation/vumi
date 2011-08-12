# -*- test-case-name: vumi.workers.integrat.tests.test_games -*-

from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.utils import safe_routing_key
from vumi.workers.integrat.worker import IntegratWorker


class MultiPlayerGameWorker(IntegratWorker):

    @inlineCallbacks
    def startWorker(self):
        """docstring for startWorker"""
        self.games = {}
        self.open_game = None
        self.game_setup()
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
            if not self.add_player_to_game(game, session_id):
                self.open_game = None
        else:
            game = self.create_new_game(session_id)
            self.open_game = game
        self.games[session_id] = game

    def close_session(self, data):
        log.msg("Close session:", data)
        game = self.games.get(data['transport_session_id'])
        if game:
            if self.open_game == game:
                self.open_game = None
            self.clean_up_game(game)
            for sid, sgame in self.games.items():
                if game == sgame:
                    self.games.pop(sid, None)
                    msg = "Game terminated due to remote player disconnect."
                    self.end(sid, msg)

    def resume_session(self, data):
        log.msg("Resume session:", data)
        session_id = data['transport_session_id']
        if session_id not in self.games:
            return
        game = self.games[session_id]
        self.continue_game(game, session_id, data['message'])

    def game_setup(self):
        pass

    def create_new_game(self, session_id):
        pass

    def add_player_to_game(self, game, session_id):
        pass

    def clean_up_game(self, game):
        pass

    def continue_game(self, game, session_id, message):
        pass


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


class TicTacToeWorker(IntegratWorker):

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


class RockPaperScissorsGame(object):
    def __init__(self, best_of, player_1):
        self.best_of = best_of
        self.player_1 = player_1
        self.player_2 = None
        self.current_move = None
        self.scores = (0, 0)
        self.last_result = None

    def set_player_2(self, player_2):
        self.player_2 = player_2

    def get_other_player(self, sid):
        if sid == self.player_1:
            return self.player_2
        return self.player_1

    def draw_board(self, sid):
        scores = self.scores
        if sid == self.player_2:
            scores = tuple(reversed(scores))
        result = []
        if self.last_result == (0, 0):
            result.append("Draw.")
        elif self.last_result == (1, 0):
            if sid == self.player_1:
                result.append("You won!")
            else:
                result.append("You lost!")
        elif self.last_result == (0, 1):
            if sid == self.player_1:
                result.append("You lost!")
            else:
                result.append("You won!")
        result.extend([
                'You: %s, opponent: %s' % scores,
                '1. rock',
                '2. paper',
                '3. scissors',
                ])
        return '\n'.join(result)

    def move(self, sid, choice):
        if not self.current_move:
            self.current_move = choice
            return False
        if sid == self.player_1:
            player_1, player_2 = choice, self.current_move
        else:
            player_1, player_2 = self.current_move, choice
        self.current_move = None
        result = self.decide(player_1, player_2)
        self.last_result = result
        self.scores = (
            self.scores[0] + result[0],
            self.scores[1] + result[1],
            )
        return True

    def decide(self, player_1, player_2):
        return {
            (1, 1): (0, 0),
            (1, 2): (1, 0),
            (1, 3): (0, 1),
            (2, 1): (0, 1),
            (2, 2): (0, 0),
            (2, 3): (1, 0),
            (3, 1): (1, 0),
            (3, 2): (0, 1),
            (3, 3): (0, 0),
            }[(player_1, player_2)]

    def check_win(self):
        return sum(self.scores) >= self.best_of


class RockPaperScissorsWorker(MultiPlayerGameWorker):

    def create_new_game(self, session_id):
        return RockPaperScissorsGame(5, session_id)

    def add_player_to_game(self, game, session_id):
        game.set_player_2(session_id)
        self.turn_reply(game)
        return False

    def clean_up_game(self, game):
        pass

    def continue_game(self, game, session_id, message):
        move = self.parse_move(message)
        if move is None:
            self.end(session_id, 'You disconnected.')
            self.end(game.get_other_player(session_id),
                     'Your opponent disconnected.')
            return
        if game.move(session_id, move):
            self.turn_reply(game)

    def parse_move(self, message):
        char = (message + ' ')[0]
        if char in '123':
            return int(char)
        return None

    def turn_reply(self, game):
        if game.check_win():
            if game.scores[0] > game.scores[1]:
                self.end(game.player_1, "You won! :-)")
                self.end(game.player_2, "You lost. :-(")
            else:
                self.end(game.player_1, "You lost. :-(")
                self.end(game.player_2, "You won! :-)")
            return
        self.reply(game.player_1, game.draw_board(game.player_1))
        self.reply(game.player_2, game.draw_board(game.player_2))
