# -*- test-case-name: vumi.demos.tests.test_rps -*-

from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.application import ApplicationWorker


class MultiPlayerGameWorker(ApplicationWorker):

    @inlineCallbacks
    def startWorker(self):
        """docstring for startWorker"""
        self.games = {}
        self.open_game = None
        self.game_setup()
        self.messages = {}
        yield super(MultiPlayerGameWorker, self).startWorker()

    def new_session(self, data):
        log.msg("New session:", data)
        log.msg("Open game:", self.open_game)
        log.msg("Games:", self.games)
        session_id = data['session_id']
        self.messages[data['session_id']] = data
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
        game = self.games.get(data['session_id'])
        if game:
            if self.open_game == game:
                self.open_game = None
            self.clean_up_game(game)
            for sid, sgame in self.games.items():
                if game == sgame:
                    self.games.pop(sid, None)
                    msg = "Game terminated due to remote player disconnect."
                    self.end(sid, msg)
        self.messages.pop(data['session_id'], None)

    def consume_user_message(self, data):
        log.msg("Resume session:", data)
        self.messages[data['session_id']] = data
        session_id = data['session_id']
        if session_id not in self.games:
            return
        game = self.games[session_id]
        self.continue_game(game, session_id, data['content'])

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

    def reply(self, player, content):
        orig = self.messages.pop(player, None)
        if orig is None:
            log.msg("Can't reply to %s, no stored message.")
            return
        return self.reply_to(orig, content, continue_session=True)


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

    @inlineCallbacks
    def turn_reply(self, game):
        if game.check_win():
            if game.scores[0] > game.scores[1]:
                self.end(game.player_1, "You won! :-)")
                self.end(game.player_2, "You lost. :-(")
            else:
                self.end(game.player_1, "You lost. :-(")
                self.end(game.player_2, "You won! :-)")
            return
        yield self.reply(game.player_1, game.draw_board(game.player_1))
        yield self.reply(game.player_2, game.draw_board(game.player_2))
