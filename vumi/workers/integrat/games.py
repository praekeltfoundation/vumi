# -*- test-case-name: vumi.workers.integrat.tests.test_games -*-

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.python import log

from vumi.utils import (safe_routing_key, get_deploy_int,
                        http_request, normalize_msisdn)
from vumi.workers.integrat.worker import IntegratWorker

import redis
import string


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
            safe_routing_key(self.config['ussd_code']),
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


class HangmanGame(object):
    """Represents a game of Hangman.

       Parameters
       ----------
       word : str
           Word to guess.
       guesses : set, optional
           Characters guessed so far. If None, defaults to the empty set.
       msg : str, optional
           Message set in reply to last user action. Defaults to 'New game!'.
       """

    UI_TEMPLATE = \
        "%(msg)s\n" \
        "Word: %(word)s\n" \
        "Letters guessed so far: %(guesses)s\n" \
        "%(prompt)s (0 to quit):\n"

    # exit codes
    NOT_DONE, DONE, DONE_WANTS_NEW = range(3)

    def __init__(self, word, guesses=None, msg="New game!"):
        self.word = word
        self.guesses = guesses if guesses is not None else set()
        self.msg = msg
        self.exit_code = self.NOT_DONE

    def state(self):
        """Serialize the Hangman object to a string."""
        guesses = "".join(sorted(self.guesses))
        return "%s:%s:%s" % (self.word, guesses, self.msg)

    @classmethod
    def from_state(cls, state):
        word, guesses, msg = state.split(":", 2)
        guesses = set(guesses)
        return cls(word=word, guesses=guesses, msg=msg)

    def event(self, message):
        """Handle an user input string."""
        message = message.lower()
        if not message:
            self.msg = "Some input required please."
        elif len(message) > 1:
            self.msg = "Single characters only please."
        elif message == '0':
            self.exit_code = self.DONE
            self.msg = "Game ended."
        elif self.won():
            self.exit_code = self.DONE_WANTS_NEW
        elif message not in string.lowercase:
            self.msg = "Letters of the alphabet only please."
        elif message in self.guesses:
            self.msg = "You've already guessed %r." % (message,)
        else:
            assert len(message) == 1
            self.guesses.add(message)
            log.msg("Message: %r, word: %r" % (message, self.word))
            if message in self.word:
                self.msg = "Word contains at least one %r! :D" % (message,)
            else:
                self.msg = "Word contains no %r. :(" % (message,)

        if self.won():
            self.msg = self.victory_message()

    def victory_message(self):
        uniques = len(set(self.word))
        guesses = len(self.guesses)
        for factor, msg in [
            (1, "Flawless victory!"),
            (1.5, "Epic victory!"),
            (2, "Standard victory!"),
            (3, "Sub-par victory!"),
            (4, "Random victory!"),
            ]:
            if guesses <= uniques * factor:
                return msg
        return "Button mashing!"

    def won(self):
        return all(x in self.guesses for x in self.word)

    def draw_board(self):
        """Return a text-based UI."""
        if self.exit_code != self.NOT_DONE:
            return "Adieu!"
        word = "".join((x if x in self.guesses else '_') for x in self.word)
        guesses = "".join(sorted(self.guesses))
        if self.won():
            prompt = "Enter anything to start a new game"
        else:
            prompt = "Enter next guess"
        return self.UI_TEMPLATE % {'word': word,
                                   'guesses': guesses,
                                   'msg': self.msg,
                                   'prompt': prompt,
                                   }


class HangmanWorker(IntegratWorker):
    """Worker that plays Hangman.

       Configuration
       -------------
       transport_name : str
           Name of the transport.
       ussd_code : str
           USSD code.
       random_word_url : URL
           Page to GET a random word from.
           E.g. http://randomword.setgetgo.com/get.php
       """

    @inlineCallbacks
    def startWorker(self):
        """Start the worker"""
        # Connect to Redis
        self.r_server = redis.Redis("localhost",
                                    db=get_deploy_int(self._amqp_client.vhost))
        log.msg("Connected to Redis")
        self.r_prefix = "hangman:%s:%s" % (
                self.config['transport_name'],
                safe_routing_key(self.config['ussd_code']))
        log.msg("r_prefix = %s" % self.r_prefix)
        self.random_word_url = self.config['random_word_url']
        log.msg("random_word_url = %s" % self.random_word_url)

        yield super(HangmanWorker, self).startWorker()

    def random_word(self):
        log.msg('Fetching random word from %s' % (self.random_word_url,))
        d = http_request(self.random_word_url, None, method='GET')

        def _decode(word):
            if not isinstance(word, unicode):
                # Hack. :-(
                return word.decode('utf-8').strip(u'\ufeff')
            return word
        return d.addCallback(_decode)

    def game_key(self, msisdn):
        "Key for looking up a users game in data store."""
        msisdn = normalize_msisdn(msisdn)
        userid = msisdn.lstrip('+')
        return "%s#%s" % (self.r_prefix, userid)

    def load_game(self, msisdn):
        """Fetch a game for the given user ID.
           """
        game_key = self.game_key(msisdn)
        state = self.r_server.get(game_key)
        if state is not None:
            game = HangmanGame.from_state(state)
        else:
            game = None
        return game

    @inlineCallbacks
    def new_game(self, msisdn):
        """Create a new game for the given user ID.
           """
        word = yield self.random_word()
        word = word.strip().lower()
        game = HangmanGame(word)
        returnValue(game)

    def save_game(self, msisdn, game):
        """Save the game state for the given game."""
        game_key = self.game_key(msisdn)
        state = game.state()
        self.r_server.set(game_key, state)

    def delete_game(self, msisdn):
        """Delete the users saved game."""
        game_key = self.game_key(msisdn)
        self.r_server.delete(game_key)

    @inlineCallbacks
    def new_session(self, data):
        """Find or creating hangman game for this player.

           Sends current state.
           """
        log.msg("New session:", data)
        session_id = data['transport_session_id']
        msisdn = data['sender']
        game = self.load_game(msisdn)
        if game is None:
            game = yield self.new_game(msisdn)
            self.save_game(msisdn, game)
        self.reply(session_id, game.draw_board())

    def close_session(self, data):
        # Hangman games intentionally stick around
        # and can be picked up again later.
        pass

    @inlineCallbacks
    def resume_session(self, data):
        log.msg("Resume session:", data)
        session_id = data['transport_session_id']
        msisdn = data['sender']
        message = data['message'].strip()
        game = self.load_game(msisdn)
        game.event(message)
        if game.exit_code == game.DONE:
            self.delete_game(msisdn)
            self.end(session_id, game.draw_board())
        elif game.exit_code == game.DONE_WANTS_NEW:
            game = yield self.new_game(msisdn)
            self.save_game(msisdn, game)
            self.reply(session_id, game.draw_board())
        else:
            self.save_game(msisdn, game)
            self.reply(session_id, game.draw_board())
