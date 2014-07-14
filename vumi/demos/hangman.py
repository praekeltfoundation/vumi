# -*- test-case-name: vumi.demos.tests.test_hangman -*-

import string

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.python import log

from vumi.application import ApplicationWorker
from vumi.utils import http_request
from vumi.components.session import SessionManager
from vumi.config import ConfigText, ConfigDict


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

    UI_TEMPLATE = (
        u"%(msg)s\n"
        u"Word: %(word)s\n"
        u"Letters guessed so far: %(guesses)s\n"
        u"%(prompt)s (0 to quit):\n")

    # exit codes
    NOT_DONE, DONE, DONE_WANTS_NEW = range(3)

    def __init__(self, word, guesses=None, msg="New game!"):
        self.word = word
        self.guesses = guesses if guesses is not None else set()
        self.msg = msg
        self.exit_code = self.NOT_DONE

    def state(self):
        """Return the game state as a dict."""
        return {
            'guesses': u"".join(sorted(self.guesses)),
            'word': self.word,
            'msg': self.msg,
            }

    @classmethod
    def from_state(cls, state):
        return cls(word=state['word'], guesses=set(state['guesses']),
                   msg=state['msg'])

    def event(self, message):
        """Handle an user input string.

           Parameters
           ----------
           message : unicode
               Message received from user.
           """
        message = message.lower()
        if not message:
            self.msg = u"Some input required please."
        elif len(message) > 1:
            self.msg = u"Single characters only please."
        elif message == '0':
            self.exit_code = self.DONE
            self.msg = u"Game ended."
        elif self.won():
            self.exit_code = self.DONE_WANTS_NEW
        elif message not in string.lowercase:
            self.msg = u"Letters of the alphabet only please."
        elif message in self.guesses:
            self.msg = u"You've already guessed '%s'." % (message,)
        else:
            assert len(message) == 1
            self.guesses.add(message)
            log.msg("Message: %r, word: %r" % (message, self.word))
            if message in self.word:
                self.msg = u"Word contains at least one '%s'! :D" % (message,)
            else:
                self.msg = u"Word contains no '%s'. :(" % (message,)

        if self.won():
            self.msg = self.victory_message()

    def victory_message(self):
        uniques = len(set(self.word))
        guesses = len(self.guesses)
        for factor, msg in [
                (1, u"Flawless victory!"),
                (1.5, u"Epic victory!"),
                (2, u"Standard victory!"),
                (3, u"Sub-par victory!"),
                (4, u"Random victory!")]:
            if guesses <= uniques * factor:
                return msg
        return u"Button mashing!"

    def won(self):
        return all(x in self.guesses for x in self.word)

    def draw_board(self):
        """Return a text-based UI."""
        if self.exit_code != self.NOT_DONE:
            return u"Adieu!"
        word = u"".join((x if x in self.guesses else '_') for x in self.word)
        guesses = "".join(sorted(self.guesses))
        if self.won():
            prompt = u"Enter anything to start a new game"
        else:
            prompt = u"Enter next guess"
        return self.UI_TEMPLATE % {'word': word,
                                   'guesses': guesses,
                                   'msg': self.msg,
                                   'prompt': prompt,
                                   }


class HangmanConfig(ApplicationWorker.CONFIG_CLASS):
    "Hangman worker config."
    worker_name = ConfigText(
        "Name of this hangman worker.", required=True, static=True)
    redis_manager = ConfigDict(
        "Redis client configuration.", default={}, static=True)

    random_word_url = ConfigText(
        "URL to GET a random word from.", required=True)


class HangmanWorker(ApplicationWorker):
    """Worker that plays Hangman.
       """

    CONFIG_CLASS = HangmanConfig

    @inlineCallbacks
    def setup_application(self):
        """Start the worker"""
        config = self.get_static_config()
        # Connect to Redis
        r_prefix = "hangman_game:%s:%s" % (
            config.transport_name, config.worker_name)
        self.session_manager = yield SessionManager.from_redis_config(
            config.redis_manager, r_prefix)

    @inlineCallbacks
    def teardown_application(self):
        yield self.session_manager.stop()

    def random_word(self, random_word_url):
        log.msg('Fetching random word from %s' % (random_word_url,))
        d = http_request(random_word_url, None, method='GET')

        def _decode(word):
            # result from http_request should always be bytes
            # convert to unicode, strip BOMs and whitespace
            word = word.decode("utf-8", "ignore")
            word = word.lstrip(u'\ufeff\ufffe')
            word = word.strip()
            return word
        return d.addCallback(_decode)

    def game_key(self, user_id):
        """Key for looking up a users game in data store."""
        return user_id.lstrip('+')

    @inlineCallbacks
    def load_game(self, msisdn):
        """Fetch a game for the given user ID.
           """
        game_key = self.game_key(msisdn)
        state = yield self.session_manager.load_session(game_key)
        if state:
            game = HangmanGame.from_state(state)
        else:
            game = None
        returnValue(game)

    @inlineCallbacks
    def new_game(self, msisdn, random_word_url):
        """Create a new game for the given user ID.
           """
        word = yield self.random_word(random_word_url)
        word = word.strip().lower()
        game = HangmanGame(word)
        game_key = self.game_key(msisdn)
        yield self.session_manager.create_session(game_key, **game.state())
        returnValue(game)

    def save_game(self, msisdn, game):
        """Save the game state for the given game."""
        game_key = self.game_key(msisdn)
        state = game.state()
        return self.session_manager.save_session(game_key, state)

    def delete_game(self, msisdn):
        """Delete the users saved game."""
        game_key = self.game_key(msisdn)
        return self.session_manager.clear_session(game_key)

    @inlineCallbacks
    def consume_user_message(self, msg):
        """Find or create a hangman game for this player.

        Then process the user's message.
        """
        log.msg("User message: %s" % msg['content'])

        user_id = msg.user()
        config = yield self.get_config(msg)
        game = yield self.load_game(user_id)
        if game is None:
            game = yield self.new_game(user_id, config.random_word_url)

        if msg['content'] is None:
            # probably new session -- just send the user the board
            self.reply_to(msg, game.draw_board(), True)
            return

        message = msg['content'].strip()
        game.event(message)

        continue_session = True
        if game.exit_code == game.DONE:
            yield self.delete_game(user_id)
            continue_session = False
        elif game.exit_code == game.DONE_WANTS_NEW:
            game = yield self.new_game(user_id, config.random_word_url)
        else:
            yield self.save_game(user_id, game)

        self.reply_to(msg, game.draw_board(), continue_session)

    def close_session(self, msg):
        """We ignore session closing and wait for the user to return."""
        pass
