# -*- encoding: utf-8 -*-

"""Tests for vumi.demos.hangman."""

import string

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.web.static import Data

from vumi.demos.hangman import HangmanGame, HangmanWorker
from vumi.message import TransportUserMessage
from vumi.application.tests.helpers import ApplicationHelper
from vumi.tests.helpers import VumiTestCase


def mkstate(word, guesses, msg):
    return {'word': word, 'guesses': guesses, 'msg': msg}


class TestHangmanGame(VumiTestCase):
    def test_easy_game(self):
        game = HangmanGame(word='moo')
        game.event('m')
        game.event('o')
        self.assertTrue(game.won())
        self.assertEqual(
            game.state(), mkstate('moo', 'mo', 'Flawless victory!'))

    def test_incorrect_guesses(self):
        game = HangmanGame(word='moo')
        game.event('f')
        game.event('g')
        self.assertFalse(game.won())
        self.assertEqual(
            game.state(), mkstate('moo', 'fg', "Word contains no 'g'. :("))

    def test_repeated_guesses(self):
        game = HangmanGame(word='moo')
        game.event('f')
        game.event('f')
        self.assertFalse(game.won())
        self.assertEqual(
            game.state(), mkstate('moo', 'f', "You've already guessed 'f'."))

    def test_button_mashing(self):
        game = HangmanGame(word='moo')
        for event in string.lowercase.replace('o', ''):
            game.event(event)
        game.event('o')
        self.assertTrue(game.won())
        self.assertEqual(
            game.state(), mkstate('moo', string.lowercase, "Button mashing!"))

    def test_new_game(self):
        game = HangmanGame(word='moo')
        for event in ('m', 'o', '-'):
            game.event(event)
        self.assertEqual(
            game.state(), mkstate('moo', 'mo', 'Flawless victory!'))
        self.assertEqual(game.exit_code, game.DONE_WANTS_NEW)

    def test_from_state(self):
        game = HangmanGame.from_state(mkstate("bar", "xyz", "Eep?"))
        self.assertEqual(game.word, "bar")
        self.assertEqual(game.guesses, set("xyz"))
        self.assertEqual(game.msg, "Eep?")
        self.assertEqual(game.exit_code, game.NOT_DONE)

    def test_from_state_non_ascii(self):
        game = HangmanGame.from_state(
            mkstate("b\xc3\xa4r".decode("utf-8"), "xyz", "Eep?"))
        self.assertEqual(game.word, u"b\u00e4r")
        self.assertEqual(game.guesses, set("xyz"))
        self.assertEqual(game.msg, "Eep?")
        self.assertEqual(game.exit_code, game.NOT_DONE)

    def test_exit(self):
        game = HangmanGame('elephant')
        game.event('0')
        self.assertEqual(game.exit_code, game.DONE)
        self.assertEqual(game.draw_board(), "Adieu!")

    def test_draw_board(self):
        game = HangmanGame('word')
        board = game.draw_board()
        msg, word, guesses, prompt, end = board.split("\n")
        self.assertEqual(msg, "New game!")
        self.assertEqual(word, "Word: ____")
        self.assertEqual(guesses, "Letters guessed so far: ")
        self.assertEqual(prompt, "Enter next guess (0 to quit):")

    def test_draw_board_at_end_of_game(self):
        game = HangmanGame('m')
        game.event('m')
        board = game.draw_board()
        msg, word, guesses, prompt, end = board.split("\n")
        self.assertEqual(msg, "Flawless victory!")
        self.assertEqual(word, "Word: m")
        self.assertEqual(guesses, "Letters guessed so far: m")
        self.assertEqual(prompt, "Enter anything to start a new game"
                                 " (0 to quit):")

    def test_displaying_word(self):
        game = HangmanGame('word')
        game.event('w')
        game.event('r')
        board = game.draw_board()
        _msg, word, _guesses, _prompt, _end = board.split("\n")
        self.assertEqual(word, "Word: w_r_")

    def test_displaying_guesses(self):
        game = HangmanGame('word')
        game.event('w')
        board = game.draw_board()
        msg, _word, _guesses, _prompt, _end = board.split("\n")
        self.assertEqual(msg, "Word contains at least one 'w'! :D")

        game.event('w')
        board = game.draw_board()
        msg, _word, _guesses, _prompt, _end = board.split("\n")
        self.assertEqual(msg, "You've already guessed 'w'.")

        game.event('x')
        board = game.draw_board()
        msg, _word, _guesses, _prompt, _end = board.split("\n")
        self.assertEqual(msg, "Word contains no 'x'. :(")

    def test_garbage_input(self):
        game = HangmanGame(word="zoo")
        for garbage in [":", "!", "\x00", "+", "abc", ""]:
            game.event(garbage)
        self.assertEqual(game.guesses, set())
        game.event('z')
        game.event('o')
        self.assertTrue(game.won())


class TestHangmanWorker(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        root = Resource()
        # data is elephant with a UTF-8 encoded BOM
        # it is a sad elephant (as seen in the wild)
        root.putChild("word", Data('\xef\xbb\xbfelephant\r\n', 'text/html'))
        site_factory = Site(root)
        self.webserver = yield reactor.listenTCP(
            0, site_factory, interface='127.0.0.1')
        self.add_cleanup(self.webserver.loseConnection)
        addr = self.webserver.getHost()
        random_word_url = "http://%s:%s/word" % (addr.host, addr.port)

        self.app_helper = self.add_helper(ApplicationHelper(HangmanWorker))

        self.worker = yield self.app_helper.get_application({
            'worker_name': 'test_hangman',
            'random_word_url': random_word_url,
        })
        yield self.worker.session_manager.redis._purge_all()  # just in case

    def send(self, content, session_event=None):
        return self.app_helper.make_dispatch_inbound(
            content, session_event=session_event)

    @inlineCallbacks
    def recv(self, n=0):
        msgs = yield self.app_helper.wait_for_dispatched_outbound(n)

        def reply_code(msg):
            if msg['session_event'] == TransportUserMessage.SESSION_CLOSE:
                return 'end'
            return 'reply'

        returnValue([(reply_code(msg), msg['content']) for msg in msgs])

    @inlineCallbacks
    def test_new_session(self):
        yield self.send(None, TransportUserMessage.SESSION_NEW)
        replies = yield self.recv(1)
        self.assertEqual(len(replies), 1)

        reply = replies[0]
        self.assertEqual(reply[0], 'reply')
        self.assertEqual(reply[1],
                         "New game!\n"
                         "Word: ________\n"
                         "Letters guessed so far: \n"
                         "Enter next guess (0 to quit):\n")

    @inlineCallbacks
    def test_random_word(self):
        word = yield self.worker.random_word(
            self.worker.config['random_word_url'])
        self.assertEqual(word, 'elephant')

    @inlineCallbacks
    def test_full_session(self):
        yield self.send(None, TransportUserMessage.SESSION_NEW)
        for event in ('e', 'l', 'p', 'h', 'a', 'n', 'o', 't'):
            yield self.send(event, TransportUserMessage.SESSION_RESUME)

        replies = yield self.recv(9)
        self.assertEqual(len(replies), 9)

        last_reply = replies[-1]
        self.assertEqual(last_reply[0], 'reply')
        self.assertEqual(last_reply[1],
                         "Epic victory!\n"
                         "Word: elephant\n"
                         "Letters guessed so far: aehlnopt\n"
                         "Enter anything to start a new game (0 to quit):\n")

        yield self.send('1')
        replies = yield self.recv(10)
        last_reply = replies[-1]
        self.assertEqual(last_reply[0], 'reply')
        self.assertEqual(last_reply[1],
                         "New game!\n"
                         "Word: ________\n"
                         "Letters guessed so far: \n"
                         "Enter next guess (0 to quit):\n")

        yield self.send('0')
        replies = yield self.recv(11)
        last_reply = replies[-1]
        self.assertEqual(last_reply[0], 'end')
        self.assertEqual(last_reply[1], "Adieu!")

    @inlineCallbacks
    def test_close_session(self):
        yield self.send(None, TransportUserMessage.SESSION_CLOSE)
        replies = yield self.recv()
        self.assertEqual(replies, [])

    @inlineCallbacks
    def test_non_ascii_input(self):
        yield self.send(None, TransportUserMessage.SESSION_NEW)
        for event in (u'ü', u'æ'):
            yield self.send(event, TransportUserMessage.SESSION_RESUME)

        replies = yield self.recv(3)
        self.assertEqual(len(replies), 3)

        for reply in replies[1:]:
            self.assertEqual(reply[0], 'reply')
            self.assertTrue(reply[1].startswith(
                'Letters of the alphabet only please.'))
