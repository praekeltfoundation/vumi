from twisted.internet.defer import inlineCallbacks

from vumi.message import TransportUserMessage
from vumi.demos.rps import RockPaperScissorsGame, RockPaperScissorsWorker
from vumi.application.tests.helpers import ApplicationHelper
from vumi.tests.helpers import VumiTestCase


class TestRockPaperScissorsGame(VumiTestCase):
    def get_game(self, scores=None):
        game = RockPaperScissorsGame(5, 'p1')
        game.set_player_2('p2')
        if scores is not None:
            game.scores = scores
        return game

    def test_game_init(self):
        game = RockPaperScissorsGame(5, 'p1')
        self.assertEquals('p1', game.player_1)
        self.assertEquals(None, game.player_2)
        self.assertEquals((0, 0), game.scores)
        self.assertEquals(None, game.current_move)

        game.set_player_2('p2')
        self.assertEquals('p1', game.player_1)
        self.assertEquals('p2', game.player_2)
        self.assertEquals((0, 0), game.scores)
        self.assertEquals(None, game.current_move)

    def test_game_moves_draw(self):
        game = self.get_game((1, 1))
        game.move('p1', 1)
        self.assertEquals(1, game.current_move)
        self.assertEquals((1, 1), game.scores)

        game.move('p2', 1)
        self.assertEquals(None, game.current_move)
        self.assertEquals((1, 1), game.scores)

    def test_game_moves_win(self):
        game = self.get_game((1, 1))
        game.move('p1', 1)
        self.assertEquals(1, game.current_move)
        self.assertEquals((1, 1), game.scores)

        game.move('p2', 2)
        self.assertEquals(None, game.current_move)
        self.assertEquals((2, 1), game.scores)

    def test_game_moves_lose(self):
        game = self.get_game((1, 1))
        game.move('p1', 1)
        self.assertEquals(1, game.current_move)
        self.assertEquals((1, 1), game.scores)

        game.move('p2', 3)
        self.assertEquals(None, game.current_move)
        self.assertEquals((1, 2), game.scores)


class TestRockPaperScissorsWorker(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.app_helper = self.add_helper(
            ApplicationHelper(RockPaperScissorsWorker))
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

        self.assertEquals(2, len(self.app_helper.get_dispatched_outbound()))

    @inlineCallbacks
    def test_moves(self):
        user1 = '+27831234567'
        user2 = '+27831234568'

        yield self.dispatch_start_message(user1)
        game = self.worker.open_game
        yield self.dispatch_start_message(user2)

        self.assertEquals(2, len(self.app_helper.get_dispatched_outbound()))
        yield self.app_helper.make_dispatch_inbound('1', from_addr=user2)
        self.assertEquals(2, len(self.app_helper.get_dispatched_outbound()))
        yield self.app_helper.make_dispatch_inbound('2', from_addr=user1)
        self.assertEquals(4, len(self.app_helper.get_dispatched_outbound()))
        self.assertEquals((0, 1), game.scores)

    @inlineCallbacks
    def test_full_game(self):
        user1 = '+27831234567'
        user2 = '+27831234568'

        yield self.dispatch_start_message(user1)
        game = self.worker.open_game
        yield self.dispatch_start_message(user2)

        for user, content in [(user1, '1'), (user2, '2')] * 5:  # best-of 5
            yield self.app_helper.make_dispatch_inbound(
                content, from_addr=user)

        self.assertEqual((5, 0), game.scores)
        [end1, end2] = self.app_helper.get_dispatched_outbound()[-2:]
        self.assertEqual("You won! :-)", end1["content"])
        self.assertEqual(user1, end1["to_addr"])
        self.assertEqual("You lost. :-(", end2["content"])
        self.assertEqual(user2, end2["to_addr"])
