from twisted.trial import unittest

from vumi.message import TransportUserMessage
from vumi.tests.utils import get_stubbed_worker
from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.demos.rps import RockPaperScissorsGame, RockPaperScissorsWorker


class TestRockPaperScissorsGame(unittest.TestCase):
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


class TestRockPaperScissorsWorker(unittest.TestCase):
    def setUp(self):
        self._amqp = FakeAMQPBroker()
        self._workers = []

    def tearDown(self):
        for worker in self._workers:
            worker.stopWorker()

    def get_worker(self):
        worker = get_stubbed_worker(RockPaperScissorsWorker, {
                'transport_name': 'foo',
                'ussd_code': '99999',
                }, self._amqp)
        self._workers.append(worker)
        worker.startWorker()
        return worker

    def mkmsg(self, from_addr, sid, content='', sev=None):
        if sev is None:
            sev = TransportUserMessage.SESSION_RESUME
        return TransportUserMessage(
            transport_name='sphex',
            transport_type='ussd',
            transport_metadata={},
            from_addr=from_addr,
            to_addr='12345',
            content=content,
            session_id=sid,
            session_event=sev,
            )

    def get_msgs(self):
        return self._amqp.get_messages('vumi', 'foo.outbound')

    def test_new_sessions(self):
        worker = self.get_worker()
        self.assertEquals({}, worker.games)
        self.assertEquals(None, worker.open_game)

        worker.dispatch_user_message(self.mkmsg(
                '+27831234567', 'sp1', sev=TransportUserMessage.SESSION_NEW))
        self.assertNotEquals(None, worker.open_game)
        game = worker.open_game
        self.assertEquals({'sp1': game}, worker.games)

        worker.dispatch_user_message(self.mkmsg(
                '+27831234568', 'sp2', sev=TransportUserMessage.SESSION_NEW))
        self.assertEquals(None, worker.open_game)
        self.assertEquals({'sp1': game, 'sp2': game}, worker.games)

        self.assertEquals(2, len(self.get_msgs()))

    def test_moves(self):
        worker = self.get_worker()
        worker.dispatch_user_message(self.mkmsg(
                '+27831234567', 'sp1', sev=TransportUserMessage.SESSION_NEW))
        game = worker.open_game
        worker.dispatch_user_message(self.mkmsg(
                '+27831234568', 'sp2', sev=TransportUserMessage.SESSION_NEW))

        self.assertEquals(2, len(self.get_msgs()))
        worker.dispatch_user_message(self.mkmsg(
                '+27831234568', 'sp2', content='1'))
        self.assertEquals(2, len(self.get_msgs()))
        worker.dispatch_user_message(self.mkmsg(
                '+27831234567', 'sp1', content='2'))
        self.assertEquals(4, len(self.get_msgs()))
        self.assertEquals((0, 1), game.scores)
