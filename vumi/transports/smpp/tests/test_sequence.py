from twisted.internet.defer import inlineCallbacks

from vumi.tests.helpers import VumiTestCase, PersistenceHelper
from vumi.transports.smpp.sequence import RedisSequence


class EsmeTestCase(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.redis = yield self.persistence_helper.get_redis_manager()

    @inlineCallbacks
    def test_iter(self):
        sequence_generator = RedisSequence(self.redis)
        iterator = iter(sequence_generator)
        self.assertEqual((yield iterator.next()), 1)
        iterator = iter(sequence_generator)
        self.assertEqual((yield iterator.next()), 2)

    @inlineCallbacks
    def test_next(self):
        sequence_generator = RedisSequence(self.redis)
        self.assertEqual((yield sequence_generator.next()), 1)

    @inlineCallbacks
    def test_get_next_sequence(self):
        sequence_generator = RedisSequence(self.redis)
        self.assertEqual((yield sequence_generator.get_next_seq()), 1)

    @inlineCallbacks
    def test_rollover(self):
        sequence_generator = RedisSequence(self.redis, rollover_at=3)
        self.assertEqual((yield sequence_generator.next()), 1)
        self.assertEqual((yield sequence_generator.next()), 2)
        self.assertEqual((yield sequence_generator.next()), 3)
        self.assertEqual((yield sequence_generator.next()), 1)
