from twisted.trial.unittest import TestCase

from vumi.service import Worker
from vumi.tests.utils import get_stubbed_worker
from vumi.tests.fake_amqp import FakeAMQClient


class ToyWorker(Worker):
    def poke(self):
        return "poke"


class UtilsTestCase(TestCase):

    def test_get_stubbed_worker(self):
        worker = get_stubbed_worker(ToyWorker)
        self.assertEqual("poke", worker.poke())
        self.assertTrue(isinstance(worker._amqp_client, FakeAMQClient))

    def test_get_stubbed_worker_with_config(self):
        options = {'key': 'value'}
        worker = get_stubbed_worker(ToyWorker, options)
        self.assertEqual({}, worker._amqp_client.vumi_options)
        self.assertEqual(options, worker.config)
