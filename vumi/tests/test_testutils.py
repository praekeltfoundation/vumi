from twisted.trial.unittest import TestCase

from vumi.service import Worker
from vumi.tests.utils import get_stubbed_worker, Mocking
from vumi.tests.fake_amqp import FakeAMQClient


class ToyWorker(Worker):
    def poke(self):
        return "poke"


class MockingHistoryItemTestCase(TestCase):
    def test_basic_item(self):
        item = Mocking.HistoryItem(("a", "b"), {"c": 1})
        self.assertEqual(item.args, ("a", "b"))
        self.assertEqual(item.kwargs, {"c": 1})

    def test_repr(self):
        item = Mocking.HistoryItem(("a", "b"), {"c": 1})
        self.assertEqual(repr(item), "<'HistoryItem' object at %s"
                         " [args: ('a', 'b'), kw: {'c': 1}]>" % id(item))


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
