from twisted.trial.unittest import TestCase

from vumi.service import Worker
from vumi.tests.utils import get_stubbed_worker, Mocking, LogCatcher
from vumi.tests.fake_amqp import FakeAMQClient
from vumi import log


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
        self.assertEqual(repr(item), "<'HistoryItem' object at %r"
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


class LogCatcherTestCase(TestCase):
    def test_simple_catching(self):
        lc = LogCatcher()
        with lc:
            log.info("Test")
        self.assertEqual(lc.messages(), ["Test"])

    def test_system_filtering(self):
        lc = LogCatcher(system="^ab")
        with lc:
            log.info("Test 1", system="abc")
            log.info("Test 2", system="def")
        self.assertEqual(lc.messages(), ["Test 1"])

    def test_message_filtering(self):
        lc = LogCatcher(message="^Keep")
        with lc:
            log.info("Keep this")
            log.info("Discard this")
        self.assertEqual(lc.messages(), ["Keep this"])

    def test_message_concatenation(self):
        lc = LogCatcher()
        with lc:
            log.info("Part 1", "Part 2")
        self.assertEqual(lc.messages(), ["Part 1 Part 2"])
