from vumi.service import Worker
from vumi.tests.utils import get_stubbed_worker, LogCatcher
from vumi.tests.fake_amqp import FakeAMQClient
from vumi import log
from vumi.tests.helpers import VumiTestCase


class ToyWorker(Worker):
    def poke(self):
        return "poke"


class TestTestUtils(VumiTestCase):

    def test_get_stubbed_worker(self):
        worker = get_stubbed_worker(ToyWorker)
        self.assertEqual("poke", worker.poke())
        self.assertTrue(isinstance(worker._amqp_client, FakeAMQClient))

    def test_get_stubbed_worker_with_config(self):
        options = {'key': 'value'}
        worker = get_stubbed_worker(ToyWorker, options)
        self.assertEqual({}, worker._amqp_client.vumi_options)
        self.assertEqual(options, worker.config)


class TestLogCatcher(VumiTestCase):
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
