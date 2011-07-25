from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from vumi.service import Worker
from vumi.tests.utils import (TestAMQClient, TestQueue, TestChannel,
                              get_stubbed_worker)


class ToyWorker(Worker):
    def poke(self):
        return "poke"


class UtilsTestCase(TestCase):
    @inlineCallbacks
    def test_test_amq_client(self):
        amq_client = TestAMQClient()
        queue = yield amq_client.queue('foo')
        channel = yield amq_client.channel('foo')
        self.assertEquals(TestQueue, type(queue))
        self.assertEquals(TestChannel, type(channel))

    def test_get_stubbed_worker(self):
        worker = get_stubbed_worker(ToyWorker)
        self.assertEquals("poke", worker.poke())
        self.assertEquals(TestAMQClient, worker._amqp_client.__class__)
    
    def test_get_stubbed_worker_with_config(self):
        options = {'key':'value'}
        worker = get_stubbed_worker(ToyWorker, options)
        self.assertEquals({}, worker._amqp_client.global_options)
        self.assertEquals(options, worker.config)