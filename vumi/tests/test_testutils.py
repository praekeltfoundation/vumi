from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from vumi.service import Worker
from vumi.tests.utils import (TestAMQClient, TestQueue, TestChannel,
                              FakeAMQBroker, get_stubbed_worker)


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
        options = {'key': 'value'}
        worker = get_stubbed_worker(ToyWorker, options)
        self.assertEquals({}, worker._amqp_client.vumi_options)
        self.assertEquals(options, worker.config)

    @inlineCallbacks
    def test_fake_broker(self):
        broker = FakeAMQBroker()
        amq_client = TestAMQClient(fake_broker=broker)
        queue = yield amq_client.queue('foo')
        channel = yield amq_client.channel('foo')
        self.assertEqual(broker, queue.fake_broker)
        self.assertEqual(broker, channel.fake_broker)

        channel.basic_publish(exchange='exchange', routing_key='rkey',
                              content='blah')
        self.assertEqual(['blah'], broker.get_dispatched('exchange', 'rkey'))
        self.assertEqual([], broker.get_dispatched('exchange', 'foo'))
        self.assertEqual([], broker.get_dispatched('meh', 'foo'))
