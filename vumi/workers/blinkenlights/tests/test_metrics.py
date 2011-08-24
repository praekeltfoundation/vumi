from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks
from vumi.tests.utils import (TestChannel, get_stubbed_worker,
                              FakeAMQBroker)
from vumi.workers.blinkenlights import metrics
from vumi.message import Message

import time


class TestGraphitePublisher(TestCase):

    def _check_msg(self, channel, metric, value, timestamp):
        msg = channel.publish_log[-1]
        self.assertEqual(msg["routing_key"], metric)
        self.assertEqual(msg["exchange"], "graphite")
        content = msg["content"]
        self.assertEqual(content.properties, {"delivery mode": 2})
        self.assertEqual(content.body, "%f %d" % (value,
                                                  timestamp - time.timezone))

    def test_publish_metric(self):
        datapoint = ("vumi.test.v1", 1.0, 1234)
        channel = TestChannel()
        pub = metrics.GraphitePublisher()
        pub.start(channel)
        pub.publish_metric(*datapoint)
        self._check_msg(channel, *datapoint)


class TestGraphiteMetricsCollector(TestCase):
    @inlineCallbacks
    def test_single_message(self):
        broker = FakeAMQBroker()
        worker = get_stubbed_worker(metrics.GraphiteMetricsCollector,
                                    broker=broker)
        yield worker.startWorker()

        broker.publish("vumi.metrics", "vumi.metrics", "foo")
        yield None

        output_channel = worker._amqp_client.channels[0]
        input_channel = worker._amqp_client.channels[1]

        self.fail("TODO: send message into input channel and test")


class TestRandomMetricsGenerator(TestCase):
    @inlineCallbacks
    def test_one_run(self):
        broker = FakeAMQBroker()
        worker = get_stubbed_worker(metrics.RandomMetricsGenerator,
                                    config={
                                        "manager_period": "0.1",
                                        "generator_period": "0.1",
                                    },
                                    broker=broker)
        yield worker.startWorker()

        yield worker.wake_after_run()
        yield worker.wake_after_run()

        msg1, msg2 = broker.get_dispatched('vumi.metrics', 'vumi.metrics')
        msg1 = Message.from_json(msg1.body).payload
        self.assertEqual(msg1["datapoints"], [])
        msg2 = Message.from_json(msg2.body).payload
        self.assertEqual(sorted(d[0] for d in msg2["datapoints"]),
                         ["vumi.random.count", "vumi.random.sum",
                          "vumi.random.timer", "vumi.random.value"])

        yield worker.stopWorker()
