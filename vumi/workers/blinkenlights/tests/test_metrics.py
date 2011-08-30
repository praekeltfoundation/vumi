from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks
from vumi.tests.utils import TestChannel, get_stubbed_worker
from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.workers.blinkenlights import metrics
from vumi.blinkenlights.message20110818 import MetricMessage
from vumi.message import Message

import time


class TestMetricAggregation(TestCase):

    def setUp(self):
        self._workers = []
        self._broker = None

    @inlineCallbacks
    def tearDown(self):
        for worker in self._workers:
            yield worker.stopWorker()

    def publish(self, msg):
        self._broker.publish_message("vumi.metrics",
                                     "vumi.metrics", msg)

    def get_dispatched(self):
        return self._broker.get_dispatched("vumi.metrics.aggregates",
                                           "vumi.metrics.aggregates")

    @inlineCallbacks
    def _setup_workers(self, bucketters, aggregators, bucket_size):
        self._broker = FakeAMQPBroker()

        bucket_workers = []
        bucket_config = {
            'buckets': aggregators,
            'bucket_size': bucket_size,
            }
        for _i in range(bucketters):
            worker = get_stubbed_worker(metrics.MetricTimeBucket,
                                        config=bucket_config,
                                        broker=self._broker)
            yield worker.startWorker()
            bucket_workers.append(worker)

        aggregator_workers = []
        aggregator_config = {
            'bucket_size': bucket_size,
            }
        for i in range(aggregators):
            config = aggregator_config.copy()
            config['bucket'] = i
            worker = get_stubbed_worker(metrics.MetricAggregator,
                                        config=config, broker=self._broker)
            yield worker.startWorker()
            aggregator_workers.append(worker)

        self._workers.extend(bucket_workers)
        self._workers.extend(aggregator_workers)

    # TODO: use parameteric test cases to test many combinations of workers
    @inlineCallbacks
    def test_aggregating_one_metric(self):
        yield self._setup_workers(1, 1, 1)

        msg = MetricMessage()
        now = int(time.time())
        msg.append(("vumi.test.foo", ["sum"], [(now, 1.0), (now, 2.0)]))
        self.publish(msg)
        self.publish(msg)

        yield self._broker.kick_delivery()
        yield self._broker.kick_delivery()
        time.sleep(1.1)
        yield self._broker.kick_delivery()

        content, = self.get_dispatched()
        vumi_msg = Message.from_json(content.body)
        msg = MetricMessage.from_dict(vumi_msg.payload)
        self.assertEqual(msg.datapoints(), [
            [u'vumi.test.foo.sum', u'', [[now, 6.0]]]
            ])


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
        worker = get_stubbed_worker(metrics.GraphiteMetricsCollector)
        broker = worker._amqp_client.broker
        yield worker.startWorker()

        msg = MetricMessage()
        msg.append(("vumi.test.foo", "", [(1234, 1.5)]))

        broker.publish_message("vumi.metrics.aggregates",
                               "vumi.metrics.aggregates", msg)
        yield broker.kick_delivery()

        content, = broker.get_dispatched("graphite", "vumi.test.foo")
        parts = content.body.split()
        value, ts = float(parts[0]), int(parts[1])
        self.assertEqual(value, 1.5)
        self.assertEqual(ts, 1234 - time.timezone)


class TestRandomMetricsGenerator(TestCase):
    @inlineCallbacks
    def test_one_run(self):
        worker = get_stubbed_worker(metrics.RandomMetricsGenerator,
                                    config={
                                        "manager_period": "0.1",
                                        "generator_period": "0.1",
                                    })
        broker = worker._amqp_client.broker
        yield worker.startWorker()

        yield worker.wake_after_run()
        yield worker.wake_after_run()

        msg1, msg2 = broker.get_dispatched('vumi.metrics', 'vumi.metrics')
        msg1 = Message.from_json(msg1.body).payload
        self.assertEqual(msg1["datapoints"], [])
        msg2 = Message.from_json(msg2.body).payload
        self.assertEqual(sorted(d[0] for d in msg2["datapoints"]),
                         ["vumi.random.count", "vumi.random.timer",
                          "vumi.random.value"])

        yield worker.stopWorker()
