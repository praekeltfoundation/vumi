from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks
from vumi.tests.utils import TestChannel, get_stubbed_worker
from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.workers.blinkenlights import metrics
from vumi.blinkenlights.message20110818 import MetricMessage
from vumi.message import Message

import time


class BrokerWrapper(object):
    """Wrap utility methods around a FakeAMQPBroker."""
    def __init__(self, broker):
        self._broker = broker

    def __getattr__(self, name):
        return getattr(self._broker, name)

    def send_datapoints(self, exchange, queue, datapoints):
        """Publish datapoints to a broker."""
        msg = MetricMessage()
        msg.extend(datapoints)
        self._broker.publish_message(exchange, queue, msg)

    def recv_datapoints(self, exchange, queue):
        """Retrieve datapoints from a broker."""
        contents = self._broker.get_dispatched(exchange, queue)
        vumi_msgs = [Message.from_json(content.body) for content in contents]
        msgs = [MetricMessage.from_dict(vm.payload) for vm in vumi_msgs]
        return [msg.datapoints() for msg in msgs]


class TestMetricTimeBucket(TestCase):
    @inlineCallbacks
    def test_bucketing(self):
        config = {'buckets': 4, 'bucket_size': 5}
        worker = get_stubbed_worker(metrics.MetricTimeBucket, config=config)
        broker = BrokerWrapper(worker._amqp_client.broker)
        yield worker.startWorker()

        datapoints = [
            ("vumi.test.foo", ("agg",), [(1230, 1.5), (1235, 2.0)]),
            ("vumi.test.bar", ("sum",), [(1240, 1.0)]),
            ]
        broker.send_datapoints("vumi.metrics", "vumi.metrics", datapoints)
        yield broker.kick_delivery()

        buckets = [broker.recv_datapoints("vumi.metrics.buckets",
                                          "bucket.%d" % i) for i in range(4)]

        expected_buckets = [
            [],
            [[[u'vumi.test.bar', ['sum'], [[1240, 1.0]]]]],
            [[[u'vumi.test.foo', ['agg'], [[1230, 1.5]]]],
             [[u'vumi.test.foo', ['agg'], [[1235, 2.0]]]]],
            [],
            ]

        self.assertEqual(buckets, expected_buckets)

        yield worker.stopWorker()


class TestMetricAggregator(TestCase):
    @inlineCallbacks
    def test_aggregating(self):
        config = {'bucket': 3, 'bucket_size': 5}
        worker = get_stubbed_worker(metrics.MetricAggregator, config=config)
        broker = BrokerWrapper(worker._amqp_client.broker)
        yield worker.startWorker()

        # TODO: publish some messages, check they get aggregated correctly

        yield worker.stopWorker()


class TestMetricAggregation(TestCase):
    """Tests tying MetricTimeBucket and MetricAggregator together."""

    def setUp(self):
        self._workers = []
        self._broker = None

    def send(self, datapoints):
        self._broker.send_datapoints("vumi.metrics",
                                     "vumi.metrics", datapoints)

    def recv(self):
        return self._broker.recv_datapoints("vumi.metrics.aggregates",
                                            "vumi.metrics.aggregates")

    @inlineCallbacks
    def tearDown(self):
        for worker in self._workers:
            yield worker.stopWorker()

    @inlineCallbacks
    def _setup_workers(self, bucketters, aggregators, bucket_size):
        broker = FakeAMQPBroker()
        self._broker = BrokerWrapper(broker)

        bucket_workers = []
        bucket_config = {
            'buckets': aggregators,
            'bucket_size': bucket_size,
            }
        for _i in range(bucketters):
            worker = get_stubbed_worker(metrics.MetricTimeBucket,
                                        config=bucket_config,
                                        broker=broker)
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
                                        config=config, broker=broker)
            yield worker.startWorker()
            aggregator_workers.append(worker)

        self._workers.extend(bucket_workers)
        self._workers.extend(aggregator_workers)

    # TODO: use parameteric test cases to test many combinations of workers
    @inlineCallbacks
    def test_aggregating_one_metric(self):
        yield self._setup_workers(1, 1, 1)

        now = int(time.time())
        datapoints = [("vumi.test.foo", ["sum"], [(now, 1.0), (now, 2.0)])]
        self.send(datapoints)
        self.send(datapoints)

        yield self._broker.kick_delivery()  # deliver to bucketters
        yield self._broker.kick_delivery()  # deliver to aggregators
        time.sleep(1.1)  # wait for aggregators to do work
        yield self._broker.kick_delivery()  # deliver

        datapoints, = self.recv()
        self.assertEqual(datapoints, [
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
        broker = BrokerWrapper(worker._amqp_client.broker)
        yield worker.startWorker()

        datapoints = [("vumi.test.foo", "", [(1234, 1.5)])]
        broker.send_datapoints("vumi.metrics.aggregates",
                               "vumi.metrics.aggregates", datapoints)
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
        broker = BrokerWrapper(worker._amqp_client.broker)
        yield worker.startWorker()

        yield worker.wake_after_run()
        yield worker.wake_after_run()

        datapoints1, datapoints2 = broker.recv_datapoints('vumi.metrics',
                                                          'vumi.metrics')
        self.assertEqual(datapoints1, [])
        self.assertEqual(sorted(d[0] for d in datapoints2),
                         ["vumi.random.count", "vumi.random.timer",
                          "vumi.random.value"])

        yield worker.stopWorker()
