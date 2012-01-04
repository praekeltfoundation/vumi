from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, Deferred
from vumi.tests.utils import TestChannel, get_stubbed_worker
from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.blinkenlights import metrics_workers
from vumi.blinkenlights.message20110818 import MetricMessage


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
        vumi_msgs = self._broker.get_messages(exchange, queue)
        msgs = [MetricMessage.from_dict(vm.payload) for vm in vumi_msgs]
        return [msg.datapoints() for msg in msgs]


class TestMetricTimeBucket(TestCase):
    @inlineCallbacks
    def test_bucketing(self):
        config = {'buckets': 4, 'bucket_size': 5}
        worker = get_stubbed_worker(metrics_workers.MetricTimeBucket,
                                    config=config)
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

    def setUp(self):
        self.now = 0
        self.workers = []

    @inlineCallbacks
    def tearDown(self):
        for worker in self.workers:
            yield worker.stopWorker()

    def fake_time(self):
        return self.now

    def get_worker(self, cls, config=None):
        if config is None:
            config = {}
        worker = get_stubbed_worker(cls, config=config)
        self.workers.append(worker)
        return worker

    @inlineCallbacks
    def test_aggregating(self):
        config = {'bucket': 3, 'bucket_size': 5}
        worker = self.get_worker(metrics_workers.MetricAggregator,
                                 config=config)
        worker._time = self.fake_time
        broker = BrokerWrapper(worker._amqp_client.broker)
        yield worker.startWorker()

        datapoints = [
            ("vumi.test.foo", ("avg",), [(1235, 1.5), (1236, 2.0)]),
            ("vumi.test.foo", ("sum",), [(1240, 1.0)]),
            ]
        broker.send_datapoints("vumi.metrics.buckets", "bucket.3", datapoints)
        broker.send_datapoints("vumi.metrics.buckets", "bucket.3", datapoints)
        broker.send_datapoints("vumi.metrics.buckets", "bucket.2", datapoints)
        yield broker.kick_delivery()

        def recv():
            return broker.recv_datapoints("vumi.metrics.aggregates",
                                          "vumi.metrics.aggregates")

        expected = []
        self.now = 1241
        worker.check_buckets()
        self.assertEqual(recv(), expected)

        expected.append([["vumi.test.foo.avg", [], [[1235, 1.75]]]])
        self.now = 1246
        worker.check_buckets()
        self.assertEqual(recv(), expected)

        # skip a few checks
        expected.append([["vumi.test.foo.sum", [], [[1240, 2.0]]]])
        self.now = 1261
        worker.check_buckets()
        self.assertEqual(recv(), expected)

    @inlineCallbacks
    def test_aggregating_lag(self):
        config = {'bucket': 3, 'bucket_size': 5, 'lag': 1}
        worker = self.get_worker(metrics_workers.MetricAggregator,
                                 config=config)
        worker._time = self.fake_time
        broker = BrokerWrapper(worker._amqp_client.broker)
        yield worker.startWorker()

        datapoints = [
            ("vumi.test.foo", ("avg",), [(1235, 1.5), (1236, 2.0)]),
            ("vumi.test.foo", ("sum",), [(1240, 1.0)]),
            ]
        broker.send_datapoints("vumi.metrics.buckets", "bucket.3", datapoints)
        broker.send_datapoints("vumi.metrics.buckets", "bucket.3", datapoints)
        broker.send_datapoints("vumi.metrics.buckets", "bucket.2", datapoints)
        yield broker.kick_delivery()

        def recv():
            return broker.recv_datapoints("vumi.metrics.aggregates",
                                          "vumi.metrics.aggregates")

        expected = []
        self.now = 1237
        worker.check_buckets()
        self.assertEqual(recv(), expected)

        expected.append([["vumi.test.foo.avg", [], [[1235, 1.75]]]])
        self.now = 1242
        worker.check_buckets()
        self.assertEqual(recv(), expected)

        # skip a few checks
        expected.append([["vumi.test.foo.sum", [], [[1240, 2.0]]]])
        self.now = 1257
        worker.check_buckets()
        self.assertEqual(recv(), expected)


class TestAggregationSystem(TestCase):
    """Tests tying MetricTimeBucket and MetricAggregator together."""

    def setUp(self):
        self.bucket_workers = []
        self.aggregator_workers = []
        self.broker = None
        self.now = 0

    def fake_time(self):
        return self.now

    def send(self, datapoints):
        self.broker.send_datapoints("vumi.metrics",
                                    "vumi.metrics", datapoints)

    def recv(self):
        return self.broker.recv_datapoints("vumi.metrics.aggregates",
                                           "vumi.metrics.aggregates")

    @inlineCallbacks
    def tearDown(self):
        for worker in self.bucket_workers + self.aggregator_workers:
            yield worker.stopWorker()

    @inlineCallbacks
    def _setup_workers(self, bucketters, aggregators, bucket_size):
        broker = FakeAMQPBroker()
        self.broker = BrokerWrapper(broker)

        bucket_config = {
            'buckets': aggregators,
            'bucket_size': bucket_size,
            }
        for _i in range(bucketters):
            worker = get_stubbed_worker(metrics_workers.MetricTimeBucket,
                                        config=bucket_config,
                                        broker=broker)
            yield worker.startWorker()
            self.bucket_workers.append(worker)

        aggregator_config = {
            'bucket_size': bucket_size,
            }
        for i in range(aggregators):
            config = aggregator_config.copy()
            config['bucket'] = i
            worker = get_stubbed_worker(metrics_workers.MetricAggregator,
                                        config=config, broker=broker)
            worker._time = self.fake_time
            yield worker.startWorker()
            self.aggregator_workers.append(worker)

    # TODO: use parameteric test cases to test many combinations of workers
    @inlineCallbacks
    def test_aggregating_one_metric(self):
        yield self._setup_workers(1, 1, 5)

        datapoints = [("vumi.test.foo", ["sum"], [(12345, 1.0), (12346, 2.0)])]
        self.send(datapoints)
        self.send(datapoints)

        yield self.broker.kick_delivery()  # deliver to bucketters
        yield self.broker.kick_delivery()  # deliver to aggregators
        self.now = 12355
        for worker in self.aggregator_workers:
            worker.check_buckets()

        datapoints, = self.recv()
        self.assertEqual(datapoints, [
            ["vumi.test.foo.sum", [], [[12345, 6.0]]]
            ])


class TestGraphitePublisher(TestCase):

    def _check_msg(self, channel, metric, value, timestamp):
        msg = channel.publish_log[-1]
        self.assertEqual(msg["routing_key"], metric)
        self.assertEqual(msg["exchange"], "graphite")
        content = msg["content"]
        self.assertEqual(content.properties, {"delivery mode": 2})
        self.assertEqual(content.body, "%f %d" % (value, timestamp))

    def test_publish_metric(self):
        datapoint = ("vumi.test.v1", 1.0, 1234)
        channel = TestChannel()
        pub = metrics_workers.GraphitePublisher()
        pub.start(channel)
        pub.publish_metric(*datapoint)
        self._check_msg(channel, *datapoint)


class TestGraphiteMetricsCollector(TestCase):
    @inlineCallbacks
    def test_single_message(self):
        worker = get_stubbed_worker(metrics_workers.GraphiteMetricsCollector)
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
        self.assertEqual(ts, 1234)


class TestRandomMetricsGenerator(TestCase):

    def setUp(self):
        self._on_run = Deferred()
        self._workers = []

    @inlineCallbacks
    def tearDown(self):
        self._on_run.callback(None)
        for worker in self._workers:
            yield worker.stopWorker()

    def on_run(self, worker):
        d, self._on_run = self._on_run, Deferred()
        d.callback(None)

    def wake_after_run(self):
        return self._on_run

    @inlineCallbacks
    def test_one_run(self):
        worker = get_stubbed_worker(metrics_workers.RandomMetricsGenerator,
                                    config={
                                        "manager_period": "0.1",
                                        "generator_period": "0.1",
                                    })
        self._workers.append(worker)
        worker.on_run = self.on_run
        broker = BrokerWrapper(worker._amqp_client.broker)
        yield worker.startWorker()

        yield self.wake_after_run()
        yield self.wake_after_run()

        datasets = broker.recv_datapoints('vumi.metrics',
                                          'vumi.metrics')
        # there should be a least one but there may be more
        # than one if the tests are running slowly
        datapoints = datasets[0]
        self.assertEqual(sorted(d[0] for d in datapoints),
                         ["vumi.random.count", "vumi.random.timer",
                          "vumi.random.value"])
