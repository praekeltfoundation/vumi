from twisted.trial.unittest import TestCase
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred
from vumi.blinkenlights import metrics
from vumi.tests.utils import get_stubbed_worker, get_stubbed_channel
from vumi.message import Message
from vumi.service import Worker

import time


class TestMetricManager(TestCase):

    def _sleep(self, delay):
        d = Deferred()
        reactor.callLater(delay, lambda: d.callback(None))
        return d

    def _check_msg(self, broker, metric, values):
        msgs = broker.get_dispatched("vumi.metrics", "vumi.metrics")
        content = msgs[-1]
        name = metric.name
        self.assertEqual(content.properties, {"delivery mode": 2})
        msg = Message.from_json(content.body)
        [datapoint] = msg.payload["datapoints"]
        self.assertEqual(datapoint[0], name)
        self.assertEqual(datapoint[1], list(metric.aggs))
        self.assertTrue(all(abs(p[0] - time.time()) < 1.0
                            for p in datapoint[2]))
        self.assertEqual([p[1] for p in datapoint[2]], values)

    def test_register(self):
        mm = metrics.MetricManager("vumi.test.")
        cnt = mm.register(metrics.Count("my.count"))
        self.assertEqual(cnt.name, "vumi.test.my.count")
        self.assertEqual(mm._metrics, [cnt])

    def test_lookup(self):
        mm = metrics.MetricManager("vumi.test.")
        cnt = mm.register(metrics.Count("my.count"))
        self.assertTrue("my.count" in mm)
        self.assertTrue(mm["my.count"] is cnt)
        self.assertEqual(mm["my.count"].name, "vumi.test.my.count")

    @inlineCallbacks
    def test_start(self):
        channel = yield get_stubbed_channel()
        broker = channel.broker
        mm = metrics.MetricManager("vumi.test.", 0.1)
        cnt = mm.register(metrics.Count("my.count"))
        mm.start(channel)
        try:
            self.assertTrue(mm._task is not None)
            self._check_msg(broker, cnt, [])

            cnt.inc()
            yield self._sleep(0.1)
            self._check_msg(broker, cnt, [1])

            cnt.inc()
            cnt.inc()
            yield self._sleep(0.1)
            self._check_msg(broker, cnt, [1, 1])
        finally:
            mm.stop()

    @inlineCallbacks
    def test_in_worker(self):
        worker = get_stubbed_worker(Worker)
        broker = worker._amqp_client.broker
        mm = yield worker.start_publisher(metrics.MetricManager,
                                          "vumi.test.", 0.1)
        acc = mm.register(metrics.Metric("my.acc"))
        try:
            self.assertTrue(mm._task is not None)

            yield self._sleep(0.1)
            self._check_msg(broker, acc, [])

            acc.set(1.5)
            acc.set(1.0)
            yield self._sleep(0.1)
            self._check_msg(broker, acc, [1.5, 1.0])
        finally:
            mm.stop()

    @inlineCallbacks
    def test_task_failure(self):
        channel = yield get_stubbed_channel()
        mm = metrics.MetricManager("vumi.test.", 0.1)

        class BadMetricError(Exception):
            pass

        class BadMetric(metrics.Metric):
            def poll(self):
                raise BadMetricError("bad metric")

        mm.register(BadMetric("bad"))
        mm.start(channel)
        error, = self.flushLoggedErrors(BadMetricError)
        self.assertTrue(error.type is BadMetricError)


class TestAggregators(TestCase):
    def test_sum(self):
        self.assertEqual(metrics.SUM([]), 0.0)
        self.assertEqual(metrics.SUM([1.0, 2.0]), 3.0)
        self.assertEqual(metrics.SUM.name, "sum")
        self.assertEqual(metrics.Aggregator.from_name("sum"), metrics.SUM)

    def test_avg(self):
        self.assertEqual(metrics.AVG([]), 0.0)
        self.assertEqual(metrics.AVG([1.0, 2.0]), 1.5)
        self.assertEqual(metrics.AVG.name, "avg")
        self.assertEqual(metrics.Aggregator.from_name("avg"), metrics.AVG)

    def test_min(self):
        self.assertEqual(metrics.MIN([]), 0.0)
        self.assertEqual(metrics.MIN([1.0, 2.0]), 1.0)
        self.assertEqual(metrics.MIN.name, "min")
        self.assertEqual(metrics.Aggregator.from_name("min"), metrics.MIN)

    def test_max(self):
        self.assertEqual(metrics.MAX([]), 0.0)
        self.assertEqual(metrics.MAX([1.0, 2.0]), 2.0)
        self.assertEqual(metrics.MAX.name, "max")
        self.assertEqual(metrics.Aggregator.from_name("max"), metrics.MAX)

    def test_already_registered(self):
        self.assertRaises(metrics.AggregatorAlreadyDefinedError,
                          metrics.Aggregator, "sum", sum)


class CheckValuesMixin(object):

    def _check_poll_base(self, metric, n):
        datapoints = metric.poll()
        self.assertTrue(all(abs(d[0] - time.time()) <= 1.0
                            for d in datapoints))
        self.assertTrue(all(isinstance(d[0], (int, long)) for d in datapoints))
        actual_values = [d[1] for d in datapoints]
        return actual_values

    def check_poll_func(self, metric, n, test):
        actual_values = self._check_poll_base(metric, n)
        self.assertEqual([test(v) for v in actual_values], [True] * n)

    def check_poll(self, metric, expected_values):
        n = len(expected_values)
        actual_values = self._check_poll_base(metric, n)
        self.assertEqual(actual_values, expected_values)


class TestMetric(TestCase, CheckValuesMixin):
    def test_manage(self):
        metric = metrics.Metric("foo")
        self.assertEqual(metric.name, None)
        metric.manage("vumi.test.")
        self.assertEqual(metric.name, "vumi.test.foo")
        self.assertRaises(metrics.MetricRegistrationError, metric.manage,
                          "vumi.othertest.")

    def test_poll(self):
        metric = metrics.Metric("foo")
        metric.manage("prefix.")
        self.check_poll(metric, [])
        metric.set(1.0)
        metric.set(2.0)
        self.check_poll(metric, [1.0, 2.0])


class TestCount(TestCase, CheckValuesMixin):
    def test_inc_and_poll(self):
        metric = metrics.Count("foo")
        metric.manage("prefix.")
        self.check_poll(metric, [])
        metric.inc()
        self.check_poll(metric, [1.0])
        self.check_poll(metric, [])
        metric.inc()
        metric.inc()
        self.check_poll(metric, [1.0, 1.0])


class TestTimer(TestCase, CheckValuesMixin):
    def test_start_and_stop(self):
        timer = metrics.Timer("foo")
        timer.manage("prefix.")
        timer.start()
        try:
            time.sleep(0.1)
        finally:
            timer.stop()
        self.check_poll_func(timer, 1, lambda x: 0.09 <= x <= 0.11)
        self.check_poll(timer, [])

    def test_already_started(self):
        timer = metrics.Timer("foo")
        timer.manage("prefix.")
        timer.start()
        self.assertRaises(metrics.TimerAlreadyStartedError, timer.start)

    def test_context_manager(self):
        timer = metrics.Timer("foo")
        timer.manage("prefix.")
        with timer:
            time.sleep(0.1)
        self.check_poll_func(timer, 1, lambda x: 0.09 <= x <= 0.11)
        self.check_poll(timer, [])

    def test_accumulate_times(self):
        timer = metrics.Timer("foo")
        timer.manage("prefix.")
        with timer:
            time.sleep(0.1)
        with timer:
            time.sleep(0.1)
        self.check_poll_func(timer, 2, lambda x: 0.09 <= x <= 0.11)
        self.check_poll(timer, [])


class TestMetricsConsumer(TestCase):
    def test_consume_message(self):
        expected_datapoints = [
            ("vumi.test.v1", 1234, 1.0),
            ("vumi.test.v2", 3456, 2.0),
            ]
        datapoints = []
        consumer = metrics.MetricsConsumer(lambda *v: datapoints.append(v))
        msg = metrics.MetricMessage()
        msg.extend(expected_datapoints)
        vumi_msg = Message.from_json(msg.to_json())
        consumer.consume_message(vumi_msg)
        self.assertEqual(datapoints, expected_datapoints)
