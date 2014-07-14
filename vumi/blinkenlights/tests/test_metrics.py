import time

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred

from vumi.blinkenlights import metrics
from vumi.tests.utils import get_stubbed_channel
from vumi.message import Message
from vumi.service import Worker
from vumi.tests.helpers import VumiTestCase, WorkerHelper


class TestMetricPublisher(VumiTestCase):

    def setUp(self):
        self.worker_helper = self.add_helper(WorkerHelper())

    @inlineCallbacks
    def start_publisher(self, publisher):
        channel = yield get_stubbed_channel(self.worker_helper.broker)
        publisher.start(channel)

    def _sleep(self, delay):
        d = Deferred()
        reactor.callLater(delay, lambda: d.callback(None))
        return d

    def _check_msg(self, prefix, metric, values):
        msgs = self.worker_helper.get_dispatched_metrics()
        if values is None:
            self.assertEqual(msgs, [])
            return
        [datapoint] = msgs[-1]
        self.assertEqual(datapoint[0], prefix + metric.name)
        self.assertEqual(datapoint[1], list(metric.aggs))
        # check datapoints within 2s of now -- the truncating of
        # time.time() to an int for timestamps can cause a 1s
        # difference by itself
        now = time.time()
        self.assertTrue(all(abs(p[0] - now) < 2.0
                            for p in datapoint[2]),
                        "Not all datapoints near now (%f): %r"
                        % (now, datapoint))
        self.assertEqual([dp[1] for dp in datapoint[2]], values)

    @inlineCallbacks
    def test_publish_single_metric(self):
        publisher = metrics.MetricPublisher()
        yield self.start_publisher(publisher)

        msg = metrics.MetricMessage()
        cnt = metrics.Count("my.count")
        msg.append(
            ("vumi.test.%s" % (cnt.name,), cnt.aggs, [(time.time(), 1)]))
        publisher.publish_message(msg)
        self._check_msg("vumi.test.", cnt, [1])

    def test_publisher_provides_interface(self):
        publisher = metrics.MetricPublisher()
        self.assertTrue(metrics.IMetricPublisher.providedBy(publisher))


class TestMetricManager(VumiTestCase):

    def setUp(self):
        self._next_publish = Deferred()
        self.add_cleanup(lambda: self._next_publish.callback(None))
        self.worker_helper = self.add_helper(WorkerHelper())

    def on_publish(self, mm):
        d, self._next_publish = self._next_publish, Deferred()
        d.callback(mm)

    def wait_publish(self):
        return self._next_publish

    @inlineCallbacks
    def start_manager_as_publisher(self, manager):
        channel = yield get_stubbed_channel(self.worker_helper.broker)
        manager.start(channel)
        self.add_cleanup(manager.stop)

    def _sleep(self, delay):
        d = Deferred()
        reactor.callLater(delay, lambda: d.callback(None))
        return d

    def _check_msg(self, manager, metric, values):
        msgs = self.worker_helper.get_dispatched_metrics()
        if values is None:
            self.assertEqual(msgs, [])
            return
        [datapoint] = msgs[-1]
        self.assertEqual(datapoint[0], manager.prefix + metric.name)
        self.assertEqual(datapoint[1], list(metric.aggs))
        # check datapoints within 2s of now -- the truncating of
        # time.time() to an int for timestamps can cause a 1s
        # difference by itself
        now = time.time()
        self.assertTrue(all(abs(p[0] - now) < 2.0
                            for p in datapoint[2]),
                        "Not all datapoints near now (%f): %r"
                        % (now, datapoint))
        self.assertEqual([dp[1] for dp in datapoint[2]], values)

    @inlineCallbacks
    def test_start_manager_no_publisher(self):
        mm = metrics.MetricManager("vumi.test.")
        self.assertEqual(mm._publisher, None)
        self.assertEqual(mm._task, None)
        channel = yield get_stubbed_channel(self.worker_helper.broker)
        mm.start(channel)
        self.add_cleanup(mm.stop)
        self.assertIsInstance(mm._publisher, metrics.MetricPublisher)
        self.assertNotEqual(mm._task, None)

    @inlineCallbacks
    def test_start_manager_publisher_and_channel(self):
        publisher = metrics.MetricPublisher()
        mm = metrics.MetricManager("vumi.test.", publisher=publisher)
        self.assertEqual(mm._publisher, publisher)
        self.assertEqual(mm._task, None)
        channel = yield get_stubbed_channel(self.worker_helper.broker)
        self.assertRaises(RuntimeError, mm.start, channel)

    def test_start_polling_no_publisher(self):
        mm = metrics.MetricManager("vumi.test.")
        self.assertEqual(mm._publisher, None)
        self.assertEqual(mm._task, None)
        mm.start_polling()
        self.add_cleanup(mm.stop_polling)
        self.assertEqual(mm._publisher, None)
        self.assertNotEqual(mm._task, None)

    def test_start_polling_with_publisher(self):
        publisher = metrics.MetricPublisher()
        mm = metrics.MetricManager("vumi.test.", publisher=publisher)
        self.assertEqual(mm._publisher, publisher)
        self.assertEqual(mm._task, None)
        mm.start_polling()
        self.add_cleanup(mm.stop_polling)
        self.assertEqual(mm._publisher, publisher)
        self.assertNotEqual(mm._task, None)

    def test_oneshot(self):
        self.patch(time, "time", lambda: 12345)
        mm = metrics.MetricManager("vumi.test.")
        cnt = metrics.Count("my.count")
        mm.oneshot(cnt, 3)
        self.assertEqual(cnt.name, "my.count")
        self.assertEqual(mm._oneshot_msgs, [
            (cnt, [(12345, 3)]),
        ])

    def test_register(self):
        mm = metrics.MetricManager("vumi.test.")
        cnt = mm.register(metrics.Count("my.count"))
        self.assertEqual(cnt.name, "my.count")
        self.assertEqual(mm._metrics, [cnt])

    def test_double_register(self):
        mm = metrics.MetricManager("vumi.test.")
        mm.register(metrics.Count("my.count"))
        self.assertRaises(metrics.MetricRegistrationError,
                          mm.register, metrics.Count("my.count"))

    def test_lookup(self):
        mm = metrics.MetricManager("vumi.test.")
        cnt = mm.register(metrics.Count("my.count"))
        self.assertTrue("my.count" in mm)
        self.assertTrue(mm["my.count"] is cnt)
        self.assertEqual(mm["my.count"].name, "my.count")

    @inlineCallbacks
    def test_publish_metrics_poll(self):
        mm = metrics.MetricManager("vumi.test.", 0.1, self.on_publish)
        cnt = mm.register(metrics.Count("my.count"))
        yield self.start_manager_as_publisher(mm)

        cnt.inc()
        mm.publish_metrics()
        self._check_msg(mm, cnt, [1])

    @inlineCallbacks
    def test_publish_metrics_oneshot(self):
        mm = metrics.MetricManager("vumi.test.", 0.1, self.on_publish)
        cnt = metrics.Count("my.count")
        yield self.start_manager_as_publisher(mm)

        mm.oneshot(cnt, 1)
        mm.publish_metrics()
        self._check_msg(mm, cnt, [1])

    @inlineCallbacks
    def test_start(self):
        mm = metrics.MetricManager("vumi.test.", 0.1, self.on_publish)
        cnt = mm.register(metrics.Count("my.count"))
        yield self.start_manager_as_publisher(mm)

        self.assertTrue(mm._task is not None)
        self._check_msg(mm, cnt, None)

        cnt.inc()
        yield self.wait_publish()
        self._check_msg(mm, cnt, [1])

        cnt.inc()
        cnt.inc()
        yield self.wait_publish()
        self._check_msg(mm, cnt, [1, 1])

    @inlineCallbacks
    def test_publish_metrics(self):
        mm = metrics.MetricManager("vumi.test.", 0.1, self.on_publish)
        cnt = metrics.Count("my.count")
        yield self.start_manager_as_publisher(mm)

        mm.oneshot(cnt, 1)
        self.assertEqual(len(mm._oneshot_msgs), 1)
        mm.publish_metrics()
        self.assertEqual(mm._oneshot_msgs, [])
        self._check_msg(mm, cnt, [1])

    def test_publish_metrics_not_started_no_publisher(self):
        mm = metrics.MetricManager("vumi.test.")
        self.assertEqual(mm._publisher, None)
        mm.oneshot(metrics.Count("my.count"), 1)
        self.assertRaises(ValueError, mm.publish_metrics)

    def test_stop_unstarted(self):
        mm = metrics.MetricManager("vumi.test.", 0.1, self.on_publish)
        mm.stop()
        mm.stop()  # Check that .stop() is idempotent.

    @inlineCallbacks
    def test_in_worker(self):
        worker = yield self.worker_helper.get_worker(Worker, {}, start=False)
        mm = yield worker.start_publisher(metrics.MetricManager,
                                          "vumi.test.", 0.1, self.on_publish)
        acc = mm.register(metrics.Metric("my.acc"))
        try:
            self.assertTrue(mm._task is not None)
            self._check_msg(mm, acc, None)

            acc.set(1.5)
            acc.set(1.0)
            yield self.wait_publish()
            self._check_msg(mm, acc, [1.5, 1.0])
        finally:
            mm.stop()

    @inlineCallbacks
    def test_task_failure(self):
        channel = yield get_stubbed_channel()
        mm = metrics.MetricManager("vumi.test.", 0.1)
        wait_error = Deferred()

        class BadMetricError(Exception):
            pass

        class BadMetric(metrics.Metric):
            def poll(self):
                wait_error.callback(None)
                raise BadMetricError("bad metric")

        mm.register(BadMetric("bad"))
        mm.start(channel)
        yield wait_error
        yield self._sleep(0)  # allow log message to be processed
        error, = self.flushLoggedErrors(BadMetricError)
        self.assertTrue(error.type is BadMetricError)


class TestAggregators(VumiTestCase):
    def test_sum(self):
        self.assertEqual(metrics.SUM([]), 0.0)
        self.assertEqual(metrics.SUM([1.0, 2.0]), 3.0)
        self.assertEqual(metrics.SUM([2.0, 1.0]), 3.0)
        self.assertEqual(metrics.SUM.name, "sum")
        self.assertEqual(metrics.Aggregator.from_name("sum"), metrics.SUM)

    def test_avg(self):
        self.assertEqual(metrics.AVG([]), 0.0)
        self.assertEqual(metrics.AVG([1.0, 2.0]), 1.5)
        self.assertEqual(metrics.AVG([2.0, 1.0]), 1.5)
        self.assertEqual(metrics.AVG.name, "avg")
        self.assertEqual(metrics.Aggregator.from_name("avg"), metrics.AVG)

    def test_min(self):
        self.assertEqual(metrics.MIN([]), 0.0)
        self.assertEqual(metrics.MIN([1.0, 2.0]), 1.0)
        self.assertEqual(metrics.MIN([2.0, 1.0]), 1.0)
        self.assertEqual(metrics.MIN.name, "min")
        self.assertEqual(metrics.Aggregator.from_name("min"), metrics.MIN)

    def test_max(self):
        self.assertEqual(metrics.MAX([]), 0.0)
        self.assertEqual(metrics.MAX([1.0, 2.0]), 2.0)
        self.assertEqual(metrics.MAX([2.0, 1.0]), 2.0)
        self.assertEqual(metrics.MAX.name, "max")
        self.assertEqual(metrics.Aggregator.from_name("max"), metrics.MAX)

    def test_last(self):
        self.assertEqual(metrics.LAST([]), 0.0)
        self.assertEqual(metrics.LAST([1.0, 2.0]), 2.0)
        self.assertEqual(metrics.LAST([2.0, 1.0]), 1.0)
        self.assertEqual(metrics.LAST.name, "last")
        self.assertEqual(metrics.Aggregator.from_name("last"), metrics.LAST)

    def test_already_registered(self):
        self.assertRaises(metrics.AggregatorAlreadyDefinedError,
                          metrics.Aggregator, "sum", sum)


class CheckValuesMixin(object):

    def _check_poll_base(self, metric, n):
        datapoints = metric.poll()
        # check datapoints within 2s of now -- the truncating of
        # time.time() to an int for timestamps can cause a 1s
        # difference by itself
        now = time.time()
        self.assertTrue(all(abs(d[0] - now) <= 2.0
                            for d in datapoints),
                        "Not all datapoints near now (%f): %r"
                        % (now, datapoints))
        self.assertTrue(all(isinstance(d[0], (int, long)) for d in datapoints))
        actual_values = [dp[1] for dp in datapoints]
        return actual_values

    def check_poll_func(self, metric, n, test):
        actual_values = self._check_poll_base(metric, n)
        self.assertEqual([test(v) for v in actual_values], [True] * n)

    def check_poll(self, metric, expected_values):
        n = len(expected_values)
        actual_values = self._check_poll_base(metric, n)
        self.assertEqual(actual_values, expected_values)


class TestMetric(VumiTestCase, CheckValuesMixin):
    def test_manage(self):
        mm = metrics.MetricManager("vumi.test.")
        metric = metrics.Metric("foo")
        metric.manage(mm)
        self.assertEqual(metric.name, "foo")
        mm2 = metrics.MetricManager("vumi.othertest.")
        self.assertRaises(metrics.MetricRegistrationError, metric.manage,
                          mm2)

    def test_managed(self):
        metric = metrics.Metric("foo")
        self.assertFalse(metric.managed)
        mm = metrics.MetricManager("vumi.test.")
        metric.manage(mm)
        self.assertTrue(metric.managed)

    def test_poll(self):
        metric = metrics.Metric("foo")
        self.check_poll(metric, [])
        metric.set(1.0)
        metric.set(2.0)
        self.check_poll(metric, [1.0, 2.0])


class TestCount(VumiTestCase, CheckValuesMixin):
    def test_inc_and_poll(self):
        metric = metrics.Count("foo")
        self.check_poll(metric, [])
        metric.inc()
        self.check_poll(metric, [1.0])
        self.check_poll(metric, [])
        metric.inc()
        metric.inc()
        self.check_poll(metric, [1.0, 1.0])


class TestTimer(VumiTestCase, CheckValuesMixin):

    def patch_time(self, starting_value):
        def fake_time():
            return self._fake_time
        self.patch(time, 'time', fake_time)
        self._fake_time = starting_value

    def incr_fake_time(self, value):
        self._fake_time += value

    def test_start_and_stop(self):
        timer = metrics.Timer("foo")

        self.patch_time(12345.0)
        timer.start()
        self.incr_fake_time(0.1)
        timer.stop()
        self.check_poll_func(timer, 1, lambda x: 0.09 < x < 0.11)
        self.check_poll(timer, [])

    def test_already_started(self):
        timer = metrics.Timer("foo")
        timer.start()
        self.assertRaises(metrics.TimerAlreadyStartedError, timer.start)

    def test_not_started(self):
        timer = metrics.Timer("foo")
        self.assertRaises(metrics.TimerNotStartedError, timer.stop)

    def test_stop_and_stop(self):
        timer = metrics.Timer("foo")
        timer.start()
        timer.stop()
        self.assertRaises(metrics.TimerNotStartedError, timer.stop)

    def test_double_start_and_stop(self):
        timer = metrics.Timer("foo")
        self.patch_time(12345.0)
        timer.start()
        self.incr_fake_time(0.1)
        timer.stop()
        timer.start()
        self.incr_fake_time(0.1)
        timer.stop()
        self.check_poll_func(timer, 2, lambda x: 0.09 < x < 0.11)
        self.check_poll(timer, [])

    def test_context_manager(self):
        timer = metrics.Timer("foo")
        self.patch_time(12345.0)
        with timer:
            self.incr_fake_time(0.1)  # feign sleep
        self.check_poll_func(timer, 1, lambda x: 0.09 < x < 0.11)
        self.check_poll(timer, [])

    def test_accumulate_times(self):
        timer = metrics.Timer("foo")
        self.patch_time(12345.0)
        with timer:
            self.incr_fake_time(0.1)  # feign sleep
        with timer:
            self.incr_fake_time(0.1)  # feign sleep
        self.check_poll_func(timer, 2, lambda x: 0.09 < x < 0.11)
        self.check_poll(timer, [])

    def test_timeit(self):
        timer = metrics.Timer("foo")
        self.patch_time(12345.0)
        with timer.timeit():
            self.incr_fake_time(0.1)
        self.check_poll_func(timer, 1, lambda x: 0.09 < x < 0.11)
        self.check_poll(timer, [])

    def test_timeit_start_and_stop(self):
        timer = metrics.Timer("foo")
        self.patch_time(12345.0)
        event_timer = timer.timeit()
        event_timer.start()
        self.incr_fake_time(0.1)
        event_timer.stop()
        self.check_poll_func(timer, 1, lambda x: 0.09 < x < 0.11)
        self.check_poll(timer, [])

    def test_timeit_start_and_start(self):
        event_timer = metrics.Timer("foo").timeit()
        event_timer.start()
        self.assertRaises(metrics.TimerAlreadyStartedError, event_timer.start)

    def test_timeit_stop_without_start(self):
        event_timer = metrics.Timer("foo").timeit()
        self.assertRaises(metrics.TimerNotStartedError, event_timer.stop)

    def test_timeit_stop_and_stop(self):
        event_timer = metrics.Timer("foo").timeit()
        event_timer.start()
        event_timer.stop()
        self.assertRaises(metrics.TimerAlreadyStoppedError, event_timer.stop)

    def test_timeit_autostart(self):
        timer = metrics.Timer("foo")
        self.patch_time(12345.0)
        event_timer = timer.timeit(start=True)
        self.incr_fake_time(0.1)
        event_timer.stop()
        self.check_poll_func(timer, 1, lambda x: 0.09 < x < 0.11)
        self.check_poll(timer, [])


class TestMetricsConsumer(VumiTestCase):
    def test_consume_message(self):
        expected_datapoints = [
            ("vumi.test.v1", 1234, 1.0),
            ("vumi.test.v2", 3456, 2.0),
            ]
        datapoints = []
        callback = lambda *v: datapoints.append(v)
        consumer = metrics.MetricsConsumer(None, callback)
        msg = metrics.MetricMessage()
        msg.extend(expected_datapoints)
        vumi_msg = Message.from_json(msg.to_json())
        consumer.consume_message(vumi_msg)
        self.assertEqual(datapoints, expected_datapoints)
