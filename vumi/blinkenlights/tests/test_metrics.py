from twisted.trial.unittest import TestCase
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred
from vumi.blinkenlights import metrics
from vumi.tests.utils import TestChannel, get_stubbed_worker
from vumi.message import Message
from vumi.service import Worker

import time


class TestMetricManager(TestCase):

    def _sleep(self, delay):
        d = Deferred()
        reactor.callLater(delay, lambda: d.callback(None))
        return d

    def _check_msg(self, channel, metric, value):
        msg = channel.publish_log[-1]
        name = metric.name
        self.assertEqual(msg["routing_key"], "vumi.metrics")
        self.assertEqual(msg["exchange"], "vumi.metrics")
        content = msg["content"]
        self.assertEqual(content.properties, {"delivery mode": 2})
        msg = Message.from_json(content.body)
        datapoints = msg.payload["datapoints"]
        self.assertEqual(len(datapoints), 1)
        self.assertEqual(datapoints[-1][0], name)
        self.assertEqual(datapoints[-1][2], value)

    def test_register(self):
        mm = metrics.MetricManager("vumi.test.")
        cnt = mm.register(metrics.Count("my.count"))
        self.assertEqual(cnt.name, "vumi.test.my.count")
        self.assertEqual(mm._metrics, [cnt])

    @inlineCallbacks
    def test_start(self):
        channel = TestChannel()
        mm = metrics.MetricManager("vumi.test.", 0.1)
        cnt = mm.register(metrics.Count("my.count"))
        mm.start(channel)
        try:
            self.assertTrue(mm._task is not None)
            self._check_msg(channel, cnt, 0)

            cnt.inc()
            yield self._sleep(0.1)
            self._check_msg(channel, cnt, 1)

            cnt.inc()
            cnt.inc()
            yield self._sleep(0.1)
            self._check_msg(channel, cnt, 2)
        finally:
            mm.stop()

    @inlineCallbacks
    def test_in_worker(self):
        worker = get_stubbed_worker(Worker)
        mm = yield worker.start_publisher(metrics.MetricManager,
                                          "vumi.test.", 0.1)
        channel = worker._amqp_client.channels[0]
        acc = mm.register(metrics.Sum("my.acc"))
        try:
            self.assertTrue(mm._task is not None)

            yield self._sleep(0.1)
            self._check_msg(channel, acc, 0)

            acc.add(1.5)
            acc.add(1.0)
            yield self._sleep(0.1)
            self._check_msg(channel, acc, 2.5)
        finally:
            mm.stop()


class TestMetricBase(TestCase):
    def test_manage(self):
        metric = metrics.Metric("foo")
        self.assertEqual(metric.name, None)
        metric.manage("vumi.test.")
        self.assertEqual(metric.name, "vumi.test.foo")
        self.assertRaises(metrics.MetricRegistrationError, metric.manage,
                          "vumi.othertest.")

    def test_poll(self):
        metric = metrics.Metric("foo")
        self.assertRaises(NotImplementedError, metric.poll)


class TestSimpleValue(TestCase):
    def test_set_and_poll(self):
        metric = metrics.SimpleValue("foo")
        self.assertEqual(metric.poll(), 0.0)
        metric.set(1.1)
        self.assertEqual(metric.poll(), 1.1)
        # check polling leaves value intact
        self.assertEqual(metric.poll(), 1.1)


class TestCount(TestCase):
    def test_inc_and_poll(self):
        metric = metrics.Count("foo")
        self.assertEqual(metric.poll(), 0)
        metric.inc()
        self.assertEqual(metric.poll(), 1)
        self.assertEqual(metric.poll(), 0)
        metric.inc()
        metric.inc()
        self.assertEqual(metric.poll(), 2)


class TestSum(TestCase):
    def test_add_and_poll(self):
        metric = metrics.Sum("foo")
        self.assertEqual(metric.poll(), 0.0)
        metric.add(1.5)
        self.assertEqual(metric.poll(), 1.5)
        self.assertEqual(metric.poll(), 0.0)
        metric.add(1.1)
        metric.add(0.4)
        self.assertEqual(metric.poll(), 1.5)


class TestTimer(TestCase):
    def test_start_and_stop(self):
        timer = metrics.Timer("foo")
        timer.start()
        try:
            time.sleep(0.1)
        finally:
            timer.stop()
        self.assertTrue(0.09 <= timer.poll() <= 0.11)
        self.assertEqual(timer.poll(), 0.0)

    def test_already_started(self):
        timer = metrics.Timer("foo")
        timer.start()
        self.assertRaises(metrics.TimerAlreadyStartedError, timer.start)

    def test_context_manager(self):
        timer = metrics.Timer("foo")
        with timer:
            time.sleep(0.1)
        self.assertTrue(0.09 <= timer.poll() <= 0.11)
        self.assertEqual(timer.poll(), 0.0)

    def test_accumulate_time(self):
        timer = metrics.Timer("foo")
        with timer:
            time.sleep(0.1)
        with timer:
            time.sleep(0.1)
        self.assertTrue(0.18 <= timer.poll() <= 0.22)
        self.assertEqual(timer.poll(), 0.0)


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
