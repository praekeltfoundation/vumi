from twisted.trial.unittest import TestCase
from vumi.blinkenlights import metrics

import time


class TestMetricManager(TestCase):
    def test_register(self):
        mm = metrics.MetricManager("vumi.test.")
        cnt = mm.register(metrics.Count("my.count"))
        self.assertEqual(cnt.name, "vumi.test.my.count")
        self.assertEqual(mm._metrics, [cnt])

    def test_start(self):
        raise NotImplementedError


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
