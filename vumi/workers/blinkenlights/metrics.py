# -*- test-case-name: vumi.workers.blinkenlights.tests.test_metrics -*-

import time
import random

from twisted.python import log
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

from vumi.service import Publisher, Worker
from vumi.blinkenlights.metrics import (MetricsConsumer, MetricManager, Count,
                                        Sum, SimpleValue, Timer)


class GraphitePublisher(Publisher):
    """Publisher for sending messages to Graphite."""

    exchange_name = "graphite"
    exchange_type = "topic"
    durable = True
    auto_delete = False
    delivery_mode = 2

    def _local_timestamp(self, timestamp):
        """Graphite requires local timestamps."""
        # TODO: investigate whether graphite can be encourage to use UTC
        #       timestamps
        return timestamp - time.timezone

    def publish_metric(self, metric, value, timestamp):
        timestamp = self._local_timestamp(timestamp)
        self.publish_raw("%f %d" % (value, timestamp), routing_key=metric)


class GraphiteMetricsCollector(Worker):
    """Worker that collects Vumi metrics and publishes them to Graphite."""

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the GraphiteMetricsCollector with"
                " config: %s" % self.config)
        self.graphite_publisher = yield self.start_publisher(GraphitePublisher)
        self.consumer = yield self.start_consumer(MetricsConsumer,
                                                  self.consume_metrics)

    def consume_metrics(self, metric_name, timestamp, value):
        self.graphite_publisher.publish_metric(metric_name, value, timestamp)

    def stopWorker(self):
        log.msg("Stopping the GraphiteMetricsCollector")


class RandomMetricsGenerator(Worker):
    """Worker that publishes a set of random metrics.

    Useful for tests and demonstrations.

    Configuration Values
    --------------------
    manager_period : float in seconds, optional
        How often to have the internal metric manager send metrics
        messages. Default is 5s.
    generator_period: float in seconds, optional
        How often the random metric loop should send values to the
        metric manager. Default is 1s.
    """

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the MetricsGenerator with config: %s" % self.config)
        manager_period = float(self.config.get("manager_period", 5.0))
        log.msg("MetricManager will sent metrics every %s seconds" %
                manager_period)
        generator_period = float(self.config.get("generator_period", 1.0))
        log.msg("Random metrics values will be generated every %s seconds" %
                generator_period)

        self.mm = yield self.start_publisher(MetricManager, "vumi.random.",
                                             manager_period)
        self.counter = self.mm.register(Count("count"))
        self.sum = self.mm.register(Sum("sum"))
        self.simple = self.mm.register(SimpleValue("value"))
        self.timer = self.mm.register(Timer("timer"))
        self.next = Deferred()
        self.task = LoopingCall(self.run)
        self.task.start(generator_period)

    @inlineCallbacks
    def run(self):
        if random.choice([True, False]):
            self.counter.inc()
        self.sum.add(random.uniform(0.0, 1.0))
        self.simple.set(random.normalvariate(2.0, 0.1))
        with self.timer:
            d = Deferred()
            wait = random.uniform(0.0, 0.1)
            reactor.callLater(wait, lambda: d.callback(None))
            yield d
        done, self.next = self.next, Deferred()
        done.callback(self)

    def wake_after_run(self):
        """Return a deferred that fires after the next run completes."""
        return self.next

    def stopWorker(self):
        self.mm.stop()
        self.task.stop()
        log.msg("Stopping the MetricsGenerator")
