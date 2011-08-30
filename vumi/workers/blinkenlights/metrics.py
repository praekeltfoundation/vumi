# -*- test-case-name: vumi.workers.blinkenlights.tests.test_metrics -*-

import time
import random
import hashlib

from twisted.python import log
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

from vumi.service import Consumer, Publisher, Worker
from vumi.blinkenlights.metrics import (MetricsConsumer, MetricManager, Count,
                                        Metric, Timer)
from vumi.blinkenlights.message20110818 import MetricMessage


class AggregatedMetricConsumer(Consumer):
    """Consumer aggregate metrics.
    """


class AggregatedMetricPublisher(Publisher):
    """Publishes aggregated metrics.
    """


class TimeBucketConsumer(Consumer):
    """Consume time bucketed metric messages.
    """


class TimeBucketPublisher(Publisher):
    """Publish time bucketed metric messages.

    Parameters
    ----------
    buckets : int
        Total number of buckets messages are being
        distributed to.
    bucket_size : int, in seconds
        Size of each time bucket in seconds.
    """
    exchange_name = "vumi.metrics.buckets"
    exchange_type = "direct"
    durable = True
    ROUTING_KEY_TEMPLATE = "bucket.%d"

    def __init__(self, buckets, bucket_size):
        self.buckets = buckets
        self.bucket_size = bucket_size

    def find_bucket(self, metric_name, ts_key):
        md5 = hashlib.md5("%s:%d" % (metric_name, ts_key))
        return int(md5.hexdigest(), 16) / self.buckets

    def publish_metric(self, metric_name, aggregates, values):
        timestamp_buckets = {}
        for timestamp, value in values:
            ts_key = timestamp / self.bucket_size
            ts_bucket = timestamp_buckets.get(ts_key)
            if ts_bucket is None:
                ts_bucket[ts_key] = []
            ts_bucket.append((timestamp, value))

        for ts_key, ts_bucket in timestamp_buckets.iteritems():
            bucket = self.find_bucket(metric_name, ts_key)
            routing_key = self.ROUTING_KEY_TEMPLATE % bucket
            msg = MetricMessage()
            msg.extend((metric_name, aggregates, ts_bucket))
            self.publish_message(msg, routing_key=routing_key)


class MetricTimeBucket(Worker):
    """Gathers metrics messages and redistributes them aggregators.

    :class:`MetricTimeBuckets` take metrics from the vumi.metrics
    exchange and redistribute them to one of N :class:`MetricAggregator`
    workers.

    There can be any number of :class:`MetricTimeBucket` workers.

    Configuration Values
    --------------------
    buckets : int (N)
        The total number of aggregator workers. :class:`MetricAggregator`
        workers must be started with bucket numbers 0 to N-1 otherwise
        metric data will go missing (or at best be stuck in a queue
        somewhere).
    bucket_size : int, in seconds
        The amount of time each time bucket represents.
    """
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting a MetricTimeBucket with config: %s" % self.config)
        buckets = int(self.config.get("buckets"))
        log.msg("Total number of buckets %d" % buckets)
        bucket_size = float(self.config.get("bucket_size"))
        log.msg("Bucket size is %d seconds" % bucket_size)
        self.publisher = yield self.start_publisher(TimeBucketPublisher,
                                                    buckets, bucket_size)
        self.consumer = yield self.start_consumer(MetricsConsumer,
                self.publisher.publish_metric)


class MetricAggregator(Worker):
    """Gathers a subset of metrics and aggregates them.

    :class:`MetricAggregators` work in sets of N.

    Configuration Values
    --------------------
    bucket : int, 0 to N-1
        An aggregator needs to know which number out of N it is. This is
        its bucket number.
    """
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting a MetricAggregator with config: %s" % self.config)
        self.bucket = int(self.config.get("bucket"))
        log.msg("MetricAggregator bucket %d" % self.bucket)


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

    def consume_metrics(self, metric_name, aggregators, values):
        for timestamp, value in values:
            self.graphite_publisher.publish_metric(metric_name, value,
                                                   timestamp)

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
        self.value = self.mm.register(Metric("value"))
        self.timer = self.mm.register(Timer("timer"))
        self.next = Deferred()
        self.task = LoopingCall(self.run)
        self.task.start(generator_period)

    @inlineCallbacks
    def run(self):
        if random.choice([True, False]):
            self.counter.inc()
        self.value.set(random.normalvariate(2.0, 0.1))
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
