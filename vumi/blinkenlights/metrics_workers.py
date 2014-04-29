# -*- test-case-name: vumi.blinkenlights.tests.test_metrics_workers -*-

import time
import random
import hashlib
from datetime import datetime

from twisted.python import log
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.internet.protocol import DatagramProtocol

from vumi.service import Consumer, Publisher, Worker
from vumi.blinkenlights.metrics import (MetricsConsumer, MetricManager, Count,
                                        Metric, Timer, Aggregator)
from vumi.blinkenlights.message20110818 import MetricMessage


class AggregatedMetricConsumer(Consumer):
    """Consumer for aggregate metrics.

    Parameters
    ----------
    callback : function (metric_name, values)
        Called for each metric datapoint as it arrives.  The
        parameters are metric_name (str) and values (a list of
        timestamp and value pairs).
    """
    exchange_name = "vumi.metrics.aggregates"
    exchange_type = "direct"
    durable = True
    routing_key = "vumi.metrics.aggregates"

    def __init__(self, channel, callback):
        self.queue_name = self.routing_key
        super(AggregatedMetricConsumer, self).__init__(channel)
        self.callback = callback

    def consume_message(self, vumi_message):
        msg = MetricMessage.from_dict(vumi_message.payload)
        for metric_name, _aggregators, values in msg.datapoints():
            self.callback(metric_name, values)


class AggregatedMetricPublisher(Publisher):
    """Publishes aggregated metrics.
    """
    exchange_name = "vumi.metrics.aggregates"
    exchange_type = "direct"
    durable = True
    routing_key = "vumi.metrics.aggregates"

    def publish_aggregate(self, metric_name, timestamp, value):
        # TODO: perhaps change interface to publish multiple metrics?
        msg = MetricMessage()
        msg.append((metric_name, (), [(timestamp, value)]))
        self.publish_message(msg)


class TimeBucketConsumer(Consumer):
    """Consume time bucketed metric messages.

    Parameters
    ----------
    bucket : int
        Bucket to consume time buckets from.
    callback : function, f(metric_name, aggregators, values)
        Called for each metric datapoint as it arrives.
        The parameters are metric_name (str),
        aggregator (list of aggregator names) and values (a
        list of timestamp and value pairs).
    """
    exchange_name = "vumi.metrics.buckets"
    exchange_type = "direct"
    durable = True
    ROUTING_KEY_TEMPLATE = "bucket.%d"

    def __init__(self, channel, bucket, callback):
        self.queue_name = self.ROUTING_KEY_TEMPLATE % bucket
        self.routing_key = self.queue_name
        super(TimeBucketConsumer, self).__init__(channel)
        self.callback = callback

    def consume_message(self, vumi_message):
        msg = MetricMessage.from_dict(vumi_message.payload)
        for metric_name, aggregators, values in msg.datapoints():
            self.callback(metric_name, aggregators, values)


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
        return int(md5.hexdigest(), 16) % self.buckets

    def publish_metric(self, metric_name, aggregates, values):
        timestamp_buckets = {}
        for timestamp, value in values:
            ts_key = int(timestamp) / self.bucket_size
            ts_bucket = timestamp_buckets.get(ts_key)
            if ts_bucket is None:
                ts_bucket = timestamp_buckets[ts_key] = []
            ts_bucket.append((timestamp, value))

        for ts_key, ts_bucket in timestamp_buckets.iteritems():
            bucket = self.find_bucket(metric_name, ts_key)
            routing_key = self.ROUTING_KEY_TEMPLATE % bucket
            msg = MetricMessage()
            msg.append((metric_name, aggregates, ts_bucket))
            self.publish_message(msg, routing_key=routing_key)


class MetricTimeBucket(Worker):
    """Gathers metrics messages and redistributes them to aggregators.

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
        bucket_size = int(self.config.get("bucket_size"))
        log.msg("Bucket size is %d seconds" % bucket_size)
        self.publisher = yield self.start_publisher(TimeBucketPublisher,
                                                    buckets, bucket_size)
        self.consumer = yield self.start_consumer(MetricsConsumer,
                self.publisher.publish_metric)


class DiscardedMetricError(Exception):
    pass


class MetricAggregator(Worker):
    """Gathers a subset of metrics and aggregates them.

    :class:`MetricAggregators` work in sets of N.

    Configuration Values
    --------------------
    bucket : int, 0 to N-1
        An aggregator needs to know which number out of N it is. This is
        its bucket number.
    bucket_size : int, in seconds
        The amount of time each time bucket represents.
    lag : int, seconds, optional
        The number of seconds after a bucket's time ends to wait
        before processing the bucket. Default is 5s.
    """

    _time = time.time  # hook for faking time in tests

    def _ts_key(self, time):
        return int(time) / self.bucket_size

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting a MetricAggregator with config: %s" % self.config)
        bucket = int(self.config.get("bucket"))
        log.msg("MetricAggregator bucket %d" % bucket)
        self.bucket_size = int(self.config.get("bucket_size"))
        log.msg("Bucket size is %d seconds" % self.bucket_size)
        self.lag = float(self.config.get("lag", 5.0))

        # ts_key -> { metric_name -> (aggregate_set, values) }
        # values is a list of (timestamp, value) pairs
        self.buckets = {}
        # initialize last processed bucket
        self._last_ts_key = self._ts_key(self._time() - self.lag) - 2

        self.publisher = yield self.start_publisher(AggregatedMetricPublisher)
        self.consumer = yield self.start_consumer(TimeBucketConsumer,
                                                  bucket, self.consume_metric)

        self._task = LoopingCall(self.check_buckets)
        done = self._task.start(self.bucket_size, False)
        done.addErrback(lambda failure: log.err(failure,
                        "MetricAggregator bucket checking task died"))

    def check_buckets(self):
        """Periodically clean out old buckets and calculate aggregates."""
        # key for previous bucket
        current_ts_key = self._ts_key(self._time() - self.lag) - 1
        for ts_key in self.buckets.keys():
            if ts_key <= self._last_ts_key:
                log.err(DiscardedMetricError("Throwing way old metric data: %r"
                                             % self.buckets[ts_key]))
                del self.buckets[ts_key]
            elif ts_key <= current_ts_key:
                aggregates = []
                ts = ts_key * self.bucket_size
                items = self.buckets[ts_key].iteritems()
                for metric_name, (agg_set, values) in items:
                    values = [v for t, v in sorted(values)]
                    for agg_name in agg_set:
                        agg_metric = "%s.%s" % (metric_name, agg_name)
                        agg_func = Aggregator.from_name(agg_name)
                        agg_value = agg_func(values)
                        aggregates.append((agg_metric, agg_value))

                for agg_metric, agg_value in aggregates:
                    self.publisher.publish_aggregate(agg_metric, ts,
                                                     agg_value)
                del self.buckets[ts_key]
        self._last_ts_key = current_ts_key

    def consume_metric(self, metric_name, aggregates, values):
        if not values:
            return
        ts_key = self._ts_key(values[0][0])
        metrics = self.buckets.get(ts_key, None)
        if metrics is None:
            metrics = self.buckets[ts_key] = {}
        metric = metrics.get(metric_name)
        if metric is None:
            metric = metrics[metric_name] = (set(), [])
        existing_aggregates, existing_values = metric
        existing_aggregates.update(aggregates)
        existing_values.extend(values)

    def stopWorker(self):
        self._task.stop()
        self.check_buckets()


class MetricsCollectorWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting %s with config: %s" % (
                type(self).__name__, self.config))
        yield self.setup_worker()
        self.consumer = yield self.start_consumer(
            AggregatedMetricConsumer, self.consume_metrics)

    def stopWorker(self):
        log.msg("Stopping %s" % (type(self).__name__,))
        return self.teardown_worker()

    def setup_worker(self):
        pass

    def teardown_worker(self):
        pass

    def consume_metrics(self, metric_name, values):
        raise NotImplementedError()


class GraphitePublisher(Publisher):
    """Publisher for sending messages to Graphite."""

    exchange_name = "graphite"
    exchange_type = "topic"
    durable = True
    auto_delete = False
    delivery_mode = 2

    def publish_metric(self, metric, value, timestamp):
        self.publish_raw("%f %d" % (value, timestamp), routing_key=metric)


class GraphiteMetricsCollector(MetricsCollectorWorker):
    """Worker that collects Vumi metrics and publishes them to Graphite."""

    @inlineCallbacks
    def setup_worker(self):
        self.graphite_publisher = yield self.start_publisher(GraphitePublisher)

    def consume_metrics(self, metric_name, values):
        for timestamp, value in values:
            self.graphite_publisher.publish_metric(
                metric_name, value, timestamp)


class UDPMetricsProtocol(DatagramProtocol):
    def __init__(self, ip, port):
        # NOTE: `host` must be an IP, not a hostname.
        self._ip = ip
        self._port = port

    def startProtocol(self):
        self.transport.connect(self._ip, self._port)

    def send_metric(self, metric_string):
        return self.transport.write(metric_string)


class UDPMetricsCollector(MetricsCollectorWorker):
    """Worker that collects Vumi metrics and publishes them over UDP."""

    DEFAULT_FORMAT_STRING = '%(timestamp)s %(metric_name)s %(value)s\n'
    DEFAULT_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S%z'

    @inlineCallbacks
    def setup_worker(self):
        self.format_string = self.config.get(
            'format_string', self.DEFAULT_FORMAT_STRING)
        self.timestamp_format = self.config.get(
            'timestamp_format', self.DEFAULT_TIMESTAMP_FORMAT)
        self.metrics_ip = yield reactor.resolve(self.config['metrics_host'])
        self.metrics_port = int(self.config['metrics_port'])
        self.metrics_protocol = UDPMetricsProtocol(
            self.metrics_ip, self.metrics_port)
        self.listener = yield reactor.listenUDP(0, self.metrics_protocol)

    def teardown_worker(self):
        return self.listener.stopListening()

    def consume_metrics(self, metric_name, values):
        for timestamp, value in values:
            timestamp = datetime.utcfromtimestamp(timestamp)
            metric_string = self.format_string % {
                'timestamp': timestamp.strftime(self.timestamp_format),
                'metric_name': metric_name,
                'value': value,
                }
            self.metrics_protocol.send_metric(metric_string)


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
    # callback for tests, f(worker)
    # (or anyone else that wants to be notified when metrics are generated)
    on_run = None

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
        with self.timer.timeit():
            d = Deferred()
            wait = random.uniform(0.0, 0.1)
            reactor.callLater(wait, lambda: d.callback(None))
            yield d
        if self.on_run is not None:
            self.on_run(self)

    def stopWorker(self):
        self.mm.stop()
        self.task.stop()
        log.msg("Stopping the MetricsGenerator")
