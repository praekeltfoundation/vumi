# -*- test-case-name: vumi.workers.blinkenlights.tests.test_metrics -*-

import time
from datetime import datetime
import random

from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor

from vumi.service import Consumer, Publisher, Worker
from vumi.blinkenlights.message20110707 import MetricsMessage


class MetricsConsumer(Consumer):
    exchange_name = "vumi.metrics"
    exchange_type = "direct"
    routing_key = "vumi.metrics"
    durable = True

    def __init__(self, callback):
        self.callback = callback
        self.queue_name = self.routing_key

    def consume_message(self, message):
        return self.callback(message)


def timedelta_millis(td):
    millis = 1000*3600*24*td.days
    millis += 1000*td.seconds
    millis += td.microseconds/1000.0
    return millis

class MetricsPublisher(Publisher):
    exchange_name = "vumi.metrics"
    exchange_type = "direct"
    routing_key = "vumi.metrics"
    durable = True
    auto_delete = False
    delivery_mode = 2

    def __init__(self):
        self._reset_current_metrics()

    def add_counter(self, counter_name, value=1):
        """
        Increment a counter.
        """
        current_value = self.counters.get(counter_name, 0)
        self.counters[counter_name] = current_value + value

    def start_timer(self, timer_name):
        """
        Start a timer.
        """
        self._get_timer(timer_name)['start'].append(datetime.utcnow())

    def stop_timer(self, timer_name):
        """
        Start a timer that was previously started with start_timer().
        """
        if timer_name not in self.timers:
            # TODO: Find a way to dedup this
            raise ValueError("Trying to stop timer that hasn't been started: %s" % (timer_name))
        timer = self._get_timer(timer_name)
        if not len(timer['stop']) < len(timer['start']):
            raise ValueError("Trying to stop timer that hasn't been started: %s" % (timer_name))
        timer['stop'].append(datetime.utcnow())

    def send_metrics(self):
        """
        Send metrics to Blinkenlights.

        All metrics collected since the last call to send_metrics()
        are sent and then cleared, ready for collecting the next set.
        """
        msg = self._make_message(self._build_metrics())
        self.publish_message(msg)
        self._reset_current_metrics()

    def _reset_current_metrics(self):
        self.counters = {}
        self.timers = {}

    def _build_metrics(self):
        metrics = []
        for name, value in self.counters.items():
            metrics.append({'name': name, 'count': value})
        for name, value in self.timers.items():
            start, stop = value['start'], value['stop']
            if len(start) != len(stop):
                # TODO: Something appropriate
                pass
            times = zip(start, stop)
            total_millis = sum(timedelta_millis(e-s) for s, e in times)
            metrics.append({'name': name, 'count': len(times), 'time': total_millis})
        return metrics

    def _get_timer(self, timer_name):
        return self.timers.setdefault(timer_name, {'start': [], 'stop': []})

    def _make_message(self, metrics):
        msg = MetricsMessage('metrics', 'metrics generator', '0', metrics)
        return msg.to_vumi_message()



class GraphitePublisher(Publisher):
    exchange_name = "graphite"
    exchange_type = "topic"
    durable = True
    auto_delete = False
    delivery_mode = 2

    def publish_metric(self, metric, value, timestamp):
        self.publish_raw("%f %d" % (value, timestamp), routing_key=metric)


class MetricsCollector(Worker):

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the MetricsCollector with config: %s" % self.config)
        self.consumer = yield self.start_consumer(MetricsConsumer, self.consume_metrics)

    def consume_metrics(self, message):
        msg = MetricsMessage.from_dict(message.payload)
        for name, metrics in msg.metrics.items():
            self.metrics.setdefault(name, []).extend(metrics)
        log.msg("Collected: " + ", ".join("%s=%s" % m for m in self.metrics_stats()))

    def metrics_stats(self, *keys):
        if not keys:
            keys = self.metrics.keys()
        return [(k, len(self.metrics[k])) for k in keys]

    def stopWorker(self):
        log.msg("Stopping the MetricsCollector")


class GraphiteMetricsCollector(Worker):

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the GraphiteMetricsCollector with config: %s" % self.config)
        self.graphite_publisher = yield self.start_publisher(GraphitePublisher)
        self.consumer = yield self.start_consumer(MetricsConsumer, self.consume_metrics)

    def process_timestamp(self, timestamp):
        unix_timestamp = time.mktime(timestamp.timetuple())
        # Convert to local time for Graphite *vomit*
        unix_timestamp = unix_timestamp - time.timezone
        return unix_timestamp

    def consume_metrics(self, message):
        msg = MetricsMessage.from_dict(message.payload)
        unix_timestamp = self.process_timestamp(msg.timestamp)
        for name, metrics in msg.metrics.items():
            value = sum(m[0] for m in metrics)
            self.graphite_publisher.publish_metric(name, value, unix_timestamp)

    def stopWorker(self):
        log.msg("Stopping the GraphiteMetricsCollector")


class RandomMetricsGenerator(Worker):

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the MetricsGenerator with config: %s" % self.config)
        self.publisher = yield self.start_publisher(MetricsPublisher)
        self.run()

    def run(self):
        self.send_some_metrics()
        reactor.callLater(random.choice([0.1, 0.2, 0.5, 0.5, 1, 1]), self.run)

    def generate_metric(self):
        metric_name = 'vumi.metrics.' + random.choice(['foo', 'bar', 'baz'])
        self.publisher.add_counter(metric_name, random.randint(1, 10))

    def send_some_metrics(self):
        num = random.randint(3, 10)
        for i in range(num):
            self.generate_metric()
        log.msg("Sending %s metrics" % (num,))
        self.publisher.send_metrics()

    def stopWorker(self):
        log.msg("Stopping the MetricsGenerator")
