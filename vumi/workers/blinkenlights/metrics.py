# -*- test-case-name: "vumi.workers.blinkenlights.tests.test_metrics" -*-

import random

from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor

from vumi.message import Message
from vumi.service import Consumer, Publisher, Worker
from vumi.blinkenlights.message import MetricsMessage


class MetricsConsumer(Consumer):
    exchange_name = "vumi.blinkenlights"
    exchange_type = "direct"
    durable = False

    def __init__(self, callback, routing_key):
        self.callback = callback
        self.routing_key = routing_key
        self.queue_name = routing_key

    def consume_message(self, message):
        return self.callback(message)


class MetricsPublisher(Publisher):
    exchange_name = "vumi.blinkenlights"
    exchange_type = "direct"
    durable = False
    auto_delete = True
    delivery_mode = 1

    def __init__(self, routing_key):
        self.routing_key = routing_key


class MetricsCollector(Worker):

    def startWorker(self):
        log.msg("Starting the MetricsCollector with config: %s" % self.config)
        listen_rkey = self.config.get('listen_rkey', 'vumi.blinkenlights.metrics')
        self.metrics = {}
        self.start_consumer(MetricsConsumer, self.consume_metrics, listen_rkey)

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


class RandomMetricsGenerator(Worker):

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the MetricsGenerator with config: %s" % self.config)
        send_rkey = self.config.get('send_rkey', 'vumi.blinkenlights.metrics')
        self.publisher = yield self.start_publisher(MetricsPublisher, send_rkey)
        self.run()

    def run(self):
        self.send_some_metrics()
        reactor.callLater(random.choice([0.1, 0.2, 0.5, 0.5, 1, 1]), self.run)

    def generate_metric(self):
        metric_type = random.choice(['Timer', 'Counter'])
        metric_name = random.choice(['foo', 'bar', 'baz']) + metric_type
        metric = {
            'name': metric_name,
            'tag1': random.choice(['aa', 'bb']),
            'tag2': random.choice(['cc', 'dd']),
            'count': random.randint(1, 10),
            }
        if metric_type == 'Timer':
            metric['time'] = random.randint(100, 1000)
        return metric

    def make_metrics_message(self, metrics):
        return MetricsMessage('metrics', 'metrics generator', '0', metrics)

    def send_some_metrics(self):
        metrics = []
        for i in range(random.randint(3, 10)):
            metrics.append(self.generate_metric())
        log.msg("Sending %s metrics" % (len(metrics),))
        self.publisher.publish_message(Message(**self.make_metrics_message(metrics).to_dict()))

    def stopWorker(self):
        log.msg("Stopping the MetricsGenerator")
