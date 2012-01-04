
from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.service import Worker


class MessageHandler(object):
    def __init__(self, worker, queue_name, publisher, publish_keys):
        self.worker = worker
        self.queue_name = queue_name
        self.publisher = publisher
        self.publish_keys = publish_keys or []  # if None assume empty set
        log.msg("SimpleDispatcher forwarding from %s to %s" % (
            self.queue_name, self.publish_keys))

    def consume_message(self, message):
        log.msg("SimpleDispatcher consuming on %s: %s" % (
            self.queue_name,
            repr(message)))
        for k in self.publish_keys:
            self.publisher.publish_message(message, routing_key=k)


class SimpleDispatcher(Worker):

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting SimpleDispatcher with config: %s" % (self.config))

        self.publisher = yield self.publish_to("simpledispatcher.fallback")
        for queue_name, publish_keys in self.config['route_mappings'].items():
            h = MessageHandler(self, queue_name, self.publisher, publish_keys)
            yield self.consume(queue_name, h.consume_message)

    def stopWorker(self):
        log.msg("Stopping SimpleDispatcher")
