# -*- test-case-name: vumi.dispatchers.tests.test_base -*-

"""Basic tools for building dispatchers."""

from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.service import Worker
from vumi.message import TransportUserMessage, TransportEvent
from vumi.utils import load_class_by_string


class BaseDispatchRouter(object):
    def __init__(self, dispatcher, config):
        pass


class BaseDispatchWorker(Worker):
    """Base class for a dispatch worker.

    """

    @inlineCallbacks
    def startWorker(self):
        log.msg('Starting a %s dispatcher with config: %s'
                % (self.__class__.__name__,  self.config))

        self._transport_names = self.config.get('transport_names', [])
        self._exposed_names = self.config.get('exposed_names', [])
        router_cls = load_class_by_string(self.config['router_class'])
        self._router = router_cls(self, self.config)

        yield self.setup_transport_publishers()
        yield self.setup_exposed_publishers()
        yield self.setup_transport_consumers()
        yield self.setup_exposed_consumers()

    @inlineCallbacks
    def setup_transport_publishers(self):
        self.transport_publisher = {}
        for transport_name in self._transport_names:
            self.transport_publisher[transport_name] = yield self.publish_to(
                '%s.outbound' % (transport_name,))

    @inlineCallbacks
    def setup_transport_consumers(self):
        self.transport_consumer = {}
        self.transport_event_consumer = {}
        for transport_name in self._transport_names:
            self.transport_consumer[transport_name] = yield self.consume(
                '%s.inbound' % (transport_name,),
                self.dispatch_user_message,
                message_class=TransportUserMessage)
        for transport_name in self._transport_names:
            self.transport_event_consumer[transport_name] = yield self.consume(
                '%s.event' % (transport_name,),
                self.dispatch_event,
                message_class=TransportEvent)

    @inlineCallbacks
    def setup_exposed_publishers(self):
        self.exposed_publisher = {}
        self.exposed_event_publisher = {}
        for exposed_name in self._exposed_names:
            self.exposed_publisher[exposed_name] = yield self.publish_to(
                '%s.inbound' % (exposed_name,))
        for exposed_name in self._exposed_names:
            self.exposed_event_publisher[exposed_name] = yield self.publish_to(
                '%s.event' % (exposed_name,))

    @inlineCallbacks
    def setup_exposed_consumers(self):
        self.exposed_consumer = {}
        for exposed_name in self._exposed_names:
            self.exposed_consumer[exposed_name] = yield self.consume(
                '%s.outbound' % (exposed_name,),
                self.dispatch_outbound_message,
                message_class=TransportUserMessage)
