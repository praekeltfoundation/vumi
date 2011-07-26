from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from vumi.service import Worker
from vumi.message import Message

class BaseWorker(Worker):
    def subscribe_to_transport(self, transport, account, identifier=None, callback=lambda *a: a):
        routing_key = '%s.inbound.%s' % (transport, account)
        if identifier:
            routing_key += ".%s" % identifier
        self.consume(routing_key, callback)
    
    @inlineCallbacks
    def publish_to_transport(self, transport, account, identifier=None):
        routing_key = '%s.outbound.%s' % (transport, account)
        if identifier:
            routing_key += ".%s" % identifier
        publisher = yield self.publish_to(routing_key)
        returnValue(publisher)
    
    @inlineCallbacks
    def say(self, transport, recipient, message):
        publisher = getattr(self, '%s_publisher' % transport)
        yield publisher.publish_message(Message(recipient=recipient, message=message))
    
    @property
    def subscribers(self):
        if not hasattr(self, '_subscribers'):
            self._subscribers = {}
        return self._subscribers

    def subscribe(self, uuid, transport, limit):
        self.subscribers[uuid] = (transport, 0, limit)

    def unsubscribe(self, uuid):
        return self.subscribers.pop(uuid)

    def is_subscribed(self, uuid):
        return uuid in self.subscribers

    def get_subscriber_info(self, uuid):
        self.subscribers
        return self.subscribers.get(uuid)

    def get_subscriber_vars(self, uuid):
        transport, counter, limit = self.get_subscriber_info(uuid)
        lookup = {
            'limit': limit,
            'counter': counter,
            'transport': transport
        }
        lookup.update(self.config)
        return lookup

    def is_within_quota(self, uuid):
        transport, counter, limit = self.get_subscriber_info(uuid)
        return counter < (limit-1)

    def increment_message_count_for(self, uuid):
        transport, counter, limit = self.get_subscriber_info(uuid)
        counter += 1
        self.subscribers[uuid] = (transport, counter, limit)
        return counter
