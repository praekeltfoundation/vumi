from twisted.internet.defer import gatherResults, inlineCallbacks, returnValue

from vumi import log
from vumi.message import TransportMessage, TransportEvent, TransportUserMessage
from vumi.middleware import MiddlewareStack


class IgnoreMessage(Exception):
    pass


class BaseConnector(object):
    """Base class for 'connector' objects.

    A connector encapsulates the 'inbound', 'outbound' and 'event' publishers
    and consumers required by vumi workers and avoids having to operate on them
    individually all over the place.
    """
    def __init__(self, worker, connector_name, prefetch_count=None,
                 middlewares=None):
        self.name = connector_name
        self.worker = worker
        self._consumers = {}
        self._publishers = {}
        self._endpoint_handlers = {}
        self._default_handlers = {}
        self._prefetch_count = prefetch_count
        self._middlewares = MiddlewareStack(middlewares
                                            if middlewares is not None else [])

    def _rkey(self, mtype):
        return '%s.%s' % (self.name, mtype)

    def setup(self):
        raise NotImplementedError()

    def teardown(self):
        d = gatherResults([c.stop() for c in self._consumers.values()])
        d.addCallback(lambda r: self._middlewares.teardown())
        return d

    @property
    def paused(self):
        return all(consumer.paused
                   for consumer in self._consumers.itervalues())

    def pause(self):
        return gatherResults([
            consumer.pause() for consumer in self._consumers.itervalues()])

    def unpause(self):
        # This doesn't return a deferred.
        for consumer in self._consumers.values():
            consumer.unpause()

    @inlineCallbacks
    def _setup_publisher(self, mtype):
        publisher = yield self.worker.publish_to(self._rkey(mtype))
        self._publishers[mtype] = publisher
        returnValue(publisher)

    @inlineCallbacks
    def _setup_consumer(self, mtype, msg_class, default_handler):
        def handler(msg):
            return self._consume_message(mtype, msg)

        consumer = yield self.worker.consume(
            self._rkey(mtype), handler, message_class=msg_class, paused=True,
            prefetch_count=self._prefetch_count)
        self._consumers[mtype] = consumer
        self._set_default_endpoint_handler(mtype, default_handler)
        returnValue(consumer)

    def _set_endpoint_handler(self, mtype, handler, endpoint_name):
        if endpoint_name is None:
            endpoint_name = TransportMessage.DEFAULT_ENDPOINT_NAME
        handlers = self._endpoint_handlers.setdefault(mtype, {})
        handlers[endpoint_name] = handler

    def _set_default_endpoint_handler(self, mtype, handler):
        self._endpoint_handlers.setdefault(mtype, {})
        self._default_handlers[mtype] = handler

    def _consume_message(self, mtype, msg):
        endpoint_name = msg.get_routing_endpoint()
        handler = self._endpoint_handlers[mtype].get(endpoint_name)
        if handler is None:
            handler = self._default_handlers.get(mtype)
        d = self._middlewares.apply_consume(mtype, msg, self.name)
        d.addCallback(handler)
        return d.addErrback(self._ignore_message, msg)

    def _publish_message(self, mtype, msg, endpoint_name):
        if endpoint_name is not None:
            msg.set_routing_endpoint(endpoint_name)
        d = self._middlewares.apply_publish(mtype, msg, self.name)
        return d.addCallback(self._publishers[mtype].publish_message)

    def _ignore_message(self, failure, msg):
        failure.trap(IgnoreMessage)
        log.debug("Ignoring msg due to %r: %r" % (failure.value, msg))


class ReceiveInboundConnector(BaseConnector):
    def setup(self):
        outbound_d = self._setup_publisher('outbound')
        inbound_d = self._setup_consumer('inbound', TransportUserMessage,
                                         self.default_inbound_handler)
        event_d = self._setup_consumer('event', TransportEvent,
                                       self.default_event_handler)
        return gatherResults([outbound_d, inbound_d, event_d])

    def default_inbound_handler(self, msg):
        log.warning("No inbound handler for %r: %r" % (self.name, msg))

    def default_event_handler(self, msg):
        log.warning("No event handler for %r: %r" % (self.name, msg))

    def set_inbound_handler(self, handler, endpoint_name=None):
        self._set_endpoint_handler('inbound', handler, endpoint_name)

    def set_default_inbound_handler(self, handler):
        self._set_default_endpoint_handler('inbound', handler)

    def set_event_handler(self, handler, endpoint_name=None):
        self._set_endpoint_handler('event', handler, endpoint_name)

    def set_default_event_handler(self, handler):
        self._set_default_endpoint_handler('event', handler)

    def publish_outbound(self, msg, endpoint_name=None):
        return self._publish_message('outbound', msg, endpoint_name)


class ReceiveOutboundConnector(BaseConnector):
    def setup(self):
        inbound_d = self._setup_publisher('inbound')
        event_d = self._setup_publisher('event')
        outbound_d = self._setup_consumer('outbound', TransportUserMessage,
                                          self.default_outbound_handler)
        return gatherResults([outbound_d, inbound_d, event_d])

    def default_outbound_handler(self, msg):
        log.warning("No outbound handler for %r: %r" % (self.name, msg))

    def set_outbound_handler(self, handler, endpoint_name=None):
        self._set_endpoint_handler('outbound', handler, endpoint_name)

    def set_default_outbound_handler(self, handler):
        self._set_default_endpoint_handler('outbound', handler)

    def publish_inbound(self, msg, endpoint_name=None):
        return self._publish_message('inbound', msg, endpoint_name)

    def publish_event(self, msg, endpoint_name=None):
        return self._publish_message('event', msg, endpoint_name)

    def _ignore_message(self, failure, msg):
        failure.trap(IgnoreMessage)
        log.debug("Ignoring msg (with NACK) due to %r: %r" % (
            failure.value, msg))
        return self.publish_event(TransportEvent(
            user_message_id=msg['message_id'], nack_reason=str(failure.value),
            event_type='nack'))
