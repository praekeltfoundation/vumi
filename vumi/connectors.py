# TODO: We use gatherResults(), which requires Twisted>=11.1
#       We need to either implement a fallback (which is easy enough) or
#       explicitly require a suitable Twisted version.

from twisted.internet.defer import gatherResults

from vumi import log
from vumi.message import TransportMessage, TransportEvent, TransportUserMessage
from vumi.middleware import MiddlewareStack


def cb_add_to_dict(r, dict_obj, key):
    dict_obj[key] = r
    return r


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

    def pause(self):
        # This doesn't return a deferred.
        for consumer in self._consumers.values():
            consumer.pause()

    def unpause(self):
        # This doesn't return a deferred.
        for consumer in self._consumers.values():
            consumer.unpause()

    def _setup_publisher(self, mtype):
        d = self.worker.publish_to(self._rkey(mtype))
        return d.addCallback(cb_add_to_dict, self._publishers, mtype)

    def _set_prefetch_count(self, consumer):
        if self._prefetch_count is not None:
            consumer.channel.basic_qos(0, self._prefetch_count, False)

    def _setup_consumer(self, mtype, msg_class):
        def handler(msg):
            return self._consume_message(mtype, msg)
        d = self.worker.consume(
            self._rkey(mtype), handler, message_class=msg_class, paused=True)
        d.addCallback(cb_add_to_dict, self._consumers, mtype)
        d.addCallback(lambda consumer: self._set_prefetch_count(consumer))
        return d

    def _set_endpoint_handler(self, mtype, handler, endpoint_name):
        if endpoint_name is None:
            endpoint_name = TransportMessage.DEFAULT_ENDPOINT_NAME
        handlers = self._endpoint_handlers.setdefault(mtype, {})
        handlers[endpoint_name] = handler

    def _consume_message(self, mtype, msg):
        endpoint_name = msg.get_routing_endpoint()
        handler = self._endpoint_handlers[mtype].get(endpoint_name)
        if handler is None:
            handler = self._default_handlers[mtype]
        d = self._middlewares.apply_consume(mtype, msg, self.name)
        return d.addCallback(handler)

    def _publish_message(self, mtype, msg, endpoint_name):
        if endpoint_name is not None:
            msg.set_routing_endpoint(endpoint_name)
        d = self._middlewares.apply_publish(mtype, msg, self.name)
        return d.addCallback(self._publishers[mtype].publish_message)


class ReceiveInboundConnector(BaseConnector):
    def setup(self):
        outbound_d = self._setup_publisher('outbound')
        inbound_d = self._setup_consumer('inbound', TransportUserMessage)
        event_d = self._setup_consumer('event', TransportEvent)
        self.set_default_inbound_handler(self.default_inbound_handler)
        self.set_default_event_handler(self.default_event_handler)
        return gatherResults([outbound_d, inbound_d, event_d])

    def default_inbound_handler(self, msg):
        log.warning("No inbound handler for %r: %r" % (self.name, msg))

    def default_event_handler(self, msg):
        log.warning("No event handler for %r: %r" % (self.name, msg))

    def set_inbound_handler(self, handler, endpoint_name=None):
        self._set_endpoint_handler('inbound', handler, endpoint_name)

    def set_default_inbound_handler(self, handler):
        self._endpoint_handlers.setdefault('inbound', {})
        self._default_handlers['inbound'] = handler

    def set_event_handler(self, handler, endpoint_name=None):
        self._set_endpoint_handler('event', handler, endpoint_name)

    def set_default_event_handler(self, handler):
        self._endpoint_handlers.setdefault('event', {})
        self._default_handlers['event'] = handler

    def publish_outbound(self, msg, endpoint_name=None):
        return self._publish_message('outbound', msg, endpoint_name)


class ReceiveOutboundConnector(BaseConnector):
    def setup(self):
        inbound_d = self._setup_publisher('inbound')
        event_d = self._setup_publisher('event')
        outbound_d = self._setup_consumer('outbound', TransportUserMessage)
        self.set_default_outbound_handler(self.default_outbound_handler)
        return gatherResults([outbound_d, inbound_d, event_d])

    def default_outbound_handler(self, msg):
        log.warning("No outbound handler for %r: %r" % (self.name, msg))

    def set_outbound_handler(self, handler, endpoint_name=None):
        self._set_endpoint_handler('outbound', handler, endpoint_name)

    def set_default_outbound_handler(self, handler):
        self._endpoint_handlers.setdefault('outbound', {})
        self._default_handlers['outbound'] = handler

    def publish_inbound(self, msg, endpoint_name=None):
        return self._publish_message('inbound', msg, endpoint_name)

    def publish_event(self, msg, endpoint_name=None):
        return self._publish_message('event', msg, endpoint_name)
