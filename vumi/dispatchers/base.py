# -*- test-case-name: vumi.dispatchers.tests.test_base -*-

"""Basic tools for building dispatchers."""

import re
import redis

from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.service import Worker
from vumi.errors import ConfigError
from vumi.message import TransportUserMessage, TransportEvent
from vumi.utils import load_class_by_string, get_first_word
from vumi import log


class BaseDispatchWorker(Worker):
    """Base class for a dispatch worker.

    """

    @inlineCallbacks
    def startWorker(self):
        log.msg('Starting a %s dispatcher with config: %s'
                % (self.__class__.__name__, self.config))

        yield self.setup_endpoints()
        yield self.setup_router()
        yield self.setup_transport_publishers()
        yield self.setup_exposed_publishers()
        yield self.setup_transport_consumers()
        yield self.setup_exposed_consumers()

    def setup_endpoints(self):
        self._transport_names = self.config.get('transport_names', [])
        self._exposed_names = self.config.get('exposed_names', [])

    def setup_router(self):
        router_cls = load_class_by_string(self.config['router_class'])
        self._router = router_cls(self, self.config)

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
                self.dispatch_inbound_message,
                message_class=TransportUserMessage)
        for transport_name in self._transport_names:
            self.transport_event_consumer[transport_name] = yield self.consume(
                '%s.event' % (transport_name,),
                self.dispatch_inbound_event,
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

    def dispatch_inbound_message(self, msg):
        return self._router.dispatch_inbound_message(msg)

    def dispatch_inbound_event(self, msg):
        return self._router.dispatch_inbound_event(msg)

    def dispatch_outbound_message(self, msg):
        return self._router.dispatch_outbound_message(msg)


class BaseDispatchRouter(object):
    """Base class for dispatch routing logic.
    """

    def __init__(self, dispatcher, config):
        self.dispatcher = dispatcher
        self.config = config
        self.setup_routing()

    def setup_routing(self):
        """Setup any things needed for routing."""
        pass

    def dispatch_inbound_message(self, msg):
        raise NotImplementedError()

    def dispatch_inbound_event(self, msg):
        raise NotImplementedError()

    def dispatch_outbound_message(self, msg):
        raise NotImplementedError()


class SimpleDispatchRouter(BaseDispatchRouter):
    """Simple dispatch router that maps transports to apps.
    """

    def dispatch_inbound_message(self, msg):
        names = self.config['route_mappings'][msg['transport_name']]
        for name in names:
            self.dispatcher.exposed_publisher[name].publish_message(msg)

    def dispatch_inbound_event(self, msg):
        names = self.config['route_mappings'][msg['transport_name']]
        for name in names:
            self.dispatcher.exposed_event_publisher[name].publish_message(msg)

    def dispatch_outbound_message(self, msg):
        name = msg['transport_name']
        name = self.config.get('transport_mappings', {}).get(name, name)
        self.dispatcher.transport_publisher[name].publish_message(msg)


class TransportToTransportRouter(BaseDispatchRouter):
    """Simple dispatch router that maps transports to apps.
    """

    def dispatch_inbound_message(self, msg):
        names = self.config['route_mappings'][msg['transport_name']]
        for name in names:
            rkey = '%s.outbound' % (name,)
            self.dispatcher.transport_publisher[name].publish_message(
                msg, routing_key=rkey)

    def dispatch_inbound_event(self, msg):
        """
        Explicitly throw away events, because transports can't receive them.
        """
        pass

    def dispatch_outbound_message(self, msg):
        """
        If we're only hooking transports up to each other, there are no
        outbound messages.
        """
        pass


class ToAddrRouter(SimpleDispatchRouter):
    """Router that dispatches based on msg to_addr.

    :type toaddr_mappings: dict
    :param toaddr_mappings:
        Mapping from application transport names to regular
        expressions. If a message's to_addr matches the given
        regular expression the message is sent to the applications
        listening on the given transport name.
    """

    def setup_routing(self):
        self.mappings = []
        for name, toaddr_pattern in self.config['toaddr_mappings'].items():
            self.mappings.append((name, re.compile(toaddr_pattern)))
            # TODO: assert that name is in list of publishers.

    def dispatch_inbound_message(self, msg):
        toaddr = msg['to_addr']
        for name, regex in self.mappings:
            if regex.match(toaddr):
                self.dispatcher.exposed_publisher[name].publish_message(msg)

    def dispatch_inbound_event(self, msg):
        pass
        # TODO:
        #   Use msg['user_message_id'] to look up where original message
        #   was dispatched to and dispatch this message there
        #   Perhaps there should be a message on the base class to support
        #   this.


class FromAddrMultiplexRouter(BaseDispatchRouter):
    """Router that multiplexes multiple transports based on msg from_addr.

    :param dict fromaddr_mappings:
        Mapping from message `from_addr` to `transport_name`.

    This router is intended to be used to multiplex a pool of transports that
    each only supports a single external address, and present them to
    applications (or downstream dispatchers) as a single transport that
    supports multiple external addresses. This is useful for multiplexing
    :class:`vumi.transports.xmpp.XMPPTransport` instances, for example.

    NOTE: This router rewrites `transport_name` in both directions. Also, only
    one exposed name is supported.
    """

    def setup_routing(self):
        if len(self.config['exposed_names']) != 1:
            raise ConfigError("Only one exposed name allowed for %s." % (
                    type(self).__name__,))
        [self.exposed_name] = self.config['exposed_names']

    def _handle_inbound(self, msg, publisher):
        msg['transport_name'] = self.exposed_name
        publisher.publish_message(msg)

    def dispatch_inbound_message(self, msg):
        self._handle_inbound(
            msg, self.dispatcher.exposed_publisher[self.exposed_name])

    def dispatch_inbound_event(self, msg):
        self._handle_inbound(
            msg, self.dispatcher.exposed_event_publisher[self.exposed_name])

    def dispatch_outbound_message(self, msg):
        name = self.config['fromaddr_mappings'][msg['from_addr']]
        msg['transport_name'] = name
        self.dispatcher.transport_publisher[name].publish_message(msg)


class UserGroupingRouter(SimpleDispatchRouter):
    """
    Router that dispatches based on msg `from_addr`. Each unique
    `from_addr` is round-robin assigned to one of the defined
    groups in `group_mappings`. All messages from that
    `from_addr` are then routed to the `app` assigned to that group.

    Useful for A/B testing.

    :type group_mappings: dict
    :param group_mappings:
        Mapping of group names to transport_names.
        If a user is assigned to a given group the
        message is sent to the application listening
        on the given transport_name.

    :type dispatcher_name: str
    :param dispatcher_name:
        The name of the dispatcher, used internally as
        the prefix for Redis keys.
    """

    def __init__(self, dispatcher, config):
        self.r_config = config.get('redis_config', {})
        self.r_prefix = config['dispatcher_name']
        self.r_server = redis.Redis(**self.r_config)
        self.groups = config['group_mappings']
        super(UserGroupingRouter, self).__init__(dispatcher, config)

    def setup_routing(self):
        self.nr_of_groups = len(self.groups)

    def get_counter(self):
        counter_key = self.r_key('round-robin')
        return self.r_server.incr(counter_key) - 1

    def get_next_group(self):
        counter = self.get_counter()
        current_group_id = counter % self.nr_of_groups
        sorted_groups = sorted(self.groups.items())
        group = sorted_groups[current_group_id]
        return group

    def get_group_key(self, group_name):
        return self.r_key('group', group_name)

    def get_user_key(self, user_id):
        return self.r_key('user', user_id)

    def r_key(self, *parts):
        return ':'.join([self.r_prefix] + map(str, parts))

    def get_group_for_user(self, user_id):
        user_key = self.get_user_key(user_id)
        group = self.r_server.get(user_key)
        if not group:
            group, transport_name = self.get_next_group()
            self.r_server.set(user_key, group)
        return group

    def dispatch_inbound_message(self, msg):
        group = self.get_group_for_user(msg.user().encode('utf8'))
        app = self.groups[group]
        self.dispatcher.exposed_publisher[app].publish_message(msg)


class ContentKeywordRouter(SimpleDispatchRouter):
    """Router that dispatches based on msg content first word also named as the
     keyword in the sms context.

    :type keyword_mappings: dict
    :param keyword_mappings:
        Mapping from application transport names to keyword.
        If a message's content first word is matching a given keyword,
        the message is send to the application listenning on the given
         transport name.
    """

    def __init__(self, dispatcher, config):
        self.r_config = config.get('redis_config', {})
        self.r_prefix = config['dispatcher_name']
        self.r_server = redis.Redis(**self.r_config)
        self.keyword_mappings = config['keyword_mappings'].items()
        self.transport_mappings = config['transport_mappings'].items()
        super(ContentKeywordRouter, self).__init__(dispatcher, config)

    def setup_routing(self):
        pass

    def get_message_key(self, message):
        return self.r_key('message', message)

    def r_key(self, *parts):
        return ':'.join([self.r_prefix] + map(str, parts))

    def publish_transport(self, name, msg):
        self.dispatcher.transport_publisher[name].publish_message(msg)

    def publish_exposed_inbound(self, name, msg):
        self.dispatcher.exposed_publisher[name].publish_message(msg)

    def publish_exposed_event(self, name, msg):
        self.dispatcher.exposed_event_publisher[name].publish_message(msg)

    def dispatch_inbound_message(self, msg):
        log.debug('Inbound message')
        msg_keyword = get_first_word(msg['content'])
        if (msg_keyword == ''):
            log.error('Message has not keyword')
            return
        for name, keyword in self.keyword_mappings:
            if (msg_keyword.lower() == keyword.lower()):
                log.debug('Message is routed to %s' % (name,))
                self.publish_exposed_inbound(name, msg)

    def dispatch_inbound_event(self, msg):
        log.debug("Inbound event")
        message_key = self.get_message_key(msg['user_message_id'])
        name = self.r_server.get(message_key)
        if (not name):
            log.error("Not route back tuple stored in Redis for %s"
                  % (msg['user_message_id'],))
        try:
            log.debug('Event is routed to %s' % (name,))
            self.publish_exposed_event(name, msg)
        except:
            log.error("No publishing route for %s" % (name,))

    @inlineCallbacks
    def dispatch_outbound_message(self, msg):
        log.debug("Outbound message")
        if msg['from_addr'] in self.config['transport_mappings'].items():
            log.error("Transport is not registered %s" % (msg['from_addr'],))
            return
        for (name, transport) in self.config['transport_mappings'].items():
            if (name == msg['from_addr']):
                self.publish_transport(transport, msg)
                message_key = self.get_message_key(msg['message_id'])
                self.r_server.set(message_key, msg['transport_name'])
                yield self.r_server.expire(
                    message_key,
                    int(self.config['expire_routing_memory']))
