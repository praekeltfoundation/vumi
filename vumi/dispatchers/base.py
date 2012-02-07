# -*- test-case-name: vumi.dispatchers.tests.test_base -*-

"""Basic tools for building dispatchers."""

import re
import redis

from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.service import Worker
from vumi.message import TransportUserMessage, TransportEvent
from vumi.utils import load_class_by_string


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


class ToAddrRouter(BaseDispatchRouter):
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

    def dispatch_outbound_message(self, msg):
        name = msg['transport_name']
        self.dispatcher.transport_publisher[name].publish_message(msg)

class UserGroupingRouter(BaseDispatchRouter):
    """
    Router that dispatches based on msg from_addr.

    :type group_mappings: dict
    :param group_mappings:
        Mapping of group names to transport_names.
        If a user is assigned to a given group the
        message is sent to the application listening
        on the given transport_name.
    """

    def __init__(self, dispatcher, config):
        self.r_config = config.get('redis_config', {})
        self.r_prefix = config['dispatcher_name']
        self.r_server = redis.Redis(**self.r_config)
        super(UserGroupingRouter, self).__init__(dispatcher, config)

    def setup_routing(self):
        self.set_groups(self.config['group_mappings'])
        self.nr_of_groups = len(self.config['group_mappings'])

    def set_groups(self, group_mappings):
        groups_key = self.r_key('groups')
        self.r_server.hmset(groups_key, group_mappings)
        return self.get_groups()

    def get_groups(self):
        groups_key = self.r_key('groups')
        return self.r_server.hgetall(groups_key)

    def get_counter(self):
        counter_key = self.r_key('round-robin')
        return self.r_server.incr(counter_key) - 1

    def get_next_group(self):
        counter = self.get_counter()
        current_group_id = counter % self.nr_of_groups
        group = self.get_groups().items()[current_group_id]
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
        group = self.get_group_for_user(msg.user())
        app = self.get_groups()[group]
        self.dispatcher.exposed_publisher[app].publish_message(msg)

    def dispatch_outbound_message(self, msg):
        name = msg['transport_name']
        self.dispatcher.transport_publisher[name].publish_message(msg)