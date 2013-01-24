# -*- test-case-name: vumi.dispatchers.tests.test_load_balancer -*-

"""Router for round-robin load balancing between two transports."""

import itertools

from vumi import log
from vumi.errors import ConfigError
from vumi.dispatchers.base import BaseDispatchRouter


class LoadBalancingRouter(BaseDispatchRouter):
    """Router that does round-robin dispatching to transports.

    Supports only one exposed name and requires at least one transport
    name.

    Configuration options:

    :param bool reply_affinity:
        If set to true, replies are sent back to the same transport
        they were sent from. If false, replies are round-robinned in
        the same way other outbound messages are. Default: true.
    :param bool rewrite_transport_name:
        If set to true, rewrites message `transport_names` in both
        directions. Default: true.
    """

    def setup_routing(self):
        self.reply_affinity = self.config.get('reply_affinity', True)
        self.rewrite_transport_names = self.config.get(
            'rewrite_transport_names', True)
        if len(self.dispatcher.exposed_names) != 1:
            raise ConfigError("Only one exposed name allowed for %s." %
                              (type(self).__name__,))
        [self.exposed_name] = self.dispatcher.exposed_names
        if not self.dispatcher.transport_names:
            raise ConfigError("At least one transport name is needed for %s" %
                              (type(self).__name__,))
        self.transport_name_cycle = itertools.cycle(
            self.dispatcher.transport_names)
        self.transport_name_set = set(self.dispatcher.transport_names)

    def push_transport_name(self, msg, transport_name):
        hm = msg['helper_metadata']
        lm = hm.setdefault('load_balancer', {})
        transport_names = lm.setdefault('transport_names', [])
        transport_names.append(transport_name)

    def pop_transport_name(self, msg):
        hm = msg['helper_metadata']
        lm = hm.get('load_balancer', {})
        transport_names = lm.get('transport_names', [])
        if not transport_names:
            return None
        return transport_names.pop()

    def dispatch_inbound_message(self, msg):
        if self.reply_affinity:
            # TODO: we should really be pushing the endpoint name
            #       but it isn't available here
            self.push_transport_name(msg, msg['transport_name'])
        if self.rewrite_transport_names:
            msg['transport_name'] = self.exposed_name
        self.dispatcher.publish_inbound_message(self.exposed_name, msg)

    def dispatch_inbound_event(self, msg):
        if self.rewrite_transport_names:
            msg['transport_name'] = self.exposed_name
        self.dispatcher.publish_inbound_event(self.exposed_name, msg)

    def dispatch_outbound_message(self, msg):
        if self.reply_affinity and msg['in_reply_to']:
            transport_name = self.pop_transport_name(msg)
            if transport_name not in self.transport_name_set:
                log.warning("LoadBalancer is configured for reply affinity but"
                            " reply for unknown load balancer endpoint %r was"
                            " was received. Using round-robin routing instead."
                            % (transport_name,))
                transport_name = self.transport_name_cycle.next()
        else:
            transport_name = self.transport_name_cycle.next()
        if self.rewrite_transport_names:
            msg['transport_name'] = transport_name
        self.dispatcher.publish_outbound_message(transport_name, msg)
