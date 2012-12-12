# -*- test-case-name: vumi.dispatchers.tests.test_load_balancer -*-

"""Router for round-robin load balancing between two transports."""

import itertools

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
    """

    def setup_routing(self):
        self.reply_affinity = self.config.get('reply_affinity', True)
        if len(self.dispatcher.exposed_names) != 1:
            raise ConfigError("Only one exposed name allowed for %s." %
                              (type(self).__name__,))
        [self.exposed_name] = self.dispatcher.exposed_names
        if not self.dispatcher.transport_names:
            raise ConfigError("At least on transport name is needed for %s" %
                              (type(self).__name__,))
        self.transport_names = itertools.cycle(self.dispatcher.transport_names)

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
        self.dispatcher.publish_inbound_message(self.exposed_name, msg)

    def dispatch_inbound_event(self, msg):
        self.dispatcher.publish_inbound_event(self.exposed_name, msg)

    def dispatch_outbound_message(self, msg):
        transport_name = None
        if self.reply_affinity:
            transport_name = self.pop_transport_name(msg)
        if not transport_name:
            transport_name = self.transport_names.next()
        self.dispatcher.publish_outbound_message(transport_name, msg)
