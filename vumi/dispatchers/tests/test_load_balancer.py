"""Tests for vumi.dispatchers.load_balancer."""

from twisted.internet.defer import inlineCallbacks

from vumi.tests.utils import VumiWorkerTestCase
from vumi.dispatchers.tests.utils import DummyDispatcher
from vumi.dispatchers.load_balancer import LoadBalancingRouter


class TestLoadBalancingRouter(VumiWorkerTestCase):

    @inlineCallbacks
    def setUp(self):
        yield super(TestLoadBalancingRouter, self).setUp()
        config = {
            "transport_names": [
                "transport_1",
                "transport_2",
            ],
            "exposed_names": ["muxed"],
            "router_class": ("vumi.dispatchers.load_balancer."
                             "LoadBalancingRouter"),
        }
        self.dispatcher = DummyDispatcher(config)
        self.router = LoadBalancingRouter(self.dispatcher, config)
        yield self.router.setup_routing()

    @inlineCallbacks
    def tearDown(self):
        yield super(TestLoadBalancingRouter, self).tearDown()
        yield self.router.teardown_routing()

    def mkmsg_in_mux(self, content, from_addr, transport_name):
        return self.mkmsg_in(transport_name=transport_name, content=content,
                             from_addr=from_addr)

    def mkmsg_ack_mux(self, from_addr, transport_name):
        ack = self.mkmsg_ack(transport_name=transport_name)
        ack['from_addr'] = from_addr
        return ack

    def mkmsg_out_mux(self, content, from_addr):
        return self.mkmsg_out(transport_name='muxed', content=content,
                              from_addr=from_addr)

    def test_inbound_message_routing(self):
        msg1 = self.mkmsg_in_mux('mux 1', 'thing1@muxme', 'transport_1')
        self.router.dispatch_inbound_message(msg1)
        msg2 = self.mkmsg_in_mux('mux 2', 'thing2@muxme', 'transport_2')
        self.router.dispatch_inbound_message(msg2)
        publishers = self.dispatcher.exposed_publisher
        self.assertEqual(publishers['muxed'].msgs, [msg1, msg2])

    def test_inbound_event_routing(self):
        msg1 = self.mkmsg_ack_mux('thing1@muxme', 'transport_1')
        self.router.dispatch_inbound_event(msg1)
        msg2 = self.mkmsg_ack_mux('thing2@muxme', 'transport_2')
        self.router.dispatch_inbound_event(msg2)
        publishers = self.dispatcher.exposed_event_publisher
        self.assertEqual(publishers['muxed'].msgs, [msg1, msg2])

    def test_outbound_message_routing(self):
        msg1 = self.mkmsg_out_mux('mux 1', 'thing1@muxme')
        self.router.dispatch_outbound_message(msg1)
        msg2 = self.mkmsg_out_mux('mux 2', 'thing2@muxme')
        self.router.dispatch_outbound_message(msg2)
        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport_1'].msgs, [msg1])
        self.assertEqual(publishers['transport_2'].msgs, [msg2])
