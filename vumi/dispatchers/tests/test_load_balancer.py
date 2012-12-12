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
            "exposed_names": ["round_robin"],
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

    def test_inbound_message_routing(self):
        msg1 = self.mkmsg_in(content='msg 1', transport_name='transport_1')
        self.router.dispatch_inbound_message(msg1)
        msg2 = self.mkmsg_in(content='msg 2', transport_name='transport_2')
        self.router.dispatch_inbound_message(msg2)
        publishers = self.dispatcher.exposed_publisher
        self.assertEqual(publishers['round_robin'].msgs, [msg1, msg2])

    def test_inbound_event_routing(self):
        msg1 = self.mkmsg_ack(transport_name='transport_1')
        self.router.dispatch_inbound_event(msg1)
        msg2 = self.mkmsg_ack(transport_name='transport_2')
        self.router.dispatch_inbound_event(msg2)
        publishers = self.dispatcher.exposed_event_publisher
        self.assertEqual(publishers['round_robin'].msgs, [msg1, msg2])

    def test_outbound_message_routing(self):
        msg1 = self.mkmsg_out(content='msg 1')
        self.router.dispatch_outbound_message(msg1)
        msg2 = self.mkmsg_out(content='msg 2')
        self.router.dispatch_outbound_message(msg2)
        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport_1'].msgs, [msg1])
        self.assertEqual(publishers['transport_2'].msgs, [msg2])
