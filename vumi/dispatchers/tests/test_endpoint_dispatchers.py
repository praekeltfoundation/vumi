from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.dispatchers.endpoint_dispatchers import RoutingTableDispatcher
from vumi.tests.utils import VumiWorkerTestCase
from vumi.tests.helpers import MessageHelper


class TestRoutingTableDispatcher(VumiWorkerTestCase):

    def setUp(self):
        self.msg_helper = MessageHelper()
        return super(TestRoutingTableDispatcher, self).setUp()

    @inlineCallbacks
    def get_dispatcher(self, **config_extras):
        config = {
            "receive_inbound_connectors": ["transport1", "transport2"],
            "receive_outbound_connectors": ["app1", "app2"],
            "routing_table": {
                "transport1": {
                    "default": ["app1", "default"],
                    },
                "transport2": {
                    "default": ["app2", "default"],
                    "ep1": ["app1", "ep1"],
                    },
                "app1": {
                    "default": ["transport1", "default"],
                    "ep2": ["transport2", "default"],
                    },
                "app2": {
                    "default": ["transport2", "default"],
                    },
                },
            }
        config.update(config_extras)
        dispatcher = yield self.get_worker(config, RoutingTableDispatcher)
        returnValue(dispatcher)

    def with_endpoint(self, msg, endpoint=None):
        msg.set_routing_endpoint(endpoint)
        return msg

    def assert_rkeys_used(self, *rkeys):
        self.assertEqual(set(rkeys), set(self._amqp.dispatched['vumi'].keys()))

    @inlineCallbacks
    def test_inbound_message_routing(self):
        yield self.get_dispatcher()
        msg = self.msg_helper.make_inbound("inbound")
        yield self.dispatch_inbound(msg, 'transport1')
        self.assert_rkeys_used('transport1.inbound', 'app1.inbound')
        self.assertEqual(
            [self.with_endpoint(msg)], self.get_dispatched_inbound('app1'))

        self.clear_all_dispatched()
        msg = self.msg_helper.make_inbound("inbound")
        yield self.dispatch_inbound(msg, 'transport2')
        self.assert_rkeys_used('transport2.inbound', 'app2.inbound')
        self.assertEqual(
            [self.with_endpoint(msg)], self.get_dispatched_inbound('app2'))

        self.clear_all_dispatched()
        msg = self.msg_helper.make_inbound("inbound")
        msg.set_routing_endpoint('ep1')
        yield self.dispatch_inbound(msg, 'transport2')
        self.assert_rkeys_used('transport2.inbound', 'app1.inbound')
        self.assertEqual(
            [self.with_endpoint(msg, 'ep1')],
            self.get_dispatched_inbound('app1'))

    @inlineCallbacks
    def test_outbound_message_routing(self):
        yield self.get_dispatcher()
        msg = self.msg_helper.make_inbound("inbound")
        yield self.dispatch_outbound(msg, 'app1')
        self.assert_rkeys_used('app1.outbound', 'transport1.outbound')
        self.assertEqual(
            [self.with_endpoint(msg)],
            self.get_dispatched_outbound('transport1'))

        self.clear_all_dispatched()
        msg = self.msg_helper.make_inbound("inbound")
        yield self.dispatch_outbound(msg, 'app2')
        self.assert_rkeys_used('app2.outbound', 'transport2.outbound')
        self.assertEqual(
            [self.with_endpoint(msg)],
            self.get_dispatched_outbound('transport2'))

        self.clear_all_dispatched()
        msg = self.msg_helper.make_inbound("inbound")
        msg.set_routing_endpoint('ep2')
        yield self.dispatch_outbound(msg, 'app1')
        self.assert_rkeys_used('app1.outbound', 'transport2.outbound')
        self.assertEqual(
            [self.with_endpoint(msg)],
            self.get_dispatched_outbound('transport2'))

    @inlineCallbacks
    def test_inbound_event_routing(self):
        yield self.get_dispatcher()
        msg = self.msg_helper.make_ack()
        yield self.dispatch_event(msg, 'transport1')
        self.assert_rkeys_used('transport1.event', 'app1.event')
        self.assertEqual(
            [self.with_endpoint(msg)], self.get_dispatched_events('app1'))

        self.clear_all_dispatched()
        msg = self.msg_helper.make_ack()
        yield self.dispatch_event(msg, 'transport2')
        self.assert_rkeys_used('transport2.event', 'app2.event')
        self.assertEqual(
            [self.with_endpoint(msg)], self.get_dispatched_events('app2'))

        self.clear_all_dispatched()
        msg = self.msg_helper.make_ack()
        msg.set_routing_endpoint('ep1')
        yield self.dispatch_event(msg, 'transport2')
        self.assert_rkeys_used('transport2.event', 'app1.event')
        self.assertEqual(
            [self.with_endpoint(msg, 'ep1')],
            self.get_dispatched_events('app1'))

    def get_dispatcher_consumers(self, dispatcher):
        consumers = []
        for conn in dispatcher.connectors.values():
            consumers.extend(conn._consumers.values())
        return consumers

    @inlineCallbacks
    def test_consumer_prefetch_count_default(self):
        dp = yield self.get_dispatcher()
        consumers = self.get_dispatcher_consumers(dp)
        for consumer in consumers:
            self.assertEqual(consumer.channel.qos_prefetch_count, 20)

    @inlineCallbacks
    def test_consumer_prefetch_count_custom(self):
        dp = yield self.get_dispatcher(amqp_prefetch_count=10)
        consumers = self.get_dispatcher_consumers(dp)
        for consumer in consumers:
            self.assertEqual(consumer.channel.qos_prefetch_count, 10)

    @inlineCallbacks
    def test_consumer_prefetch_count_none(self):
        dp = yield self.get_dispatcher(amqp_prefetch_count=None)
        consumers = self.get_dispatcher_consumers(dp)
        for consumer in consumers:
            self.assertFalse(consumer.channel.qos_prefetch_count)
