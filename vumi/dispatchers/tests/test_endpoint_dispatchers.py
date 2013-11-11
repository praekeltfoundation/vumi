from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.endpoint_dispatchers import RoutingTableDispatcher
from vumi.dispatchers.tests.helpers import DispatcherHelper
from vumi.tests.helpers import VumiTestCase


class TestRoutingTableDispatcher(VumiTestCase):

    def setUp(self):
        self.disp_helper = DispatcherHelper(RoutingTableDispatcher, self)
        self.add_cleanup(self.disp_helper.cleanup)

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
        return self.disp_helper.get_dispatcher(config)

    def ch(self, connector_name):
        return self.disp_helper.get_connector_helper(connector_name)

    def assert_rkeys_used(self, *rkeys):
        broker = self.disp_helper.worker_helper.broker
        self.assertEqual(set(rkeys), set(broker.dispatched['vumi'].keys()))

    def assert_dispatched_endpoint(self, msg, endpoint, dispatched_msgs):
        msg.set_routing_endpoint(endpoint)
        self.assertEqual([msg], dispatched_msgs)

    @inlineCallbacks
    def test_inbound_message_routing(self):
        yield self.get_dispatcher()
        msg = yield self.ch("transport1").make_dispatch_inbound("inbound")
        self.assert_rkeys_used('transport1.inbound', 'app1.inbound')
        self.assert_dispatched_endpoint(
            msg, 'default', self.ch('app1').get_dispatched_inbound())

        self.disp_helper.clear_all_dispatched()
        msg = yield self.ch("transport2").make_dispatch_inbound("inbound")
        self.assert_rkeys_used('transport2.inbound', 'app2.inbound')
        self.assert_dispatched_endpoint(
            msg, 'default', self.ch('app2').get_dispatched_inbound())

        self.disp_helper.clear_all_dispatched()
        msg = yield self.ch("transport2").make_dispatch_inbound(
            "inbound", endpoint='ep1')
        self.assert_rkeys_used('transport2.inbound', 'app1.inbound')
        self.assert_dispatched_endpoint(
            msg, 'ep1', self.ch('app1').get_dispatched_inbound())

    @inlineCallbacks
    def test_outbound_message_routing(self):
        yield self.get_dispatcher()
        msg = yield self.ch('app1').make_dispatch_outbound("outbound")
        self.assert_rkeys_used('app1.outbound', 'transport1.outbound')
        self.assert_dispatched_endpoint(
            msg, 'default', self.ch('transport1').get_dispatched_outbound())

        self.disp_helper.clear_all_dispatched()
        msg = yield self.ch('app2').make_dispatch_outbound("outbound")
        self.assert_rkeys_used('app2.outbound', 'transport2.outbound')
        self.assert_dispatched_endpoint(
            msg, 'default', self.ch('transport2').get_dispatched_outbound())

        self.disp_helper.clear_all_dispatched()
        msg = yield self.ch('app1').make_dispatch_outbound(
            "outbound", endpoint='ep2')
        self.assert_rkeys_used('app1.outbound', 'transport2.outbound')
        self.assert_dispatched_endpoint(
            msg, 'default', self.ch('transport2').get_dispatched_outbound())

    @inlineCallbacks
    def test_inbound_event_routing(self):
        yield self.get_dispatcher()
        msg = yield self.ch('transport1').make_dispatch_ack()
        self.assert_rkeys_used('transport1.event', 'app1.event')
        self.assert_dispatched_endpoint(
            msg, 'default', self.ch('app1').get_dispatched_events())

        self.disp_helper.clear_all_dispatched()
        msg = yield self.ch('transport2').make_dispatch_ack()
        self.assert_rkeys_used('transport2.event', 'app2.event')
        self.assert_dispatched_endpoint(
            msg, 'default', self.ch('app2').get_dispatched_events())

        self.disp_helper.clear_all_dispatched()
        msg = yield self.ch('transport2').make_dispatch_ack(endpoint='ep1')
        self.assert_rkeys_used('transport2.event', 'app1.event')
        self.assert_dispatched_endpoint(
            msg, 'ep1', self.ch('app1').get_dispatched_events())

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
