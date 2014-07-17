from twisted.python.failure import Failure
from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.endpoint_dispatchers import (
    Dispatcher, RoutingTableDispatcher)
from vumi.dispatchers.tests.helpers import DispatcherHelper
from vumi.tests.utils import LogCatcher
from vumi.tests.helpers import VumiTestCase


class DummyError(Exception):
    """Custom exception to use in test cases."""


class TestDispatcher(VumiTestCase):

    def setUp(self):
        self.disp_helper = self.add_helper(DispatcherHelper(Dispatcher))

    def get_dispatcher(self, **config_extras):
        config = {
            "receive_inbound_connectors": ["transport1", "transport2"],
            "receive_outbound_connectors": ["app1", "app2"],
            }
        config.update(config_extras)
        return self.disp_helper.get_dispatcher(config)

    def ch(self, connector_name):
        return self.disp_helper.get_connector_helper(connector_name)

    @inlineCallbacks
    def test_default_errback(self):
        disp = yield self.get_dispatcher()
        msg = self.disp_helper.make_inbound('bad')
        f = Failure(DummyError("worse"))
        with LogCatcher() as lc:
            yield disp.default_errback(f, msg, "app1")
            [err1] = lc.errors
        self.assertEqual(
            err1['why'], "Error routing message for app1")
        self.assertEqual(
            err1['failure'], f)
        self.flushLoggedErrors(DummyError)

    @inlineCallbacks
    def check_errback_called(self, method_to_raise, errback_method, direction):
        dummy_error = DummyError("eep")

        def raiser(*args):
            raise dummy_error

        errors = []

        def record_error(self, f, msg, connector_name):
            errors.append((f, msg, connector_name))

        raiser_patch = self.patch(Dispatcher, method_to_raise, raiser)
        recorder_patch = self.patch(Dispatcher, errback_method, record_error)
        disp = yield self.get_dispatcher()

        if direction == 'inbound':
            connector_name = 'transport1'
            msg = yield self.ch('transport1').make_dispatch_inbound("inbound")
        elif direction == 'outbound':
            connector_name = 'app1'
            msg = yield self.ch('app1').make_dispatch_outbound("outbound")
        elif direction == 'event':
            connector_name = 'transport1'
            msg = yield self.ch('transport1').make_dispatch_ack()
        else:
            raise ValueError(
                "Unexcepted value %r for direction" % (direction,))

        [(err_f, err_msg, err_connector)] = errors
        self.assertEqual(err_f.value, dummy_error)
        self.assertEqual(err_msg, msg)
        self.assertEqual(err_connector, connector_name)

        yield self.disp_helper.cleanup_worker(disp)
        raiser_patch.restore()
        recorder_patch.restore()

    @inlineCallbacks
    def test_inbound_errbacks(self):
        for err_method in ('process_inbound', 'get_config'):
            yield self.check_errback_called(
                err_method, 'default_errback', 'inbound')
            yield self.check_errback_called(
                err_method, 'errback_inbound', 'inbound')

    @inlineCallbacks
    def test_outbound_errbacks(self):
        for err_method in ('process_outbound', 'get_config'):
            yield self.check_errback_called(
                err_method, 'default_errback', 'outbound')
            yield self.check_errback_called(
                err_method, 'errback_outbound', 'outbound')

    @inlineCallbacks
    def test_event_errbacks(self):
        for err_method in ('process_event', 'get_config'):
            yield self.check_errback_called(
                err_method, 'default_errback', 'event')
            yield self.check_errback_called(
                err_method, 'errback_event', 'event')


class TestRoutingTableDispatcher(VumiTestCase):

    def setUp(self):
        self.disp_helper = self.add_helper(
            DispatcherHelper(RoutingTableDispatcher))

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
