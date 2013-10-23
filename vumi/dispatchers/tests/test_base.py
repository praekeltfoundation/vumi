from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.dispatchers.base import (
    BaseDispatchWorker, ToAddrRouter, FromAddrMultiplexRouter)
from vumi.tests.utils import VumiWorkerTestCase, LogCatcher
from vumi.dispatchers.tests.utils import DispatcherTestCase, DummyDispatcher
from vumi.tests.helpers import MessageHelper


class TestBaseDispatchWorker(VumiWorkerTestCase):

    def setUp(self):
        self.msg_helper = MessageHelper()
        return super(TestBaseDispatchWorker, self).setUp()

    @inlineCallbacks
    def get_dispatcher(self, **config_extras):
        config = {
            "transport_names": [
                "transport1",
                "transport2",
                "transport3",
                ],
            "exposed_names": [
                "app1",
                "app2",
                "app3",
                ],
            "router_class": "vumi.dispatchers.base.SimpleDispatchRouter",
            "route_mappings": {
                "transport1": ["app1"],
                "transport2": ["app2"],
                "transport3": ["app1", "app3"]
                },
            "middleware": [
                {"mw1": "vumi.middleware.tests.utils.RecordingMiddleware"},
                {"mw2": "vumi.middleware.tests.utils.RecordingMiddleware"},
                ],
            }
        config.update(config_extras)
        dispatcher = yield self.get_worker(config, BaseDispatchWorker)
        returnValue(dispatcher)

    def dispatch(self, message, rkey=None, exchange='vumi'):
        return self._dispatch(message, rkey, exchange)

    def mk_middleware_records(self, rkey_in, rkey_out):
        records = []
        for rkey, direction in [(rkey_in, False), (rkey_out, True)]:
            endpoint, method = rkey.split('.', 1)
            mw = [[name, method, endpoint] for name in ("mw1", "mw2")]
            if direction:
                mw.reverse()
            records.extend(mw)
        return records

    def assert_messages(self, rkeys_in, rkey_out, msgs):
        received_msgs = self._amqp.get_messages('vumi', rkey_out)
        for rmsg, rkey_in in zip(received_msgs, rkeys_in):
            middleware_records = self.mk_middleware_records(rkey_in, rkey_out)
            self.assertEqual(rmsg.payload.pop('record'),
                             middleware_records)
        self.assertEqual(msgs, received_msgs)

    def assert_no_messages(self, *rkeys):
        for rkey in rkeys:
            self.assertEqual([], self._amqp.get_messages('vumi', rkey))

    def clear_dispatched(self):
        self._amqp.dispatched = {}

    @inlineCallbacks
    def test_inbound_message_routing(self):
        yield self.get_dispatcher()
        msg = self.msg_helper.make_inbound("foo", transport_name='transport1')
        yield self.dispatch(msg, 'transport1.inbound')
        self.assert_messages(['transport1.inbound'], 'app1.inbound', [msg])
        self.assert_no_messages('app1.event', 'app2.inbound', 'app2.event',
                                'app3.inbound', 'app3.event')

        self.clear_dispatched()
        msg = self.msg_helper.make_inbound("foo", transport_name='transport2')
        yield self.dispatch(msg, 'transport2.inbound')
        self.assert_messages(['transport2.inbound'], 'app2.inbound', [msg])
        self.assert_no_messages('app1.inbound', 'app1.event', 'app2.event',
                                'app3.inbound', 'app3.event')

        self.clear_dispatched()
        msg = self.msg_helper.make_inbound("foo", transport_name='transport3')
        yield self.dispatch(msg, 'transport3.inbound')
        self.assert_messages(['transport3.inbound'], 'app1.inbound', [msg])
        self.assert_messages(['transport3.inbound'], 'app3.inbound', [msg])
        self.assert_no_messages('app1.event', 'app2.inbound', 'app2.event',
                                'app3.event')

    @inlineCallbacks
    def test_inbound_ack_routing(self):
        yield self.get_dispatcher()
        msg = self.msg_helper.make_ack(transport_name='transport1')
        yield self.dispatch(msg, 'transport1.event')
        self.assert_messages(['transport1.event'], 'app1.event', [msg])
        self.assert_no_messages('app1.inbound', 'app2.event', 'app2.inbound',
                                'app3.event', 'app3.inbound')

        self.clear_dispatched()
        msg = self.msg_helper.make_ack(transport_name='transport2')
        yield self.dispatch(msg, 'transport2.event')
        self.assert_messages(['transport2.event'], 'app2.event', [msg])
        self.assert_no_messages('app1.event', 'app1.inbound', 'app2.inbound',
                                'app3.event', 'app3.inbound')

        self.clear_dispatched()
        msg = self.msg_helper.make_ack(transport_name='transport3')
        yield self.dispatch(msg, 'transport3.event')
        self.assert_messages(['transport3.event'], 'app1.event', [msg])
        self.assert_messages(['transport3.event'], 'app3.event', [msg])
        self.assert_no_messages('app1.inbound', 'app2.event', 'app2.inbound',
                                'app3.inbound')

    @inlineCallbacks
    def test_outbound_message_routing(self):
        yield self.get_dispatcher()
        apps = ['app1.outbound', 'app2.outbound', 'app3.outbound']
        msgs = [
            self.msg_helper.make_outbound(str(i), transport_name='transport1')
            for i in range(3)]
        for app, msg in zip(apps, msgs):
            yield self.dispatch(msg, app)
        self.assert_messages(apps, 'transport1.outbound', msgs)
        self.assert_no_messages('transport2.outbound', 'transport3.outbound')

        self.clear_dispatched()
        msgs = [
            self.msg_helper.make_outbound(str(i), transport_name='transport2')
            for i in range(3)]
        for app, msg in zip(apps, msgs):
            yield self.dispatch(msg, app)
        self.assert_messages(apps, 'transport2.outbound', msgs)
        self.assert_no_messages('transport1.outbound', 'transport3.outbound')

        self.clear_dispatched()
        msgs = [
            self.msg_helper.make_outbound(str(i), transport_name='transport3')
            for i in range(3)]
        for app, msg in zip(apps, msgs):
            yield self.dispatch(msg, app)
        self.assert_messages(apps, 'transport3.outbound', msgs)
        self.assert_no_messages('transport1.outbound', 'transport2.outbound')

    @inlineCallbacks
    def test_unroutable_outbound_error(self):
        dispatcher = yield self.get_dispatcher()
        router = dispatcher._router
        msg = self.msg_helper.make_outbound("out", transport_name='foo')
        with LogCatcher() as log:
            yield router.dispatch_outbound_message(msg)
            [error] = log.errors
            self.assertTrue(('Unknown transport_name: foo' in
                                error['message'][0]))

    @inlineCallbacks
    def test_outbound_message_routing_transport_mapping(self):
        """
        Test that transport mappings are applied for outbound messages.
        """
        yield self.get_dispatcher(
            transport_mappings={'upstream1': 'transport1'},
            transport_names=[
                'transport1',
                'transport2',
                'transport3',
                'upstream1',
            ])
        apps = ['app1.outbound', 'app2.outbound', 'app3.outbound']

        msgs = [
            self.msg_helper.make_outbound(str(i), transport_name='upstream1')
            for i in range(3)]
        for app, msg in zip(apps, msgs):
            yield self.dispatch(msg, app)
        self.assert_messages(apps, 'transport1.outbound', msgs)
        self.assert_no_messages('transport2.outbound', 'transport3.outbound',
                                'upstream1.outbound')

        self.clear_dispatched()
        msgs = [
            self.msg_helper.make_outbound(str(i), transport_name='transport2')
            for i in range(3)]
        for app, msg in zip(apps, msgs):
            yield self.dispatch(msg, app)
        self.assert_messages(apps, 'transport2.outbound', msgs)
        self.assert_no_messages('transport1.outbound', 'transport3.outbound')

    def get_dispatcher_consumers(self, dispatcher):
        return (dispatcher.transport_consumer.values() +
                dispatcher.transport_event_consumer.values() +
                dispatcher.exposed_consumer.values())

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


class TestToAddrRouter(VumiWorkerTestCase):

    @inlineCallbacks
    def setUp(self):
        yield super(TestToAddrRouter, self).setUp()
        self.config = {
            'transport_names': ['transport1'],
            'exposed_names': ['app1', 'app2'],
            'toaddr_mappings': {
                'app1': 'to:.*:1',
                'app2': 'to:app2',
                },
            }
        self.dispatcher = DummyDispatcher(self.config)
        self.router = ToAddrRouter(self.dispatcher, self.config)
        yield self.router.setup_routing()
        self.msg_helper = MessageHelper()

    def test_dispatch_inbound_message(self):
        msg = self.msg_helper.make_inbound(
            "1", to_addr='to:foo:1', transport_name='transport1')
        self.router.dispatch_inbound_message(msg)
        publishers = self.dispatcher.exposed_publisher
        self.assertEqual(publishers['app1'].msgs, [msg])
        self.assertEqual(publishers['app2'].msgs, [])

    def test_dispatch_outbound_message(self):
        msg = self.msg_helper.make_outbound("out", transport_name='transport1')
        self.router.dispatch_outbound_message(msg)
        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport1'].msgs, [msg])

        self.dispatcher.transport_publisher['transport1'].clear()
        self.config['transport_mappings'] = {
            'upstream1': 'transport1',
            }

        msg = self.msg_helper.make_outbound("out", transport_name='upstream1')
        self.router.dispatch_outbound_message(msg)
        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport1'].msgs, [msg])


class TestTransportToTransportRouter(VumiWorkerTestCase):

    @inlineCallbacks
    def setUp(self):
        yield super(TestTransportToTransportRouter, self).setUp()
        config = {
            "transport_names": [
                "transport1",
                "transport2",
                ],
            "exposed_names": [],
            "router_class": "vumi.dispatchers.base.TransportToTransportRouter",
            "route_mappings": {
                "transport1": ["transport2"],
                },
            }
        self.worker = yield self.get_worker(config, BaseDispatchWorker)
        self.msg_helper = MessageHelper()

    def dispatch(self, message, rkey=None, exchange='vumi'):
        if rkey is None:
            rkey = self.rkey('outbound')
        self._amqp.publish_message(exchange, rkey, message)
        return self._amqp.kick_delivery()

    def assert_messages(self, rkey, msgs):
        self.assertEqual(msgs, self._amqp.get_messages('vumi', rkey))

    def assert_no_messages(self, *rkeys):
        for rkey in rkeys:
            self.assertEqual([], self._amqp.get_messages('vumi', rkey))

    def clear_dispatched(self):
        self._amqp.dispatched = {}

    @inlineCallbacks
    def test_inbound_message_routing(self):
        msg = self.msg_helper.make_inbound("foo", transport_name='transport1')
        yield self.dispatch(msg, 'transport1.inbound')
        self.assert_messages('transport2.outbound', [msg])
        self.assert_no_messages('transport2.inbound', 'transport1.outbound')


class TestFromAddrMultiplexRouter(VumiWorkerTestCase):

    @inlineCallbacks
    def setUp(self):
        yield super(TestFromAddrMultiplexRouter, self).setUp()
        config = {
            "transport_names": [
                "transport_1",
                "transport_2",
                "transport_3",
                ],
            "exposed_names": ["muxed"],
            "router_class": "vumi.dispatchers.base.FromAddrMultiplexRouter",
            "fromaddr_mappings": {
                "thing1@muxme": "transport_1",
                "thing2@muxme": "transport_2",
                "thing3@muxme": "transport_3",
                },
            }
        self.dispatcher = DummyDispatcher(config)
        self.router = FromAddrMultiplexRouter(self.dispatcher, config)
        yield self.router.setup_routing()
        self.msg_helper = MessageHelper()

    @inlineCallbacks
    def tearDown(self):
        yield super(TestFromAddrMultiplexRouter, self).tearDown()
        yield self.router.teardown_routing()

    def make_inbound_mux(self, content, from_addr, transport_name):
        return self.msg_helper.make_inbound(
            content, transport_name=transport_name, from_addr=from_addr)

    def make_ack_mux(self, from_addr, transport_name):
        return self.msg_helper.make_ack(
            transport_name=transport_name, from_addr=from_addr)

    def make_outbound_mux(self, content, from_addr):
        return self.msg_helper.make_outbound(
            content, transport_name='muxed', from_addr=from_addr)

    def test_inbound_message_routing(self):
        msg1 = self.make_inbound_mux('mux 1', 'thing1@muxme', 'transport_1')
        self.router.dispatch_inbound_message(msg1)
        msg2 = self.make_inbound_mux('mux 2', 'thing2@muxme', 'transport_2')
        self.router.dispatch_inbound_message(msg2)
        publishers = self.dispatcher.exposed_publisher
        self.assertEqual(publishers['muxed'].msgs, [msg1, msg2])

    def test_inbound_event_routing(self):
        msg1 = self.make_ack_mux('thing1@muxme', 'transport_1')
        self.router.dispatch_inbound_event(msg1)
        msg2 = self.make_ack_mux('thing2@muxme', 'transport_2')
        self.router.dispatch_inbound_event(msg2)
        publishers = self.dispatcher.exposed_event_publisher
        self.assertEqual(publishers['muxed'].msgs, [msg1, msg2])

    def test_outbound_message_routing(self):
        msg1 = self.make_outbound_mux('mux 1', 'thing1@muxme')
        self.router.dispatch_outbound_message(msg1)
        msg2 = self.make_outbound_mux('mux 2', 'thing2@muxme')
        self.router.dispatch_outbound_message(msg2)
        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport_1'].msgs, [msg1])
        self.assertEqual(publishers['transport_2'].msgs, [msg2])


class UserGroupingRouterTestCase(DispatcherTestCase):

    dispatcher_class = BaseDispatchWorker
    transport_name = 'test_transport'

    @inlineCallbacks
    def setUp(self):
        yield super(UserGroupingRouterTestCase, self).setUp()
        self.config = {
            'dispatcher_name': 'user_group_dispatcher',
            'router_class': 'vumi.dispatchers.base.UserGroupingRouter',
            'transport_names': [
                self.transport_name,
            ],
            'exposed_names': [
                'app1',
                'app2',
            ],
            'group_mappings': {
                'group1': 'app1',
                'group2': 'app2',
                },
            'transport_mappings': {
                'upstream1': self.transport_name,
                },
            }

        self.dispatcher = yield self.get_dispatcher(self.config)
        self.router = self.dispatcher._router
        yield self.router._redis_d
        self.redis = self.router.redis
        yield self.redis._purge_all()  # just in case
        self.msg_helper = MessageHelper(transport_name=self.transport_name)

    @inlineCallbacks
    def tearDown(self):
        yield super(UserGroupingRouterTestCase, self).tearDown()
        yield self.redis.close_manager()

    @inlineCallbacks
    def test_group_assignment(self):
        msg = self.msg_helper.make_inbound("foo")
        selected_group = yield self.router.get_group_for_user(msg.user())
        self.assertTrue(selected_group)
        for i in range(0, 10):
            group = yield self.router.get_group_for_user(msg.user())
            self.assertEqual(group, selected_group)

    @inlineCallbacks
    def test_round_robin_group_assignment(self):
        messages = [
            self.msg_helper.make_inbound(str(i), from_addr='from_%s' % (i,))
            for i in range(0, 4)]
        groups = [(yield self.router.get_group_for_user(message.user()))
                  for message in messages]
        self.assertEqual(groups, [
            'group1',
            'group2',
            'group1',
            'group2',
        ])

    def make_inbound_from(self, from_addr):
        return self.msg_helper.make_inbound("foo", from_addr=from_addr)

    @inlineCallbacks
    def test_routing_to_application(self):
        # generate 4 messages, 2 from each user
        msg1 = self.make_inbound_from('from_1')
        msg2 = self.make_inbound_from('from_2')
        msg3 = self.make_inbound_from('from_3')
        msg4 = self.make_inbound_from('from_4')
        # send them through to the dispatcher
        messages = [msg1, msg2, msg3, msg4]
        for message in messages:
            yield self.dispatch(message, transport_name=self.transport_name)

        app1_msgs = self.get_dispatched_messages('app1', direction='inbound')
        app2_msgs = self.get_dispatched_messages('app2', direction='inbound')
        self.assertEqual(app1_msgs, [msg1, msg3])
        self.assertEqual(app2_msgs, [msg2, msg4])

    @inlineCallbacks
    def test_routing_to_transport(self):
        app_msg = self.make_inbound_from('from_1')
        yield self.dispatch(app_msg, transport_name='app1',
                                direction='outbound')
        [transport_msg] = self.get_dispatched_messages(self.transport_name,
                                                direction='outbound')
        self.assertEqual(app_msg, transport_msg)

    @inlineCallbacks
    def test_routing_to_transport_mapped(self):
        app_msg = self.msg_helper.make_inbound(
            "foo", transport_name='upstream1', from_addr='from_1')
        yield self.dispatch(
            app_msg, transport_name='app1', direction='outbound')
        [transport_msg] = self.get_dispatched_messages(
            self.transport_name, direction='outbound')
        self.assertEqual(app_msg, transport_msg)


class TestContentKeywordRouter(DispatcherTestCase):

    dispatcher_class = BaseDispatchWorker
    transport_name = 'test_transport'

    @inlineCallbacks
    def setUp(self):
        yield super(TestContentKeywordRouter, self).setUp()
        self.config = {
            'dispatcher_name': 'keyword_dispatcher',
            'router_class': 'vumi.dispatchers.base.ContentKeywordRouter',
            'transport_names': ['transport1', 'transport2'],
            'transport_mappings': {
                'shortcode1': 'transport1',
                'shortcode2': 'transport2',
                },
            'exposed_names': ['app1', 'app2', 'app3', 'fallback_app'],
            'rules': [{'app': 'app1',
                       'keyword': 'KEYWORD1',
                       'to_addr': '8181',
                       'prefix': '+256',
                       },
                      {'app': 'app2',
                       'keyword': 'KEYWORD2',
                       }],
            'keyword_mappings': {
                'app2': 'KEYWORD3',
                'app3': 'KEYWORD1',
                },
            'fallback_application': 'fallback_app',
            'expire_routing_memory': '3',
            }
        self.dispatcher = yield self.get_dispatcher(self.config)
        self.router = self.dispatcher._router
        yield self.router._redis_d
        self.redis = self.router.redis
        yield self.redis._purge_all()  # just in case
        self.msg_helper = MessageHelper()

    @inlineCallbacks
    def tearDown(self):
        yield self.router.session_manager.stop()
        yield super(TestContentKeywordRouter, self).tearDown()

    @inlineCallbacks
    def test_inbound_message_routing(self):
        msg = self.msg_helper.make_inbound('KEYWORD1 rest of a msg',
                                           to_addr='8181',
                                           from_addr='+256788601462')

        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='inbound')

        msg2 = self.msg_helper.make_inbound('KEYWORD2 rest of a msg',
                                            to_addr='8181',
                                            from_addr='+256788601462')

        yield self.dispatch(msg2,
                            transport_name='transport1',
                            direction='inbound')

        msg3 = self.msg_helper.make_inbound('KEYWORD3 rest of a msg',
                                            to_addr='8181',
                                            from_addr='+256788601462')

        yield self.dispatch(msg3,
                            transport_name='transport1',
                            direction='inbound')

        app1_inbound_msg = self.get_dispatched_messages('app1',
                                                        direction='inbound')
        self.assertEqual(app1_inbound_msg, [msg])
        app2_inbound_msg = self.get_dispatched_messages('app2',
                                                        direction='inbound')
        self.assertEqual(app2_inbound_msg, [msg2, msg3])
        app3_inbound_msg = self.get_dispatched_messages('app3',
                                                        direction='inbound')
        self.assertEqual(app3_inbound_msg, [msg])

    @inlineCallbacks
    def test_inbound_message_routing_empty_message_content(self):
        msg = self.msg_helper.make_inbound(None)

        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='inbound')

        app1_inbound_msg = self.get_dispatched_messages('app1',
                                                        direction='inbound')
        self.assertEqual(app1_inbound_msg, [])
        app2_inbound_msg = self.get_dispatched_messages('app2',
                                                        direction='inbound')
        self.assertEqual(app2_inbound_msg, [])
        fallback_msgs = self.get_dispatched_messages('fallback_app',
                                                     direction='inbound')
        self.assertEqual(fallback_msgs, [msg])

    @inlineCallbacks
    def test_inbound_message_routing_not_casesensitive(self):
        msg = self.msg_helper.make_inbound('keyword1 rest of a msg',
                                           to_addr='8181',
                                           from_addr='+256788601462')

        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='inbound')

        app1_inbound_msg = self.get_dispatched_messages('app1',
                                                        direction='inbound')
        self.assertEqual(app1_inbound_msg, [msg])

    @inlineCallbacks
    def test_inbound_event_routing_ok(self):
        msg = self.msg_helper.make_ack(
            self.msg_helper.make_outbound("foo", message_id='1'),
            transport_name='transport1')
        yield self.router.session_manager.create_session(
            'message:1', name='app2')

        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='event')

        app2_event_msg = self.get_dispatched_messages('app2',
                                                      direction='event')
        self.assertEqual(app2_event_msg, [msg])
        app1_event_msg = self.get_dispatched_messages('app1',
                                                      direction='event')
        self.assertEqual(app1_event_msg, [])

    @inlineCallbacks
    def test_inbound_event_routing_failing_publisher_not_defined(self):
        msg = self.msg_helper.make_ack(transport_name='transport1')

        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='event')

        app1_routed_msg = self.get_dispatched_messages('app1',
                                                       direction='event')
        self.assertEqual(app1_routed_msg, [])
        app2_routed_msg = self.get_dispatched_messages('app2',
                                                       direction='event')
        self.assertEqual(app2_routed_msg, [])

    @inlineCallbacks
    def test_inbound_event_routing_failing_no_routing_back_in_redis(self):
        msg = self.msg_helper.make_ack(transport_name='transport1')

        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='event')

        app1_routed_msg = self.get_dispatched_messages('app1',
                                                       direction='event')
        self.assertEqual(app1_routed_msg, [])
        app2_routed_msg = self.get_dispatched_messages('app2',
                                                       direction='event')
        self.assertEqual(app2_routed_msg, [])

    @inlineCallbacks
    def test_outbound_message_routing(self):
        msg = self.msg_helper.make_outbound("KEYWORD1 rest of msg",
                                            from_addr='shortcode1',
                                            transport_name='app2',
                                            message_id='1')

        yield self.dispatch(msg,
                            transport_name='app2',
                            direction='outbound')

        transport1_msgs = self.get_dispatched_messages('transport1',
                                                       direction='outbound')
        self.assertEqual(transport1_msgs, [msg])
        transport2_msgs = self.get_dispatched_messages('transport2',
                                                       direction='outbound')
        self.assertEqual(transport2_msgs, [])

        session = yield self.router.session_manager.load_session('message:1')
        self.assertEqual(session['name'], 'app2')


class TestRedirectOutboundRouterForSMPP(DispatcherTestCase):
    """
    This is a test to cover our use case when using SMPP 3.4 with
    split Tx and Rx binds. The outbound traffic needs to go to the Tx, while
    the Rx just should go through. Upstream everything should be seen
    as arriving from the dispatcher and so the `transport_name` should be
    overwritten.
    """
    dispatcher_class = BaseDispatchWorker

    @inlineCallbacks
    def setUp(self):
        yield super(TestRedirectOutboundRouterForSMPP, self).setUp()
        self.config = {
            'dispatcher_name': 'redirect_outbound_dispatcher',
            'router_class': 'vumi.dispatchers.base.RedirectOutboundRouter',
            'transport_names': ['smpp_rx_transport', 'smpp_tx_transport'],
            'exposed_names': ['upstream'],
            'redirect_outbound': {
                'upstream': 'smpp_tx_transport',
            },
            'redirect_inbound': {
                'smpp_tx_transport': 'upstream',
                'smpp_rx_transport': 'upstream',
            },
        }
        self.dispatcher = yield self.get_dispatcher(self.config)
        self.router = self.dispatcher._router
        self.msg_helper = MessageHelper()

    @inlineCallbacks
    def test_outbound_message_via_tx(self):
        msg = self.msg_helper.make_outbound("foo", transport_name='upstream')
        yield self.dispatch(msg, transport_name='upstream',
            direction='outbound')
        [outbound] = self.get_dispatched_messages('smpp_tx_transport',
            direction='outbound')
        self.assertEqual(outbound['message_id'], msg['message_id'])

    @inlineCallbacks
    def test_inbound_event_tx(self):
        ack = self.msg_helper.make_ack(transport_name='smpp_tx_transport')
        yield self.dispatch(ack, transport_name='smpp_tx_transport',
                                    direction='event')
        [event] = self.get_dispatched_messages('upstream',
            direction='event')
        self.assertEqual(event['transport_name'], 'upstream')
        self.assertEqual(event['event_id'], ack['event_id'])

    @inlineCallbacks
    def test_inbound_event_rx(self):
        ack = self.msg_helper.make_ack(transport_name='smpp_rx_transport')
        yield self.dispatch(ack, transport_name='smpp_rx_transport',
                                    direction='event')
        [event] = self.get_dispatched_messages('upstream',
            direction='event')
        self.assertEqual(event['transport_name'], 'upstream')
        self.assertEqual(event['event_id'], ack['event_id'])

    @inlineCallbacks
    def test_inbound_message_via_rx(self):
        msg = self.msg_helper.make_inbound(
            "foo", transport_name='smpp_rx_transport')
        yield self.dispatch(msg, transport_name='smpp_rx_transport',
                                    direction='inbound')
        [app_msg] = self.get_dispatched_messages('upstream',
            direction='inbound')
        self.assertEqual(app_msg['transport_name'], 'upstream')
        self.assertEqual(app_msg['message_id'], msg['message_id'])

    @inlineCallbacks
    def test_error_logging_for_bad_app(self):
        msgt1 = self.msg_helper.make_outbound(
            "foo", transport_name='foo')  # Does not exist
        with LogCatcher() as log:
            yield self.dispatch(msgt1, transport_name='upstream',
                direction='outbound')
            [err] = log.errors
            self.assertTrue('No redirect_outbound specified for foo' in
                                err['message'][0])


class TestRedirectOutboundRouter(DispatcherTestCase):

    dispatcher_class = BaseDispatchWorker
    transport_name = 'test_transport'

    @inlineCallbacks
    def setUp(self):
        yield super(TestRedirectOutboundRouter, self).setUp()
        self.config = {
            'dispatcher_name': 'redirect_outbound_dispatcher',
            'router_class': 'vumi.dispatchers.base.RedirectOutboundRouter',
            'transport_names': ['transport1', 'transport2'],
            'exposed_names': ['app1', 'app2'],
            'redirect_outbound': {
                'app1': 'transport1',
                'app2': 'transport2',
            },
            'redirect_inbound': {
                'transport1': 'app1',
                'transport2': 'app2',
            }
        }
        self.dispatcher = yield self.get_dispatcher(self.config)
        self.router = self.dispatcher._router
        self.msg_helper = MessageHelper()

    @inlineCallbacks
    def test_outbound_redirect(self):
        msgt1 = self.msg_helper.make_outbound("t1", transport_name='app1')
        msgt2 = self.msg_helper.make_outbound("t2", transport_name='app2')
        yield self.dispatch(msgt1, transport_name='app1',
            direction='outbound')
        yield self.dispatch(msgt2, transport_name='app2',
            direction='outbound')
        [outbound1] = self.get_dispatched_messages('transport1',
            direction='outbound')
        [outbound2] = self.get_dispatched_messages('transport2',
            direction='outbound')

        self.assertEqual(outbound1, msgt1)
        self.assertEqual(outbound2, msgt2)

    @inlineCallbacks
    def test_inbound_event(self):
        ack = self.msg_helper.make_ack(transport_name='transport1')
        yield self.dispatch(ack, transport_name='transport1',
                                    direction='event')
        [event] = self.get_dispatched_messages('app1',
            direction='event')
        self.assertEqual(event['transport_name'], 'app1')
        self.assertEqual(event['event_id'], ack['event_id'])

    @inlineCallbacks
    def test_inbound_message(self):
        msg = self.msg_helper.make_inbound("foo", transport_name='transport1')
        yield self.dispatch(msg, transport_name='transport1',
                                    direction='inbound')
        [app_msg] = self.get_dispatched_messages('app1',
            direction='inbound')
        self.assertEqual(app_msg['transport_name'], 'app1')
        self.assertEqual(app_msg['message_id'], msg['message_id'])

    @inlineCallbacks
    def test_error_logging_for_bad_app(self):
        msgt1 = self.msg_helper.make_outbound(
            "foo", transport_name='app3')  # Does not exist
        with LogCatcher() as log:
            yield self.dispatch(msgt1, transport_name='app2',
                direction='outbound')
            [err] = log.errors
            self.assertTrue('No redirect_outbound specified for app3' in
                                err['message'][0])
