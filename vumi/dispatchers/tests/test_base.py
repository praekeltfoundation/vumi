from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.dispatchers.base import (
    BaseDispatchWorker, ToAddrRouter, FromAddrMultiplexRouter)
from vumi.dispatchers.tests.helpers import DispatcherHelper, DummyDispatcher
from vumi.errors import DispatcherError
from vumi.tests.utils import LogCatcher
from vumi.tests.helpers import VumiTestCase, MessageHelper


class TestBaseDispatchWorker(VumiTestCase):
    def setUp(self):
        self.disp_helper = self.add_helper(
            DispatcherHelper(BaseDispatchWorker))

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
        return self.disp_helper.get_dispatcher(config)

    def ch(self, connector_name):
        return self.disp_helper.get_connector_helper(connector_name)

    def mk_middleware_records(self, rkey_in, rkey_out):
        records = []
        for rkey, direction in [(rkey_in, False), (rkey_out, True)]:
            endpoint, method = rkey.split('.', 1)
            mw = [[name, method, endpoint] for name in ("mw1", "mw2")]
            if direction:
                mw.reverse()
            records.extend(mw)
        return records

    def assert_inbound(self, dst_conn, src_conn, msg):
        [dst_msg] = self.disp_helper.get_dispatched_inbound(dst_conn)
        middleware_records = self.mk_middleware_records(
            src_conn + '.inbound', dst_conn + '.inbound')
        self.assertEqual(dst_msg.payload.pop('record'), middleware_records)
        self.assertEqual(msg, dst_msg)

    def assert_event(self, dst_conn, src_conn, msg):
        [dst_msg] = self.disp_helper.get_dispatched_events(dst_conn)
        middleware_records = self.mk_middleware_records(
            src_conn + '.event', dst_conn + '.event')
        self.assertEqual(dst_msg.payload.pop('record'), middleware_records)
        self.assertEqual(msg, dst_msg)

    def assert_outbound(self, dst_conn, src_conn_msg_pairs):
        dst_msgs = self.disp_helper.get_dispatched_outbound(dst_conn)
        for src_conn, msg in src_conn_msg_pairs:
            dst_msg = dst_msgs.pop(0)
            middleware_records = self.mk_middleware_records(
                src_conn + '.outbound', dst_conn + '.outbound')
            self.assertEqual(dst_msg.payload.pop('record'), middleware_records)
            self.assertEqual(msg, dst_msg)
        self.assertEqual([], dst_msgs)

    def assert_no_inbound(self, *conns):
        for conn in conns:
            self.assertEqual([], self.disp_helper.get_dispatched_inbound(conn))

    def assert_no_outbound(self, *conns):
        for conn in conns:
            self.assertEqual(
                [], self.disp_helper.get_dispatched_outbound(conn))

    def assert_no_events(self, *conns):
        for conn in conns:
            self.assertEqual([], self.disp_helper.get_dispatched_events(conn))

    @inlineCallbacks
    def test_inbound_message_routing(self):
        yield self.get_dispatcher()
        msg = yield self.ch('transport1').make_dispatch_inbound(
            "foo", transport_name='transport1')
        self.assert_inbound('app1', 'transport1', msg)
        self.assert_no_inbound('app2', 'app3')
        self.assert_no_events('app1', 'app2', 'app3')

        self.disp_helper.clear_all_dispatched()
        msg = yield self.ch('transport2').make_dispatch_inbound(
            "foo", transport_name='transport2')
        self.assert_inbound('app2', 'transport2', msg)
        self.assert_no_inbound('app1', 'app3')
        self.assert_no_events('app1', 'app2', 'app3')

        self.disp_helper.clear_all_dispatched()
        msg = yield self.ch('transport3').make_dispatch_inbound(
            "foo", transport_name='transport3')
        self.assert_inbound('app1', 'transport3', msg)
        self.assert_inbound('app3', 'transport3', msg)
        self.assert_no_inbound('app2')
        self.assert_no_events('app1', 'app2', 'app3')

    @inlineCallbacks
    def test_inbound_ack_routing(self):
        yield self.get_dispatcher()
        msg = yield self.ch('transport1').make_dispatch_ack(
            transport_name='transport1')
        self.assert_event('app1', 'transport1', msg)
        self.assert_no_inbound('app1', 'app2', 'app3')
        self.assert_no_events('app2', 'app3')

        self.disp_helper.clear_all_dispatched()
        msg = yield self.ch('transport2').make_dispatch_ack(
            transport_name='transport2')
        self.assert_event('app2', 'transport2', msg)
        self.assert_no_inbound('app1', 'app2', 'app3')
        self.assert_no_events('app1', 'app3')

        self.disp_helper.clear_all_dispatched()
        msg = yield self.ch('transport3').make_dispatch_ack(
            transport_name='transport3')
        self.assert_event('app1', 'transport3', msg)
        self.assert_event('app3', 'transport3', msg)
        self.assert_no_inbound('app1', 'app2', 'app3')
        self.assert_no_events('app2')

    @inlineCallbacks
    def test_outbound_message_routing(self):
        yield self.get_dispatcher()

        @inlineCallbacks
        def dispatch_for_transport(transport):
            app_msg_pairs = []
            for app in ['app1', 'app2', 'app3']:
                msg = yield self.ch(app).make_dispatch_outbound(
                    app, transport_name=transport)
                app_msg_pairs.append((app, msg))
            returnValue(app_msg_pairs)

        app_msg_pairs = yield dispatch_for_transport('transport1')
        self.assert_outbound('transport1', app_msg_pairs)
        self.assert_no_outbound('transport2', 'transport3')

        self.disp_helper.clear_all_dispatched()
        app_msg_pairs = yield dispatch_for_transport('transport2')
        self.assert_outbound('transport2', app_msg_pairs)
        self.assert_no_outbound('transport1', 'transport3')

        self.disp_helper.clear_all_dispatched()
        app_msg_pairs = yield dispatch_for_transport('transport3')
        self.assert_outbound('transport3', app_msg_pairs)
        self.assert_no_outbound('transport1', 'transport2')

    @inlineCallbacks
    def test_unroutable_outbound_error(self):
        dispatcher = yield self.get_dispatcher()
        router = dispatcher._router
        msg = self.disp_helper.make_outbound("out", transport_name='foo')
        with LogCatcher() as log:
            yield router.dispatch_outbound_message(msg)
            [error] = log.errors
            self.assertTrue(('Unknown transport_name: foo' in
                                str(error['failure'].value)))
        [f] = self.flushLoggedErrors(DispatcherError)
        self.assertEqual(f, error['failure'])

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

        @inlineCallbacks
        def dispatch_for_transport(transport):
            app_msg_pairs = []
            for app in ['app1', 'app2', 'app3']:
                msg = yield self.ch(app).make_dispatch_outbound(
                    app, transport_name=transport)
                app_msg_pairs.append((app, msg))
            returnValue(app_msg_pairs)

        app_msg_pairs = yield dispatch_for_transport('upstream1')
        self.assert_outbound('transport1', app_msg_pairs)
        self.assert_no_outbound('transport2', 'transport3', 'upstream1')

        self.disp_helper.clear_all_dispatched()
        app_msg_pairs = yield dispatch_for_transport('transport2')
        self.assert_outbound('transport2', app_msg_pairs)
        self.assert_no_outbound('transport1', 'transport3', 'upstream1')

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


class TestToAddrRouter(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
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
        self.msg_helper = self.add_helper(MessageHelper())

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


class TestTransportToTransportRouter(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.disp_helper = self.add_helper(
            DispatcherHelper(BaseDispatchWorker))
        self.worker = yield self.disp_helper.get_worker(BaseDispatchWorker, {
            "transport_names": [
                "transport1",
                "transport2",
            ],
            "exposed_names": [],
            "router_class": "vumi.dispatchers.base.TransportToTransportRouter",
            "route_mappings": {
                "transport1": ["transport2"],
            },
        })

    @inlineCallbacks
    def test_inbound_message_routing(self):
        tx1_helper = self.disp_helper.get_connector_helper('transport1')
        tx2_helper = self.disp_helper.get_connector_helper('transport2')
        msg = yield tx1_helper.make_dispatch_inbound(
            "foo", transport_name='transport1')
        self.assertEqual([msg], tx1_helper.get_dispatched_inbound())
        self.assertEqual([msg], tx2_helper.get_dispatched_outbound())
        self.assertEqual([], tx1_helper.get_dispatched_outbound())
        self.assertEqual([], tx2_helper.get_dispatched_inbound())


class TestFromAddrMultiplexRouter(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
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
        self.add_cleanup(self.router.teardown_routing)
        yield self.router.setup_routing()
        self.msg_helper = self.add_helper(MessageHelper())

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


class TestUserGroupingRouter(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.disp_helper = self.add_helper(
            DispatcherHelper(BaseDispatchWorker))
        self.dispatcher = yield self.disp_helper.get_dispatcher({
            'dispatcher_name': 'user_group_dispatcher',
            'router_class': 'vumi.dispatchers.base.UserGroupingRouter',
            'transport_names': [
                'transport1',
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
                'upstream1': 'transport1',
            },
        })
        self.router = self.dispatcher._router
        yield self.router._redis_d
        self.redis = self.router.redis
        yield self.redis._purge_all()  # just in case

    @inlineCallbacks
    def test_group_assignment(self):
        msg = self.disp_helper.make_inbound("foo")
        selected_group = yield self.router.get_group_for_user(msg.user())
        self.assertTrue(selected_group)
        for i in range(0, 10):
            group = yield self.router.get_group_for_user(msg.user())
            self.assertEqual(group, selected_group)

    @inlineCallbacks
    def test_round_robin_group_assignment(self):
        messages = [
            self.disp_helper.make_inbound(str(i), from_addr='from_%s' % (i,))
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
        return self.disp_helper.make_inbound("foo", from_addr=from_addr)

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
            yield self.disp_helper.dispatch_inbound(message, 'transport1')

        app1_msgs = self.disp_helper.get_dispatched_inbound('app1')
        app2_msgs = self.disp_helper.get_dispatched_inbound('app2')
        self.assertEqual(app1_msgs, [msg1, msg3])
        self.assertEqual(app2_msgs, [msg2, msg4])

    @inlineCallbacks
    def test_routing_to_transport(self):
        app_msg = self.disp_helper.make_outbound(
            'foo', transport_name='transport1')
        yield self.disp_helper.dispatch_outbound(app_msg, 'app1')
        [tx_msg] = self.disp_helper.get_dispatched_outbound('transport1')
        self.assertEqual(app_msg, tx_msg)

    @inlineCallbacks
    def test_routing_to_transport_mapped(self):
        app_msg = self.disp_helper.make_outbound(
            'foo', transport_name='upstream1')
        yield self.disp_helper.dispatch_outbound(app_msg, 'app1')
        [tx_msg] = self.disp_helper.get_dispatched_outbound('transport1')
        self.assertEqual(app_msg, tx_msg)


class TestContentKeywordRouter(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.disp_helper = self.add_helper(
            DispatcherHelper(BaseDispatchWorker))
        self.dispatcher = yield self.disp_helper.get_dispatcher({
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
        })
        self.router = self.dispatcher._router
        yield self.router._redis_d
        self.add_cleanup(self.router.session_manager.stop)
        self.redis = self.router.redis
        yield self.redis._purge_all()  # just in case

    def ch(self, connector_name):
        return self.disp_helper.get_connector_helper(connector_name)

    def send_inbound(self, content, **kw):
        return self.ch('transport1').make_dispatch_inbound(content, **kw)

    def assert_dispatched(self, connector_name, msgs):
        self.assertEqual(
            msgs, self.disp_helper.get_dispatched_inbound(connector_name))

    @inlineCallbacks
    def test_inbound_message_routing(self):
        msg1 = yield self.send_inbound(
            'KEYWORD1 rest of msg', to_addr='8181', from_addr='+256788601462')
        msg2 = yield self.send_inbound(
            'KEYWORD2 rest of msg', to_addr='8181', from_addr='+256788601462')
        msg3 = yield self.send_inbound(
            'KEYWORD3 rest of msg', to_addr='8181', from_addr='+256788601462')

        self.assert_dispatched('app1', [msg1])
        self.assert_dispatched('app2', [msg2, msg3])
        self.assert_dispatched('app3', [msg1])

    @inlineCallbacks
    def test_inbound_message_routing_empty_message_content(self):
        msg = yield self.send_inbound(None)
        self.assert_dispatched('app1', [])
        self.assert_dispatched('app2', [])
        self.assert_dispatched('fallback_app', [msg])

    @inlineCallbacks
    def test_inbound_message_routing_not_casesensitive(self):
        msg = yield self.send_inbound(
            'keyword1 rest of msg', to_addr='8181', from_addr='+256788601462')
        self.assert_dispatched('app1', [msg])

    @inlineCallbacks
    def test_inbound_event_routing_ok(self):
        yield self.router.session_manager.create_session(
            'message:1', name='app2')
        ack = yield self.ch('transport1').make_dispatch_ack(
            self.disp_helper.make_outbound("foo", message_id='1'),
            transport_name='transport1')

        self.assertEqual([], self.disp_helper.get_dispatched_events('app1'))
        self.assertEqual([ack], self.disp_helper.get_dispatched_events('app2'))

    @inlineCallbacks
    def test_inbound_event_routing_failing_no_routing_back_in_redis(self):
        ack = yield self.ch('transport1').make_dispatch_ack(
            transport_name='transport1')

        self.assertEqual([], self.disp_helper.get_dispatched_events('app1'))
        self.assertEqual([], self.disp_helper.get_dispatched_events('app2'))

        [redis_lookup_fail, no_route_fail] = self.flushLoggedErrors(
            DispatcherError)
        self.assertEqual(str(redis_lookup_fail.value), (
            'No transport_name for return route found in Redis while'
            ' dispatching transport event for message %s'
            % ack['user_message_id']))
        self.assertEqual(str(no_route_fail.value),
                         'No publishing route for None')

    @inlineCallbacks
    def test_outbound_message_routing(self):
        msg = yield self.ch('app2').make_dispatch_outbound(
            "KEYWORD1 rest of msg", from_addr='shortcode1',
            transport_name='app2', message_id='1')

        self.assertEqual(
            [msg], self.disp_helper.get_dispatched_outbound('transport1'))
        self.assertEqual(
            [], self.disp_helper.get_dispatched_outbound('transport2'))

        session = yield self.router.session_manager.load_session('message:1')
        self.assertEqual(session['name'], 'app2')


class TestRedirectOutboundRouterForSMPP(VumiTestCase):
    """
    This is a test to cover our use case when using SMPP 3.4 with
    split Tx and Rx binds. The outbound traffic needs to go to the Tx, while
    the Rx just should go through. Upstream everything should be seen
    as arriving from the dispatcher and so the `transport_name` should be
    overwritten.
    """

    @inlineCallbacks
    def setUp(self):
        self.disp_helper = self.add_helper(
            DispatcherHelper(BaseDispatchWorker))
        self.dispatcher = yield self.disp_helper.get_dispatcher({
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
        })
        self.router = self.dispatcher._router

    def ch(self, connector_name):
        return self.disp_helper.get_connector_helper(connector_name)

    @inlineCallbacks
    def test_outbound_message_via_tx(self):
        msg = yield self.ch('upstream').make_dispatch_outbound(
            "foo", transport_name='upstream')
        [out] = self.disp_helper.get_dispatched_outbound('smpp_tx_transport')
        self.assertEqual(out['message_id'], msg['message_id'])

    @inlineCallbacks
    def test_inbound_event_tx(self):
        ack = yield self.ch('smpp_tx_transport').make_dispatch_ack(
            transport_name='smpp_tx_transport')
        [event] = self.disp_helper.get_dispatched_events('upstream')
        self.assertEqual(event['transport_name'], 'upstream')
        self.assertEqual(event['event_id'], ack['event_id'])

    @inlineCallbacks
    def test_inbound_event_rx(self):
        ack = yield self.ch('smpp_rx_transport').make_dispatch_ack(
            transport_name='smpp_rx_transport')
        [event] = self.disp_helper.get_dispatched_events('upstream')
        self.assertEqual(event['transport_name'], 'upstream')
        self.assertEqual(event['event_id'], ack['event_id'])

    @inlineCallbacks
    def test_inbound_message_via_rx(self):
        msg = yield self.ch('smpp_rx_transport').make_dispatch_inbound(
            "foo", transport_name='smpp_rx_transport')
        [app_msg] = self.disp_helper.get_dispatched_inbound('upstream')
        self.assertEqual(app_msg['transport_name'], 'upstream')
        self.assertEqual(app_msg['message_id'], msg['message_id'])

    @inlineCallbacks
    def test_error_logging_for_bad_app(self):
        msgt1 = self.disp_helper.make_outbound(
            "foo", transport_name='foo')  # Does not exist
        with LogCatcher() as log:
            yield self.disp_helper.dispatch_outbound(msgt1, 'upstream')
            [err] = log.errors
            self.assertTrue('No redirect_outbound specified for foo' in
                                str(err['failure'].value))
        [f] = self.flushLoggedErrors(DispatcherError)
        self.assertEqual(f, err['failure'])


class TestRedirectOutboundRouter(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.disp_helper = self.add_helper(
            DispatcherHelper(BaseDispatchWorker))
        self.dispatcher = yield self.disp_helper.get_dispatcher({
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
        })
        self.router = self.dispatcher._router

    def ch(self, connector_name):
        return self.disp_helper.get_connector_helper(connector_name)

    @inlineCallbacks
    def test_outbound_redirect(self):
        msgt1 = yield self.ch('app1').make_dispatch_outbound(
            "t1", transport_name='app1')
        msgt2 = yield self.ch('app2').make_dispatch_outbound(
            "t2", transport_name='app2')
        self.assertEqual(
            [msgt1], self.disp_helper.get_dispatched_outbound('transport1'))
        self.assertEqual(
            [msgt2], self.disp_helper.get_dispatched_outbound('transport2'))

    @inlineCallbacks
    def test_inbound_event(self):
        ack = yield self.ch('transport1').make_dispatch_ack(
            transport_name='transport1')
        [event] = self.disp_helper.get_dispatched_events('app1')
        self.assertEqual(event['transport_name'], 'app1')
        self.assertEqual(event['event_id'], ack['event_id'])

    @inlineCallbacks
    def test_inbound_message(self):
        msg = yield self.ch('transport1').make_dispatch_inbound(
            "foo", transport_name='transport1')
        [app_msg] = self.disp_helper.get_dispatched_inbound('app1')
        self.assertEqual(app_msg['transport_name'], 'app1')
        self.assertEqual(app_msg['message_id'], msg['message_id'])

    @inlineCallbacks
    def test_error_logging_for_bad_app(self):
        msgt1 = self.disp_helper.make_outbound(
            "foo", transport_name='app3')  # Does not exist
        with LogCatcher() as log:
            yield self.disp_helper.dispatch_outbound(msgt1, 'app2')
            [err] = log.errors
            self.assertTrue('No redirect_outbound specified for app3' in
                                str(err['failure'].value))
        [f] = self.flushLoggedErrors(DispatcherError)
        self.assertEqual(f, err['failure'])
