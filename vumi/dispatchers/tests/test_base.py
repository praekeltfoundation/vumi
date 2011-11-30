
from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.message import TransportUserMessage, TransportEvent
from vumi.dispatchers.base import BaseDispatchWorker, ToAddrRouter
from vumi.tests.utils import get_stubbed_worker


class MessageMakerMixIn(object):
    """TestCase mixin for creating transport messages."""

    def mkmsg_ack(self, transport_name, **kw):
        event_kw = dict(
            event_type='ack',
            user_message_id='1',
            sent_message_id='abc',
            transport_name=transport_name,
            transport_metadata={},
            )
        event_kw.update(kw)
        return TransportEvent(**event_kw)

    def mkmsg_in(self, transport_name, content='foo', **kw):
        msg_kw = dict(
            from_addr='+41791234567',
            to_addr='9292',
            transport_name=transport_name,
            transport_type='sms',
            transport_metadata={},
            content=content,
            )
        msg_kw.update(kw)
        return TransportUserMessage(**msg_kw)

    def mkmsg_out(self, transport_name, content='hello world', **kw):
        msg_kw = dict(
            to_addr='+41791234567',
            from_addr='9292',
            transport_name=transport_name,
            transport_type='sms',
            transport_metadata={},
            content=content,
            )
        msg_kw.update(kw)
        return TransportUserMessage(**msg_kw)


class TestBaseDispatchWorker(TestCase, MessageMakerMixIn):

    @inlineCallbacks
    def setUp(self):
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
            }
        self.worker = get_stubbed_worker(BaseDispatchWorker, config)
        self._amqp = self.worker._amqp_client.broker
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()

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
        msg = self.mkmsg_in('transport1')
        yield self.dispatch(msg, 'transport1.inbound')
        self.assert_messages('app1.inbound', [msg])
        self.assert_no_messages('app1.event', 'app2.inbound', 'app2.event',
                                'app3.inbound', 'app3.event')

        self.clear_dispatched()
        msg = self.mkmsg_in('transport2')
        yield self.dispatch(msg, 'transport2.inbound')
        self.assert_messages('app2.inbound', [msg])
        self.assert_no_messages('app1.inbound', 'app1.event', 'app2.event',
                                'app3.inbound', 'app3.event')

        self.clear_dispatched()
        msg = self.mkmsg_in('transport3')
        yield self.dispatch(msg, 'transport3.inbound')
        self.assert_messages('app1.inbound', [msg])
        self.assert_messages('app3.inbound', [msg])
        self.assert_no_messages('app1.event', 'app2.inbound', 'app2.event',
                                'app3.event')

    @inlineCallbacks
    def test_inbound_event_routing(self):
        msg = self.mkmsg_ack('transport1')
        yield self.dispatch(msg, 'transport1.event')
        self.assert_messages('app1.event', [msg])
        self.assert_no_messages('app1.inbound', 'app2.event', 'app2.inbound',
                                'app3.event', 'app3.inbound')

        self.clear_dispatched()
        msg = self.mkmsg_ack('transport2')
        yield self.dispatch(msg, 'transport2.event')
        self.assert_messages('app2.event', [msg])
        self.assert_no_messages('app1.event', 'app1.inbound', 'app2.inbound',
                                'app3.event', 'app3.inbound')

        self.clear_dispatched()
        msg = self.mkmsg_ack('transport3')
        yield self.dispatch(msg, 'transport3.event')
        self.assert_messages('app1.event', [msg])
        self.assert_messages('app3.event', [msg])
        self.assert_no_messages('app1.inbound', 'app2.event', 'app2.inbound',
                                'app3.inbound')

    @inlineCallbacks
    def test_outbound_message_routing(self):
        msgs = [self.mkmsg_out('transport1') for _ in range(3)]
        yield self.dispatch(msgs[0], 'app1.outbound')
        yield self.dispatch(msgs[1], 'app2.outbound')
        yield self.dispatch(msgs[2], 'app3.outbound')
        self.assert_messages('transport1.outbound', msgs)
        self.assert_no_messages('transport2.outbound', 'transport3.outbound')

        self.clear_dispatched()
        msgs = [self.mkmsg_out('transport2') for _ in range(3)]
        yield self.dispatch(msgs[0], 'app1.outbound')
        yield self.dispatch(msgs[1], 'app2.outbound')
        yield self.dispatch(msgs[2], 'app3.outbound')
        self.assert_messages('transport2.outbound', msgs)
        self.assert_no_messages('transport1.outbound', 'transport3.outbound')

        self.clear_dispatched()
        msgs = [self.mkmsg_out('transport3') for _ in range(3)]
        yield self.dispatch(msgs[0], 'app1.outbound')
        yield self.dispatch(msgs[1], 'app2.outbound')
        yield self.dispatch(msgs[2], 'app3.outbound')
        self.assert_messages('transport3.outbound', msgs)
        self.assert_no_messages('transport1.outbound', 'transport2.outbound')


class DummyDispatcher(object):

    class DummyPublisher(object):
        def __init__(self):
            self.msgs = []

        def publish_message(self, msg):
            self.msgs.append(msg)

    def __init__(self, config):
        self.transport_publisher = {}
        for transport in config['transport_names']:
            self.transport_publisher[transport] = self.DummyPublisher()
        self.exposed_publisher = {}
        for exposed in config['exposed_names']:
            self.exposed_publisher[exposed] = self.DummyPublisher()


class TestToAddrRouter(TestCase, MessageMakerMixIn):
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

    def test_dispatch_inbound_message(self):
        msg = self.mkmsg_in(to_addr='to:foo:1', transport_name='transport1')
        self.router.dispatch_inbound_message(msg)
        publishers = self.dispatcher.exposed_publisher
        self.assertEqual(publishers['app1'].msgs, [msg])
        self.assertEqual(publishers['app2'].msgs, [])

    def test_dispatch_outbound_message(self):
        msg = self.mkmsg_out(transport_name='transport1')
        self.router.dispatch_outbound_message(msg)
        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport1'].msgs, [msg])


class TestTransportToTransportRouter(TestCase, MessageMakerMixIn):

    @inlineCallbacks
    def setUp(self):
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
        self.worker = get_stubbed_worker(BaseDispatchWorker, config)
        self._amqp = self.worker._amqp_client.broker
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()

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
        msg = self.mkmsg_in('transport1')
        yield self.dispatch(msg, 'transport1.inbound')
        self.assert_messages('transport2.outbound', [msg])
        self.assert_no_messages('transport2.inbound', 'transport1.outbound')
