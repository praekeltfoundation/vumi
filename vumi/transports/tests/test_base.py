from twisted.internet.defer import inlineCallbacks

from vumi.tests.helpers import VumiTestCase
from vumi.transports.base import Transport
from vumi.transports.tests.helpers import TransportHelper


class TestBaseTransport(VumiTestCase):

    TEST_MIDDLEWARE_CONFIG = {
        "middleware": [
            {"mw1": "vumi.middleware.tests.utils.RecordingMiddleware"},
            {"mw2": "vumi.middleware.tests.utils.RecordingMiddleware"},
        ],
    }

    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(Transport))

    @inlineCallbacks
    def test_start_transport(self):
        tr = yield self.tx_helper.get_transport({})
        self.assertEqual(self.tx_helper.transport_name, tr.transport_name)
        self.assertTrue(len(tr.connectors) >= 1)
        connector = tr.connectors[tr.transport_name]
        self.assertTrue(connector._consumers.keys(), set(['outbound']))
        self.assertTrue(connector._publishers.keys(),
                        set(['inbound', 'event']))
        self.assertEqual(tr.failure_publisher.routing_key,
                         '%s.failures' % (tr.transport_name,))

    @inlineCallbacks
    def test_middleware_for_inbound_messages(self):
        transport = yield self.tx_helper.get_transport(
            self.TEST_MIDDLEWARE_CONFIG)
        orig_msg = self.tx_helper.make_inbound("inbound")
        yield transport.publish_message(**orig_msg.payload)
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['record'], [
            ['mw2', 'inbound', self.tx_helper.transport_name],
            ['mw1', 'inbound', self.tx_helper.transport_name],
            ])

    @inlineCallbacks
    def test_middleware_for_events(self):
        transport = yield self.tx_helper.get_transport(
            self.TEST_MIDDLEWARE_CONFIG)
        orig_msg = self.tx_helper.make_ack()
        yield transport.publish_event(**orig_msg.payload)
        [msg] = self.tx_helper.get_dispatched_events()
        self.assertEqual(msg['record'], [
            ['mw2', 'event', self.tx_helper.transport_name],
            ['mw1', 'event', self.tx_helper.transport_name],
            ])

    @inlineCallbacks
    def test_middleware_for_failures(self):
        transport = yield self.tx_helper.get_transport(
            self.TEST_MIDDLEWARE_CONFIG)
        orig_msg = self.tx_helper.make_outbound("outbound")
        yield transport.send_failure(orig_msg, ValueError(), "dummy_traceback")
        [msg] = self.tx_helper.get_dispatched_failures()
        self.assertEqual(msg['record'], [
            ['mw2', 'failure', self.tx_helper.transport_name],
            ['mw1', 'failure', self.tx_helper.transport_name],
            ])

    @inlineCallbacks
    def test_middleware_for_outbound_messages(self):
        msgs = []
        transport = yield self.tx_helper.get_transport(
            self.TEST_MIDDLEWARE_CONFIG)
        transport.add_outbound_handler(msgs.append)

        yield self.tx_helper.make_dispatch_outbound("outbound")

        [msg] = msgs
        self.assertEqual(msg['record'], [
            ('mw1', 'outbound', self.tx_helper.transport_name),
            ('mw2', 'outbound', self.tx_helper.transport_name),
            ])

    def get_tx_consumers(self, tx):
        for connector in tx.connectors.values():
            for consumer in connector._consumers.values():
                yield consumer

    @inlineCallbacks
    def test_transport_prefetch_count_custom(self):
        transport = yield self.tx_helper.get_transport({
            'amqp_prefetch_count': 1,
            })
        consumers = list(self.get_tx_consumers(transport))
        self.assertEqual(1, len(consumers))
        for consumer in consumers:
            self.assertEqual(consumer.channel.qos_prefetch_count, 1)

    @inlineCallbacks
    def test_transport_prefetch_count_default(self):
        transport = yield self.tx_helper.get_transport({})
        consumers = list(self.get_tx_consumers(transport))
        self.assertEqual(1, len(consumers))
        for consumer in consumers:
            self.assertEqual(consumer.channel.qos_prefetch_count, 20)

    @inlineCallbacks
    def test_add_outbound_handler(self):
        transport = yield self.tx_helper.get_transport({})

        msgs = []
        msg = transport.add_outbound_handler(msgs.append, endpoint_name='foo')

        msg = yield self.tx_helper.make_dispatch_outbound(
            "outbound", endpoint='foo')
        self.assertEqual(msgs, [msg])
