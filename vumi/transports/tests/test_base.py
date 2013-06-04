from twisted.internet.defer import inlineCallbacks

from vumi.transports.tests.utils import TransportTestCase
from vumi.transports.base import Transport


class BaseTransportTestCase(TransportTestCase):
    """
    This is a test for the base Transport class.

    Not to be confused with TransportTestCase above.
    """

    transport_name = 'carrier_pigeon'
    transport_class = Transport

    TEST_MIDDLEWARE_CONFIG = {
        "middleware": [
            {"mw1": "vumi.middleware.tests.utils.RecordingMiddleware"},
            {"mw2": "vumi.middleware.tests.utils.RecordingMiddleware"},
        ],
    }

    @inlineCallbacks
    def test_start_transport(self):
        tr = yield self.get_transport({})
        self.assertEqual(self.transport_name, tr.transport_name)
        self.assert_basic_rkeys(tr)

    @inlineCallbacks
    def test_middleware_for_inbound_messages(self):
        transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG)
        orig_msg = self.mkmsg_in()
        orig_msg['timestamp'] = 0
        yield transport.publish_message(**orig_msg.payload)
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['record'], [
            ['mw2', 'inbound', self.transport_name],
            ['mw1', 'inbound', self.transport_name],
            ])

    @inlineCallbacks
    def test_middleware_for_events(self):
        transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG)
        orig_msg = self.mkmsg_ack()
        orig_msg['event_id'] = 1234
        orig_msg['timestamp'] = 0
        yield transport.publish_event(**orig_msg.payload)
        [msg] = self.get_dispatched_events()
        self.assertEqual(msg['record'], [
            ['mw2', 'event', self.transport_name],
            ['mw1', 'event', self.transport_name],
            ])

    @inlineCallbacks
    def test_middleware_for_failures(self):
        transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG)
        orig_msg = self.mkmsg_out()
        orig_msg['timestamp'] = 0
        yield transport.send_failure(orig_msg, ValueError(), "dummy_traceback")
        [msg] = self.get_dispatched_failures()
        self.assertEqual(msg['record'], [
            ['mw2', 'failure', self.transport_name],
            ['mw1', 'failure', self.transport_name],
            ])

    @inlineCallbacks
    def test_middleware_for_outbound_messages(self):
        transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG)
        msgs = []
        transport.handle_outbound_message = msgs.append
        orig_msg = self.mkmsg_out()
        orig_msg['timestamp'] = 0
        yield self.dispatch(orig_msg)
        [msg] = msgs
        self.assertEqual(msg['record'], [
            ('mw1', 'outbound', self.transport_name),
            ('mw2', 'outbound', self.transport_name),
            ])

    def get_tx_consumers(self, tx):
        for connector in tx.connectors.values():
            for consumer in connector._consumers.values():
                yield consumer

    @inlineCallbacks
    def test_transport_prefetch_count_custom(self):
        transport = yield self.get_transport({
            'amqp_prefetch_count': 1,
            })
        consumers = list(self.get_tx_consumers(transport))
        self.assertEqual(1, len(consumers))
        for consumer in consumers:
            self.assertEqual(consumer.channel.qos_prefetch_count, 1)

    @inlineCallbacks
    def test_transport_prefetch_count_default(self):
        transport = yield self.get_transport({})
        consumers = list(self.get_tx_consumers(transport))
        self.assertEqual(1, len(consumers))
        for consumer in consumers:
            self.assertEqual(consumer.channel.qos_prefetch_count, 20)

    def test_metadata(self):
        transport = yield self.get_transport({})
        md = transport._hb_metadata.produce()
        self.assertEqual(md, {'type': 'transport'})
