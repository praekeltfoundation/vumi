from twisted.internet.defer import inlineCallbacks

from vumi.transports.tests.utils import TransportTestCase
from vumi.transports.parlayx import ParlayXTransport


class ParlayXTransportTestCase(TransportTestCase):

    transport_class = ParlayXTransport
    timeout = 1

    @inlineCallbacks
    def setUp(self):
        super(ParlayXTransportTestCase, self).setUp()

        config = {
            'extra-config': 'foo',
        }
        self.transport = yield self.get_transport(config)

    @inlineCallbacks
    def test_ack(self):
        msg = self.mkmsg_out()
        msg['content'] = 'success!'
        yield self.dispatch(msg)
        [event] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_nack(self):
        msg = self.mkmsg_out()
        msg['content'] = 'fail!'
        yield self.dispatch(msg)
        [event] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['nack_reason'], 'failed')
