from twisted.internet.defer import inlineCallbacks

from vumi.tests.helpers import VumiTestCase
from vumi.transports.devnull import DevNullTransport
from vumi.tests.utils import LogCatcher
from vumi.transports.tests.helpers import TransportHelper


class TestDevNullTransport(VumiTestCase):

    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(DevNullTransport))

    @inlineCallbacks
    def test_outbound_logging(self):
        yield self.tx_helper.get_transport({
            'ack_rate': 1,
            'failure_rate': 0,
            'reply_rate': 1,
        })
        with LogCatcher() as logger:
            msg = yield self.tx_helper.make_dispatch_outbound("outbound")
        log_msg = logger.messages()[0]
        self.assertTrue(msg['to_addr'] in log_msg)
        self.assertTrue(msg['from_addr'] in log_msg)
        self.assertTrue(msg['content'] in log_msg)

    @inlineCallbacks
    def test_ack_publishing(self):
        yield self.tx_helper.get_transport({
            'ack_rate': 1,
            'failure_rate': 0.2,
            'reply_rate': 0.8,
        })
        yield self.tx_helper.make_dispatch_outbound("outbound")
        [ack, dr] = self.tx_helper.get_dispatched_events()
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(dr['event_type'], 'delivery_report')

    @inlineCallbacks
    def test_nack_publishing(self):
        yield self.tx_helper.get_transport({
            'ack_rate': 0,
            'failure_rate': 0.2,
            'reply_rate': 0.8,
        })
        yield self.tx_helper.make_dispatch_outbound("outbound")
        [nack] = self.tx_helper.get_dispatched_events()
        self.assertEqual(nack['event_type'], 'nack')

    @inlineCallbacks
    def test_reply_sending(self):
        yield self.tx_helper.get_transport({
            'ack_rate': 1,
            'failure_rate': 0,
            'reply_rate': 1,
        })

        msg = yield self.tx_helper.make_dispatch_outbound("outbound")
        [reply_msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['content'], reply_msg['content'])
        self.assertEqual(msg['to_addr'], reply_msg['from_addr'])
        self.assertEqual(msg['from_addr'], reply_msg['to_addr'])
