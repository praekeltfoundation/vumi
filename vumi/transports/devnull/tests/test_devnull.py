from twisted.internet.defer import inlineCallbacks

from vumi.transports.tests.utils import TransportTestCase
from vumi.transports.devnull import DevNullTransport
from vumi.tests.utils import LogCatcher
from vumi.tests.helpers import MessageHelper


class DevNullTransportTestCase(TransportTestCase):

    transport_class = DevNullTransport
    msg_helper = MessageHelper()

    @inlineCallbacks
    def test_outbound_logging(self):
        yield self.get_transport({
            'ack_rate': 1,
            'failure_rate': 0,
            'reply_rate': 1,
        })
        msg = self.msg_helper.make_outbound("outbound")
        with LogCatcher() as logger:
            yield self.dispatch(msg)
        log_msg = logger.messages()[0]
        self.assertTrue(msg['to_addr'] in log_msg)
        self.assertTrue(msg['from_addr'] in log_msg)
        self.assertTrue(msg['content'] in log_msg)

    @inlineCallbacks
    def test_ack_publishing(self):
        yield self.get_transport({
            'ack_rate': 1,
            'failure_rate': 0.2,
            'reply_rate': 0.8,
        })
        msg = self.msg_helper.make_outbound("outbound")
        yield self.dispatch(msg)
        [ack, dr] = self.get_dispatched_events()
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(dr['event_type'], 'delivery_report')

    @inlineCallbacks
    def test_nack_publishing(self):
        yield self.get_transport({
            'ack_rate': 0,
            'failure_rate': 0.2,
            'reply_rate': 0.8,
        })
        msg = self.msg_helper.make_outbound("outbound")
        yield self.dispatch(msg)
        [nack] = self.get_dispatched_events()
        self.assertEqual(nack['event_type'], 'nack')

    @inlineCallbacks
    def test_reply_sending(self):
        yield self.get_transport({
            'ack_rate': 1,
            'failure_rate': 0,
            'reply_rate': 1,
        })

        msg = self.msg_helper.make_outbound("outbound")
        yield self.dispatch(msg)
        [reply_msg] = self.get_dispatched_messages()
        self.assertEqual(msg['content'], reply_msg['content'])
        self.assertEqual(msg['to_addr'], reply_msg['from_addr'])
        self.assertEqual(msg['from_addr'], reply_msg['to_addr'])
