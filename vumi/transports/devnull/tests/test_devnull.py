from twisted.internet.defer import inlineCallbacks

from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.devnull import DevNullTransport
from vumi.tests.utils import LogCatcher


class DevNullTransportTestCase(TransportTestCase):

    transport_class = DevNullTransport

    @inlineCallbacks
    def setUp(self):
        yield super(DevNullTransportTestCase, self).setUp()
        self.transport = yield self.get_transport({
            'failure_rate': 0.2,
            'reply_rate': 0.8,
        })

    @inlineCallbacks
    def test_outbound_logging(self):
        msg = self.mkmsg_out()
        with LogCatcher() as logger:
            yield self.dispatch(msg)
        [log] = logger.logs
        log_msg = log['message'][0]
        self.assertTrue(msg['to_addr'] in log_msg)
        self.assertTrue(msg['from_addr'] in log_msg)
        self.assertTrue(msg['content'] in log_msg)

    @inlineCallbacks
    def test_ack_publishing(self):
        msg = self.mkmsg_out()
        yield self.dispatch(msg)
        event = (yield self.wait_for_dispatched_events(1))[0]
        self.assertTrue(event['event_type'], 'ack')

    @inlineCallbacks
    def test_reply_sending(self):
        for i in range(10):
            msg = self.mkmsg_out()
            yield self.dispatch(msg)
        messages = self.get_dispatched_messages()
        # we should've received at least 1 reply.
        self.assertTrue(messages)
        self.assertEqual(messages[0]['content'], msg['content'])
