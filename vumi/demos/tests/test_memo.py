"""Tests for vumi.demos.memo."""

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.tests.utils import get_stubbed_worker
from vumi.demos.memo import MemoWorker
from vumi.message import TransportUserMessage


class TestMemoWorker(unittest.TestCase):
    @inlineCallbacks
    def setUp(self):
        self.transport_name = 'test_transport'
        self.worker = get_stubbed_worker(MemoWorker, {
                'transport_name': self.transport_name})
        self.broker = self.worker._amqp_client.broker
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()

    @inlineCallbacks
    def send(self, content, channel=None):
        transport_metadata = {}
        if channel is not None:
            transport_metadata['irc_channel'] = channel
        msg = TransportUserMessage(content=content,
                                   from_addr='testnick', to_addr='+134567',
                                   transport_name='test',
                                   transport_type='fake',
                                   transport_metadata=transport_metadata)
        self.broker.publish_message('vumi', '%s.inbound' % self.transport_name,
                                    msg)
        yield self.broker.kick_delivery()

    @inlineCallbacks
    def recv(self, n=0):
        msgs = yield self.broker.wait_messages('vumi', '%s.outbound'
                                                % self.transport_name, n)

        def reply_code(msg):
            if msg['session_event'] == TransportUserMessage.SESSION_CLOSE:
                return 'end'
            return 'reply'

        returnValue([(reply_code(msg), msg['content']) for msg in msgs])

    @inlineCallbacks
    def test_no_memos(self):
        yield self.send("Message from someone with no messages.")
        replies = yield self.recv()
        self.assertEquals([], replies)

    @inlineCallbacks
    def test_leave_memo(self):
        yield self.send('bot: tell memoed hey there', channel='#test')
        self.assertEquals(self.worker.memos, {
            ('#test', 'memoed'): [('testnick', 'hey there')]
            })
        replies = yield self.recv()
        self.assertEqual(replies, [
            ('reply', 'Sure thing, boss.'),
            ])
