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
    def send(self, content, from_addr='testnick', channel=None):
        transport_metadata = {}
        if channel is not None:
            transport_metadata['irc_channel'] = channel
        msg = TransportUserMessage(content=content,
                                   from_addr=from_addr,
                                   to_addr='some-addr',
                                   transport_name='test',
                                   transport_type='fake',
                                   transport_metadata=transport_metadata)
        self.broker.publish_message('vumi', '%s.inbound' % self.transport_name,
                                    msg)
        yield self.broker.kick_delivery()

    def clear_messages(self):
        self.broker.clear_messages('vumi', '%s.outbound' % self.transport_name)

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

    @inlineCallbacks
    def test_send_memos(self):
        yield self.send('bot: tell testmemo this is memo 1', channel='#test')
        yield self.send('bot: tell testmemo this is memo 2', channel='#test')
        yield self.send('bot: tell testmemo this is a different channel',
                        channel='#another')

        # replies to setting memos
        replies = yield self.recv(3)
        self.clear_messages()

        yield self.send('ping', channel='#test', from_addr='testmemo')
        replies = yield self.recv(2)
        self.assertEqual(replies, [
            ('reply', 'message from testnick: this is memo 1'),
            ('reply', 'message from testnick: this is memo 2'),
            ])
        self.clear_messages()

        yield self.send('ping', channel='#another', from_addr='testmemo')
        replies = yield self.recv(1)
        self.assertEqual(replies, [
            ('reply', 'message from testnick: this is a different channel'),
            ])
