import json

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.tests.utils import get_stubbed_worker
from vumi.workers.irc.workers import MemoWorker


class TestMemoWorker(unittest.TestCase):
    @inlineCallbacks
    def get_worker(self):
        worker = get_stubbed_worker(MemoWorker, {
                # 'transport_name': 'foo',
                # 'ussd_code': '99999',
                })
        yield worker.startWorker()
        returnValue(worker)

    def mkmsg(self, msg, addressed=False, nickname='testdude', channel='#test'):
        return {
            'server': 'testserv',
            'message_type': 'message',
            'nickname': nickname,
            'channel': channel,
            'message_content': msg,
            'addressed': addressed,
            }

    def snarf_msgs(self, worker, channel_id):
        msg_log = worker._amqp_client.channels[channel_id].publish_log
        return [json.loads(m['content'].body) for m in msg_log]

    @inlineCallbacks
    def test_no_memos(self):
        worker = yield self.get_worker()
        self.assertEquals({}, worker.memos)
        yield worker.process_message(self.mkmsg('hey there'))
        self.assertEquals({}, worker.memos)
        self.assertEquals([], worker._amqp_client.channels[0].publish_message_log)

    @inlineCallbacks
    def test_leave_memo(self):
        worker = yield self.get_worker()
        self.assertEquals({}, worker.memos)
        yield worker.process_message(self.mkmsg('bot: tell memoed hey there', True))
        self.assertEquals({('#test', 'memoed'): [('testdude', 'hey there')]},
                          worker.memos)
        self.assertEquals(['Sure thing, boss.'],
                          [m['message_content'] for m in self.snarf_msgs(worker, 0)])

