import json

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.tests.utils import get_stubbed_worker
from vumi.transports.irc.workers import MemoWorker


class TestMemoWorker(unittest.TestCase):
    @inlineCallbacks
    def get_worker(self):
        worker = get_stubbed_worker(MemoWorker, {
                # 'transport_name': 'foo',
                # 'ussd_code': '99999',
                })
        yield worker.startWorker()
        returnValue(worker)

    def mkmsg(self, msg, addressed=False, nickname='testdude',
              channel='#test'):
        return {
            'server': 'testserv',
            'message_type': 'message',
            'nickname': nickname,
            'channel': channel,
            'message_content': msg,
            'addressed': addressed,
            }

    def snarf_msgs(self, worker, rkey):
        msgs = worker._amqp_client.broker.get_dispatched('vumi', rkey)
        return [json.loads(m.body) for m in msgs]

    @inlineCallbacks
    def test_no_memos(self):
        worker = yield self.get_worker()
        self.assertEquals({}, worker.memos)
        yield worker.process_message(self.mkmsg('hey there'))
        self.assertEquals({}, worker.memos)
        self.assertEquals([], self.snarf_msgs(worker, 'irc.outbound'))

    @inlineCallbacks
    def test_leave_memo(self):
        worker = yield self.get_worker()
        self.assertEquals({}, worker.memos)
        yield worker.process_message(self.mkmsg('bot: tell memoed hey there',
                                                True))
        self.assertEquals({('#test', 'memoed'): [('testdude', 'hey there')]},
                          worker.memos)
        self.assertEquals(['Sure thing, boss.'],
                          [m['message_content']
                           for m in self.snarf_msgs(worker, 'irc.outbound')])
