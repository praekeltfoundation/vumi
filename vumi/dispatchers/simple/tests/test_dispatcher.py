
from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.simple.dispatcher import SimpleDispatcher
from vumi.tests.helpers import VumiTestCase, WorkerHelper


class TestDispatcher(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.worker_helper = self.add_helper(WorkerHelper())
        self.worker = yield self.worker_helper.get_worker(SimpleDispatcher, {
            "route_mappings": {
                "test1.inbound": ["test2.outbound", "test3.outbound"],
                "test2.inbound": ["test1.outbound"],
                "test3.inbound": ["test1.outbound"],
            },
        })

    @inlineCallbacks
    def test_1_to_2_and_3(self):
        self.worker_helper.broker.publish_raw("vumi",
                "test1.inbound", '{"content": "test1 to test2 and test3"}')
        msg2, = yield self.worker_helper.broker.wait_messages("vumi",
            "test2.outbound", 1)
        self.assertEqual(msg2.payload['content'], "test1 to test2 and test3")
        msg3, = yield self.worker_helper.broker.wait_messages("vumi",
            "test3.outbound", 1)
        self.assertEqual(msg3.payload['content'], "test1 to test2 and test3")

    @inlineCallbacks
    def test_2_to_1(self):
        self.worker_helper.broker.publish_raw("vumi",
                "test2.inbound", '{"content": "test2 to test1"}')
        msg1, = yield self.worker_helper.broker.wait_messages("vumi",
            "test1.outbound", 1)
        self.assertEqual(msg1.payload['content'], "test2 to test1")

    @inlineCallbacks
    def test_3_to_1(self):
        self.worker_helper.broker.publish_raw("vumi",
                "test3.inbound", '{"content": "test3 to test1"}')
        msg1, = yield self.worker_helper.broker.wait_messages("vumi",
            "test1.outbound", 1)
        self.assertEqual(msg1.payload['content'], "test3 to test1")
