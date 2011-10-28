
from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.simple.dispatcher import SimpleDispatcher
from vumi.tests.utils import get_stubbed_worker


class TestTransport(TestCase):

    @inlineCallbacks
    def setUp(self):
        config = {
                "route_mappings": {
                    "test1.inbound": [
                        "test2.outbound",
                        "test3.outbound"],
                    "test2.inbound": [
                        "test1.outbound"],
                    "test3.inbound": [
                        "test1.outbound"]
                    }
            }
        self.worker = get_stubbed_worker(SimpleDispatcher, config)
        self.broker = self.worker._amqp_client.broker
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()

    @inlineCallbacks
    def test_1_to_2_and_3(self):
        self.broker.publish_raw("vumi",
                "test1.inbound", '{"content": "test1 to test2 and test3"}')
        msg2, = yield self.broker.wait_messages("vumi",
            "test2.outbound", 1)
        self.assertEqual(msg2.payload['content'], "test1 to test2 and test3")
        msg3, = yield self.broker.wait_messages("vumi",
            "test3.outbound", 1)
        self.assertEqual(msg3.payload['content'], "test1 to test2 and test3")

    @inlineCallbacks
    def test_2_to_1(self):
        self.broker.publish_raw("vumi",
                "test2.inbound", '{"content": "test2 to test1"}')
        msg1, = yield self.broker.wait_messages("vumi",
            "test1.outbound", 1)
        self.assertEqual(msg1.payload['content'], "test2 to test1")

    @inlineCallbacks
    def test_3_to_1(self):
        self.broker.publish_raw("vumi",
                "test3.inbound", '{"content": "test3 to test1"}')
        msg1, = yield self.broker.wait_messages("vumi",
            "test1.outbound", 1)
        self.assertEqual(msg1.payload['content'], "test3 to test1")
