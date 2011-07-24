from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase


from vumi.tests.utils import TestAMQClient, TestQueue

class UtilsTestCase(TestCase):
    @inlineCallbacks
    def test_test_amq_client(self):
        amq_client = TestAMQClient()
        q = yield amq_client.queue('foo')
        self.assertEquals(TestQueue, type(q))
