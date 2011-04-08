from twisted.unittest import TestCase
from vumi.utils import TestPublisher
from vumi.message import Message


class SMSBatchConsumerTestCase(TestCase):

    def setUp(self):
        self.debatcher = SMSBatchConsumer()
        self.publisher = TestPublisher()
    
    def tearDown(self):
        pass
    
    def test_debatching(self):
        """It should publish an incoming batch of 3 into separate
        individual messages"""
        
        self.

    
