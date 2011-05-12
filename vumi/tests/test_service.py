from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks
from txamqp.content import Content
from vumi.service import Worker
from vumi.tests.utils import TestChannel, TestQueue, fake_amq_message
from vumi.message import Message

class TestWorker(Worker):

    def __init__(self, queue):
        self._queue = queue

    def get_channel(self):
        return TestChannel()
    
    def queue(self, *args, **kwargs):
        return self._queue


class ServiceTestCase(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass
    
    def test_consume(self):
        """The consume helper should direct all incoming messages matching the
        specified routing_key, queue_name & exchange to the given callback"""
        
        message = fake_amq_message({"key":"value"})
        queue = TestQueue([message])
        worker = TestWorker(queue)
        
        # buffer to check messages consumed
        log = [] 
        # consume all messages on the given routing key and append
        # them to the log
        worker.consume('test.routing.key', lambda msg: log.append(msg))
        # if all works well then the consume method should funnel the test
        # message straight to the callback, the callback will apend it to the
        # log and we can test it.
        self.assertEquals(log.pop(), Message(key="value"))
    
    @inlineCallbacks
    def test_start_publisher(self):
        """The publisher should publish"""
        queue = TestQueue([])
        worker = TestWorker(queue)
        publisher = yield worker.publish_to('test.routing.key') 
        self.assertEquals(publisher.routing_key, 'test.routing.key')
        publisher.publish_message(Message(key="value"))
        published_kwargs = publisher.channel.publish_log.pop()
        self.assertEquals(published_kwargs['exchange'], 'vumi')
        self.assertEquals(published_kwargs['routing_key'], 'test.routing.key')

        delivered_content = published_kwargs['content']
        self.assertEquals(delivered_content.body, '{"key": "value"}')
        self.assertEquals(delivered_content.properties, {'delivery mode': 2})
