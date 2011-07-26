from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.service import Worker, WorkerCreator
from vumi.tests.utils import TestQueue, fake_amq_message, TestWorker
from vumi.message import Message


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



class LoadableTestWorker(Worker):
    def poke(self):
        return "poke"


class NoQueueWorkerCreator(WorkerCreator):
    def _connect(self, *_args, **_kw):
        pass


class TestWorkerCreator(TestCase):
    def get_creator(self, **options):
        vumi_options = {
            "hostname": "localhost",
            "port": 5672,
            "username": "vumitest",
            "password": "vumitest",
            "vhost": "/test",
            "specfile": "config/amqp-spec-0-8.xml",
            }
        vumi_options.update(options)
        return NoQueueWorkerCreator(vumi_options)

    def test_create_worker(self):
        """
        WorkerCreator should successfully create an instance of the test worker.
        """

        creator = self.get_creator()
        worker_class = "%s.%s" % (LoadableTestWorker.__module__,
                                  LoadableTestWorker.__name__)
        worker = creator.create_worker(worker_class, {}).buildProtocol(None)
        self.assertEquals("poke", worker.poke())

