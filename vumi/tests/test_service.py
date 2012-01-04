from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.service import Worker, WorkerCreator
from vumi.tests.utils import (fake_amq_message, get_stubbed_worker)
from vumi.message import Message


class ServiceTestCase(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_consume(self):
        """The consume helper should direct all incoming messages matching the
        specified routing_key, queue_name & exchange to the given callback"""

        message = fake_amq_message({"key": "value"})
        worker = get_stubbed_worker(Worker)

        # buffer to check messages consumed
        log = [Message.from_json(message.content.body)]
        # consume all messages on the given routing key and append
        # them to the log
        worker.consume('test.routing.key', lambda msg: log.append(msg))
        # if all works well then the consume method should funnel the test
        # message straight to the callback, the callback will apend it to the
        # log and we can test it.
        worker._amqp_client.broker.basic_publish('vumi', 'test.routing.key',
                                                 message.content)
        self.assertEquals(log, [Message(key="value")])

    @inlineCallbacks
    def test_start_publisher(self):
        """The publisher should publish"""
        worker = get_stubbed_worker(Worker)
        publisher = yield worker.publish_to('test.routing.key')
        self.assertEquals(publisher.routing_key, 'test.routing.key')
        publisher.publish_message(Message(key="value"))
        [published_msg] = publisher.channel.broker.get_dispatched(
            'vumi', 'test.routing.key')

        self.assertEquals(published_msg.body, '{"key": "value"}')
        self.assertEquals(published_msg.properties, {'delivery mode': 2})


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
            "specfile": "amqp-spec-0-8.xml",
            }
        vumi_options.update(options)
        return NoQueueWorkerCreator(vumi_options)

    def test_create_worker(self):
        """
        WorkerCreator should successfully create a test worker instance.
        """

        creator = self.get_creator()
        worker_class = "%s.%s" % (LoadableTestWorker.__module__,
                                  LoadableTestWorker.__name__)
        worker = creator.create_worker(worker_class, {})
        self.assertEquals("poke", worker.poke())
