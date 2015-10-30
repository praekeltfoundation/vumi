import json

from twisted.internet.defer import inlineCallbacks, Deferred

from vumi.message import Message
from vumi.service import Worker, WorkerCreator
from vumi.tests.helpers import VumiTestCase, WorkerHelper


class TestService(VumiTestCase):
    def setUp(self):
        self.worker_helper = self.add_helper(WorkerHelper())

    @inlineCallbacks
    def test_consume(self):
        """The consume helper should direct all incoming messages matching the
        specified routing_key, queue_name & exchange to the given callback"""

        worker = yield self.worker_helper.get_worker(Worker, {}, start=False)

        # buffer to check messages consumed
        log = []
        # consume all messages on the given routing key and append
        # them to the log
        yield worker.consume('test.routing.key', lambda msg: log.append(msg))
        # if all works well then the consume method should funnel the test
        # message straight to the callback, the callback will apend it to the
        # log and we can test it.
        self.worker_helper.broker.basic_publish('vumi', 'test.routing.key',
                                                json.dumps({"key": "value"}))
        yield self.worker_helper.broker.wait_delivery()
        self.assertEquals(log, [Message(key="value")])

    @inlineCallbacks
    def test_consume_with_prefetch(self):
        """The consume helper should direct all incoming messages matching the
        specified routing_key, queue_name & exchange to the given callback"""

        worker = yield self.worker_helper.get_worker(Worker, {}, start=False)

        # buffer to check messages consumed
        log = []
        # consume all messages on the given routing key and append
        # them to the log
        consumer = yield worker.consume(
            'test.routing.key', lambda msg: log.append(msg), prefetch_count=10)
        yield self.worker_helper.broker.wait_delivery()

        server = self.worker_helper.broker.server_protocols[-1]
        server_channel = server.channels[consumer.channel.channel_number]
        self.assertEqual(10, server_channel._get_consumer_prefetch(
            consumer._consumer_tag))

    @inlineCallbacks
    def test_broken_consume(self):
        """
        If a consumer function throws an exception, we still decrement the
        progress counter.
        """
        worker = yield self.worker_helper.get_worker(Worker, {}, start=False)
        start_d = Deferred()
        pause_d = Deferred()

        @inlineCallbacks
        def consume_func(msg):
            start_d.callback(None)
            yield pause_d
            raise Exception("oops")

        consumer = yield worker.consume('test.routing.key', consume_func)

        # Assert we have no messages being processed and publish a message.
        self.assertEqual(consumer._in_progress, 0)
        self.worker_helper.broker.basic_publish(
            'vumi', 'test.routing.key', json.dumps({"key": "value"}))
        # Wait for the processing to start and assert that it's in progress.
        yield start_d
        self.assertEqual(consumer._in_progress, 1)
        # Unpause the processing and wait for it to finish.
        pause_d.callback(None)
        yield self.worker_helper.kick_delivery()
        # If we get here without timing out, everything should be happy.
        self.assertEqual(consumer._in_progress, 0)
        [failure] = self.flushLoggedErrors()
        self.assertEqual(failure.getErrorMessage(), "oops")

    @inlineCallbacks
    def test_start_publisher(self):
        """The publisher should publish"""
        worker = yield self.worker_helper.get_worker(Worker, {}, start=False)
        publisher = yield worker.publish_to('test.routing.key')
        self.assertEquals(publisher.routing_key, 'test.routing.key')
        publisher.publish_message(Message(key="value"))
        yield self.worker_helper.kick_delivery()
        [published_msg] = self.worker_helper.broker.get_dispatched(
            'vumi', 'test.routing.key')

        self.assertEquals(published_msg, '{"key": "value"}')


class LoadableTestWorker(Worker):
    def poke(self):
        return "poke"


class NoQueueWorkerCreator(WorkerCreator):
    def _connect(self, *_args, **_kw):
        pass


class TestWorkerCreator(VumiTestCase):
    def get_creator(self, **options):
        vumi_options = {
            "hostname": "127.0.0.1",
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
