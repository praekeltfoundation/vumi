from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.tests.utils import StubbedWorkerCreator, get_stubbed_worker
from vumi.service import Worker
from vumi.message import TransportUserMessage
from vumi.multiworker import MultiWorker


class ToyWorker(Worker):
    events = []

    @inlineCallbacks
    def startWorker(self):
        self.events.append("START: %s" % self.name)
        self.pub = yield self.publish_to("%s.out" % self.name)
        self.consume("%s.in" % self.name, self.process_message,
                     message_class=TransportUserMessage)

    def stopWorker(self):
        self.events.append("STOP: %s" % self.name)

    def process_message(self, message):
        return self.pub.publish_message(
            message.reply(''.join(reversed(message['content']))))


class StubbedMultiWorker(MultiWorker):
    def WORKER_CREATOR(self, options):
        worker_creator = StubbedWorkerCreator(options)
        worker_creator.broker = self._amqp_client.broker
        return worker_creator


def mkmsg(content):
    return TransportUserMessage(
        from_addr='from',
        to_addr='to',
        transport_name='sphex',
        transport_type='test',
        transport_metadata={},
        content=content,
        )


class MultiWorkerTestCase(TestCase):

    timeout = 3

    base_config = {
        'worker_classes': {
            'worker1': "%s.ToyWorker" % (__name__,),
            'worker2': "%s.ToyWorker" % (__name__,),
            'worker3': "%s.ToyWorker" % (__name__,),
            },
        'worker_config_extras': {
            'worker1': {
                'foo': 'bar',
                }
            },
        }

    def setUp(self):
        ToyWorker.events[:] = []

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopService()
        ToyWorker.events[:] = []

    def get_multiworker(self, config):
        self.worker = get_stubbed_worker(StubbedMultiWorker, config)
        self.broker = self.worker._amqp_client.broker
        return self.worker

    def dispatch(self, message, worker_name):
        rkey = "%s.in" % (worker_name,)
        self.broker.publish_message('vumi', rkey, message)
        return self.broker.kick_delivery()

    def get_replies(self, worker_name):
        msgs = self.broker.get_messages('vumi', "%s.out" % (worker_name,))
        return [m['content'] for m in msgs]

    @inlineCallbacks
    def test_start_stop_workers(self):
        worker = self.get_multiworker(self.base_config)
        self.assertEqual([], ToyWorker.events)
        yield worker.startWorker()
        self.assertEqual(['START: worker%s' % (i + 1) for i in range(3)],
                         sorted(ToyWorker.events))
        ToyWorker.events[:] = []
        yield worker.stopService()
        self.assertEqual(['STOP: worker%s' % (i + 1) for i in range(3)],
                         sorted(ToyWorker.events))

    @inlineCallbacks
    def test_message_flow(self):
        worker = self.get_multiworker(self.base_config)
        yield worker.startWorker()
        yield self.dispatch(mkmsg("foo"), "worker1")
        self.assertEqual(['oof'], self.get_replies("worker1"))
        yield self.dispatch(mkmsg("bar"), "worker2")
        yield self.dispatch(mkmsg("baz"), "worker3")
        self.assertEqual(['rab'], self.get_replies("worker2"))
        self.assertEqual(['zab'], self.get_replies("worker3"))

    @inlineCallbacks
    def test_config(self):
        worker = self.get_multiworker(self.base_config)
        yield worker.startWorker()
        worker1 = worker.getServiceNamed("worker1")
        worker2 = worker.getServiceNamed("worker2")
        self.assertEqual({'foo': 'bar'}, worker1.config)
        self.assertEqual({}, worker2.config)
