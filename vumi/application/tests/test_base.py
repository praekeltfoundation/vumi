from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, Deferred
from vumi.tests.utils import TestChannel, get_stubbed_worker
from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.workers.blinkenlights import metrics
from vumi.blinkenlights.message20110818 import MetricMessage
from vumi.message import Message


from vumi.application.base import ApplicationWorker
from vumi.message import TransportUserMessage, TransportEvent


class DummyApplicationWorker(ApplicationWorker):

    def __init__(self, *args, **kwargs):
        super(ApplicationWorker, self).__init__(*args, **kwargs)
        self.record = []

    def consume_unknown_event(self, event):
        self.record.append(('unknown', event))

    def consume_ack(self, event):
        self.record.append(('ack', event))

    def consume_delivery_report(self, event):
        self.record.append(('delivery_report', event))


class TestApplicationWorker(TestCase):

    @inlineCallbacks
    def setUp(self):
        self.transport_name = 'test'
        self.config = {'transport_name': self.transport_name}
        self.worker = get_stubbed_worker(DummyApplicationWorker,
                                         config=self.config)
        self.broker = self.worker._amqp_client.broker
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()

    @inlineCallbacks
    def send(self, msg, routing_suffix='outbound'):
        routing_key = "%s.%s" % (self.transport_name, routing_suffix)
        self.broker.publish_message("vumi", routing_key, msg)
        yield self.broker.kick_delivery()

    @inlineCallbacks
    def send_event(self, event):
        yield self.send(event, 'event')

    def test_unknown_event_dispatch(self):
        pass

    @inlineCallbacks
    def test_event_dispatch(self):
        bad_event = TransportEvent(event_type='ack',
                                   user_message_id='bad-uuid')
        bad_event['event_type'] = 'eep'
        events = [
            ('ack', TransportEvent(event_type='ack',
                                   user_message_id='ack-uuid')),
            ('delivery_report', TransportEvent(event_type='delivery_report',
                                               delivery_status='pending',
                                               user_message_id='dr-uuid')),
            ('unknown', bad_event),
            ]
        for name, event in events:
            yield self.send_event(event)
            self.assertEqual(self.worker.record, [(name, event)])
            del self.worker.record[:]

    def test_user_message_dispatch(self):
        pass

    def test_reply_to(self):
        pass
