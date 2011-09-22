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
        self.record.append(('unknown_event', event))

    def consume_ack(self, event):
        self.record.append(('ack', event))

    def consume_delivery_report(self, event):
        self.record.append(('delivery_report', event))

    def consume_user_message(self, message):
        self.record.append(('user_message', message))

    def consume_unknown_message(self, message):
        self.record.append(('unknown_message', message))


class FakeUserMessage(TransportUserMessage):
    def __init__(self, **kw):
        kw['to_addr'] = 'to'
        kw['from_addr'] = 'from'
        kw['transport_name'] = 'test'
        kw['transport_type'] = 'fake'
        kw['transport_metadata'] = {}
        super(FakeUserMessage, self).__init__(**kw)


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
    def send(self, msg, routing_suffix='inbound'):
        routing_key = "%s.%s" % (self.transport_name, routing_suffix)
        self.broker.publish_message("vumi", routing_key, msg)
        yield self.broker.kick_delivery()

    @inlineCallbacks
    def send_event(self, event):
        yield self.send(event, 'event')

    def recv(self, routing_suffix='outbound'):
        routing_key = "%s.%s" % (self.transport_name, routing_suffix)
        contents = self.broker.get_dispatched("vumi", routing_key)
        return [Message.from_json(content.body) for content in contents]

    @inlineCallbacks
    def test_event_dispatch(self):
        bad_event1 = TransportEvent(event_type='ack',
                                    sent_message_id='remote-id',
                                    user_message_id='bad-uuid')
        bad_event1['event_type'] = 'eep'
        bad_event2 = FakeUserMessage()
        events = [
            ('ack', TransportEvent(event_type='ack',
                                   sent_message_id='remote-id',
                                   user_message_id='ack-uuid')),
            ('delivery_report', TransportEvent(event_type='delivery_report',
                                               delivery_status='pending',
                                               user_message_id='dr-uuid')),
            ('unknown_event', bad_event1),
            ('unknown_event', bad_event2),
            ]
        for name, event in events:
            yield self.send_event(event)
            self.assertEqual(self.worker.record, [(name, event)])
            del self.worker.record[:]

    @inlineCallbacks
    def test_user_message_dispatch(self):
        messages = [
            ('user_message', FakeUserMessage()),
            ('unknown_message', TransportEvent(event_type='ack',
                                               sent_message_id='remote-id',
                                               user_message_id='ack-uuid')),
            ]
        for name, message in messages:
            yield self.send(message)
            self.assertEqual(self.worker.record, [(name, message)])
            del self.worker.record[:]

    def test_reply_to(self):
        msg = FakeUserMessage()
        self.worker.reply_to(msg, "More!")
        self.worker.reply_to(msg, "End!", False)
        replies = self.recv()
        expecteds = [msg.reply("More!"), msg.reply("End!", False)]
        for key in ['timestamp', 'message_id']:
            for msg in expecteds + replies:
                del msg.payload[key]

        for reply, expected in zip(replies, expecteds):
            self.assertEqual(reply, expected)
        self.assertEqual(len(replies), len(expecteds))

    def test_subclassing_api(self):
        worker = get_stubbed_worker(ApplicationWorker,
                                    {'transport_name': 'test'})
        ack = TransportEvent(event_type='ack',
                             sent_message_id='remote-id',
                             user_message_id='ack-uuid')
        dr = TransportEvent(event_type='delivery_report',
                            delivery_status='pending',
                            user_message_id='dr-uuid')
        worker.consume_ack(ack)
        worker.consume_delivery_report(dr)
        worker.consume_unknown_event(FakeUserMessage())
        worker.consume_user_message(FakeUserMessage())
        worker.consume_unknown_message(ack)
