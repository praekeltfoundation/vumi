from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.application.base import ApplicationWorker, SESSION_NEW, SESSION_CLOSE
from vumi.message import TransportUserMessage, TransportEvent
from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.tests.utils import get_stubbed_worker
from datetime import datetime


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

    def new_session(self, message):
        self.record.append(('new_session', message))

    def close_session(self, message):
        self.record.append(('close_session', message))


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
        return self.broker.get_messages("vumi", routing_key)

    @inlineCallbacks
    def test_event_dispatch(self):
        events = [
            ('ack', TransportEvent(event_type='ack',
                                   sent_message_id='remote-id',
                                   user_message_id='ack-uuid')),
            ('delivery_report', TransportEvent(event_type='delivery_report',
                                               delivery_status='pending',
                                               user_message_id='dr-uuid')),
            ]
        for name, event in events:
            yield self.send_event(event)
            self.assertEqual(self.worker.record, [(name, event)])
            del self.worker.record[:]

    @inlineCallbacks
    def test_unknown_event_dispatch(self):
        # temporarily pretend the worker doesn't know about acks
        del self.worker._event_handlers['ack']
        bad_event = TransportEvent(event_type='ack',
                                   sent_message_id='remote-id',
                                   user_message_id='bad-uuid')
        yield self.send_event(bad_event)
        self.assertEqual(self.worker.record, [('unknown_event', bad_event)])

    @inlineCallbacks
    def test_user_message_dispatch(self):
        messages = [
            ('user_message', FakeUserMessage()),
            ('new_session', FakeUserMessage(session_event=SESSION_NEW)),
            ('close_session', FakeUserMessage(session_event=SESSION_CLOSE)),
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
        worker.new_session(FakeUserMessage())
        worker.close_session(FakeUserMessage())


class ApplicationTestCase(TestCase):

    """
    This is a base class for testing application workers.

    """

    # base timeout of 5s for all application tests
    timeout = 5

    transport_name = "sphex"
    transport_type = None
    application_class = None

    def setUp(self):
        self._workers = []
        self._amqp = FakeAMQPBroker()

    def tearDown(self):
        for worker in self._workers:
            worker.stopWorker()

    def rkey(self, name):
        return "%s.%s" % (self.transport_name, name)

    @inlineCallbacks
    def get_application(self, config, cls=None, start=True):
        """
        Get an instance of a worker class.

        :param config: Config dict.
        :param cls: The Application class to instantiate.
                    Defaults to :attr:`application_class`
        :param start: True to start the application (default), False otherwise.

        Some default config values are helpfully provided in the
        interests of reducing boilerplate:

        * ``transport_name`` defaults to :attr:`self.transport_name`
        """

        if cls is None:
            cls = self.application_class
        config.setdefault('transport_name', self.transport_name)
        worker = get_stubbed_worker(cls, config, self._amqp)
        self._workers.append(worker)
        if start:
            yield worker.startWorker()
        returnValue(worker)

    def mkmsg_in(self, content='hello world', message_id='abc',
                 to_addr='9292', from_addr='+41791234567',
                 session_event=None, transport_type=None,
                 helper_metadata=None, transport_metadata=None):
        if transport_type is None:
            transport_type = self.transport_type
        if helper_metadata is None:
            helper_metadata = {}
        if transport_metadata is None:
            transport_metadata = {}
        return TransportUserMessage(
            from_addr=from_addr,
            to_addr=to_addr,
            message_id=message_id,
            transport_name=self.transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            helper_metadata=helper_metadata,
            content=content,
            session_event=session_event,
            timestamp=datetime.now(),
            )

    def mkmsg_out(self, content='hello world', message_id='1',
                  to_addr='+41791234567', from_addr='9292',
                  session_event=None, in_reply_to=None,
                  transport_type=None, transport_metadata=None,
                  stubs=False):
        if transport_type is None:
            transport_type = self.transport_type
        if transport_metadata is None:
            transport_metadata = {}
        params = dict(
            to_addr=to_addr,
            from_addr=from_addr,
            message_id=message_id,
            transport_name=self.transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            session_event=session_event,
            in_reply_to=in_reply_to,
            )
        if stubs:
            params['timestamp'] = datetime.now()
        return TransportUserMessage(**params)

    def get_dispatched_messages(self):
        return self._amqp.get_messages('vumi', self.rkey('outbound'))

    def wait_for_dispatched_messages(self, amount):
        return self._amqp.wait_messages('vumi', self.rkey('outbound'), amount)

    def dispatch(self, message, rkey=None, exchange='vumi'):
        if rkey is None:
            rkey = self.rkey('inbound')
        self._amqp.publish_message(exchange, rkey, message)
        return self._amqp.kick_delivery()
