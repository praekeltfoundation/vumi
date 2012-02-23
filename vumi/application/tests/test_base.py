from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.errors import ConfigError
from vumi.application.base import (ApplicationWorker, DecisionTreeWorker,
                                    SESSION_NEW, SESSION_CLOSE)
from vumi.message import TransportUserMessage, TransportEvent
from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.tests.utils import get_stubbed_worker, FakeRedis
from datetime import datetime


class DummyApplicationWorker(ApplicationWorker):

    SEND_TO_TAGS = frozenset(['default', 'outbound1'])

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
        self.config = {
            'transport_name': self.transport_name,
            'send_to': {
                'default': {
                    'transport_name': 'default_transport',
                    },
                'outbound1': {
                    'transport_name': 'outbound1_transport',
                    },
                },
            }
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

    def assert_msgs_match(self, msgs, expected_msgs):
        for key in ['timestamp', 'message_id']:
            for msg in msgs + expected_msgs:
                self.assertTrue(key in msg.payload)
                msg[key] = 'OVERRIDDEN_BY_TEST'

        for msg, expected_msg in zip(msgs, expected_msgs):
            self.assertEqual(msg, expected_msg)
        self.assertEqual(len(msgs), len(expected_msgs))

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
        self.assert_msgs_match(replies, expecteds)

    def test_reply_to_group(self):
        msg = FakeUserMessage()
        self.worker.reply_to_group(msg, "Group!")
        replies = self.recv()
        expecteds = [msg.reply_group("Group!")]
        self.assert_msgs_match(replies, expecteds)

    def test_send_to(self):
        sent_msg = self.worker.send_to('+12345', "Hi!")
        sends = self.recv()
        expecteds = [TransportUserMessage.send('+12345', "Hi!",
                transport_name='default_transport')]
        self.assert_msgs_match(sends, expecteds)
        self.assert_msgs_match(sends, [sent_msg])

    def test_send_to_with_options(self):
        sent_msg = self.worker.send_to('+12345', "Hi!",
                transport_type=TransportUserMessage.TT_USSD)
        sends = self.recv()
        expecteds = [TransportUserMessage.send('+12345', "Hi!",
                transport_type=TransportUserMessage.TT_USSD,
                transport_name='default_transport')]
        self.assert_msgs_match(sends, expecteds)
        self.assert_msgs_match(sends, [sent_msg])

    def test_send_to_with_tag(self):
        sent_msg = self.worker.send_to('+12345', "Hi!", "outbound1",
                transport_type=TransportUserMessage.TT_USSD)
        sends = self.recv()
        expecteds = [TransportUserMessage.send('+12345', "Hi!",
                transport_type=TransportUserMessage.TT_USSD,
                transport_name='outbound1_transport')]
        self.assert_msgs_match(sends, expecteds)
        self.assert_msgs_match(sends, [sent_msg])

    def test_send_to_with_bad_tag(self):
        self.assertRaises(ValueError, self.worker.send_to,
                          '+12345', "Hi!", "outbound_unknown")

    @inlineCallbacks
    def test_send_to_with_no_send_to_tags(self):
        config = {'transport_name': 'notags_app'}
        notags_worker = get_stubbed_worker(ApplicationWorker,
                                           config=config)
        yield notags_worker.startWorker()
        self.assertRaises(ValueError, notags_worker.send_to,
                          '+12345', "Hi!")

    @inlineCallbacks
    def test_send_to_with_bad_config(self):
        config = {'transport_name': 'badconfig_app',
                  'send_to': {
                      'default': {},  # missing transport_name
                      'outbound1': {},  # also missing transport_name
                      },
                  }
        badcfg_worker = get_stubbed_worker(DummyApplicationWorker,
                                           config=config)
        errors = []
        d = badcfg_worker.startWorker()
        d.addErrback(lambda result: errors.append(result))
        yield d
        self.assertEqual(errors[0].type, ConfigError)

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
        * ``send_to`` defaults to a dictionary with config for each tag
          defined in worker's SEND_TO_TAGS attribute. Each tag's config
          contains a transport_name set to ``<tag>_outbound``.
        """

        if cls is None:
            cls = self.application_class
        config.setdefault('transport_name', self.transport_name)
        if 'send_to' not in config and cls.SEND_TO_TAGS:
            config['send_to'] = {}
            for tag in cls.SEND_TO_TAGS:
                config['send_to'][tag] = {
                    'transport_name': '%s_outbound' % tag}
        worker = get_stubbed_worker(cls, config, self._amqp)
        self._workers.append(worker)
        if start:
            yield worker.startWorker()
        returnValue(worker)

    def mkmsg_in(self, content='hello world', message_id='abc',
                 to_addr='9292', from_addr='+41791234567', group=None,
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
            group=group,
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
                  to_addr='+41791234567', from_addr='9292', group=None,
                  session_event=None, in_reply_to=None,
                  transport_type=None, transport_metadata=None,
                  ):
        if transport_type is None:
            transport_type = self.transport_type
        if transport_metadata is None:
            transport_metadata = {}
        params = dict(
            to_addr=to_addr,
            from_addr=from_addr,
            group=group,
            message_id=message_id,
            transport_name=self.transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            session_event=session_event,
            in_reply_to=in_reply_to,
            )
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


class MockDecisionTreeWorker(DecisionTreeWorker):

    test_yaml = '''
        __data__:
            url: localhost:8080/api/get_data
            username: admin
            password: pass
            params:
                - telNo
            json: "{}"

        __start__:
            display:
                english: "Hello."
            next: users

        users:
            question:
                english: "Hi. There are multiple users with this phone number. Who are you?"
            options: name
            next: toys

        toys:
            question:
                english: "What kind of toys did you make?"
            options: name
            next: quantityToys

        quantityToys:
            question:
                english: "How many toys did you make?"
            validate: integer
            next: quantitySold

        quantitySold:
            question:
                english: "How many toys did you sell?"
            validate: integer
            next: recordTimestamp

        recordTimestamp:
            question:
                english: "When did this happen?"
            options:
                  - display:
                        english: "Today"
                    default: today
                    next: __finish__
                  - display:
                        english: "Yesterday"
                    default: yesterday
                    next: __finish__
                  - display:
                        english: "An earlier day"
                    next:
                        question:
                            english: "Which day was it [dd/mm/yyyy]?"
                        validate: date
                        next: __finish__

        __finish__:
            display:
                english: "Thank you! Your work was recorded successfully."

        __post__:
            url: localhost:8080/api/save_data
            username: admin
            password: pass
            params:
                - result
    '''

    def post_result(self, result):
        self.mock_result = result

    def call_for_json(self):
        return '''{
                    "users": [
                        {
                            "name":"David",
                            "toys": [
                                {
                                    "name":"truck",
                                    "quantityMade": 0,
                                    "recordTimestamp": 0,
                                    "toyId": "toy1",
                                    "quantitySold": 0
                                },
                                {
                                    "name": "car",
                                    "quantityMade": 0,
                                    "recordTimestamp": 0,
                                    "toyId": "toy2",
                                    "quantitySold": 0
                                }
                            ],
                            "timestamp": "1309852944",
                            "userId": "user1"
                        }
                    ],
                    "msisdn": "456789"
                }'''

class TestDecisionTreeWorker(TestCase):

    def replace_timestamp(self, string):
        newstring = re.sub(r'imestamp": "\d*"',
                            'imestamp": "0"',
                            string)
        return newstring

    @inlineCallbacks
    def setUp(self):
        self.transport_name = 'test_transport'
        self.worker = get_stubbed_worker(MockDecisionTreeWorker, {
            'transport_name': self.transport_name,
            'worker_name': 'test_decision_tree',
            'redis': {}
            })
        self.broker = self.worker._amqp_client.broker
        self.worker.r_server = FakeRedis()
        self.worker.set_yaml_template(self.worker.test_yaml)
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
        self.worker.r_server.teardown()
        yield self.worker.stopWorker()

    @inlineCallbacks
    def send(self, content, session_event=None, from_addr=None):
        if from_addr is None:
            from_addr = "456789"
        msg = TransportUserMessage(content=content,
                                   session_event=session_event,
                                   from_addr=from_addr,
                                   to_addr='+5678',
                                   transport_name=self.transport_name,
                                   transport_type='fake',
                                   transport_metadata={})
        self.broker.publish_message('vumi', '%s.inbound' % self.transport_name,
                                    msg)
        yield self.broker.kick_delivery()

    @inlineCallbacks
    def recv(self, n=0):
        msgs = yield self.broker.wait_messages('vumi', '%s.outbound'
                                                % self.transport_name, n)

        def reply_code(msg):
            if msg['session_event'] == TransportUserMessage.SESSION_CLOSE:
                return 'end'
            return 'reply'

        returnValue([(reply_code(msg), msg['content']) for msg in msgs])

    def test_pass(self):
        pass
