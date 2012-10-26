
from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.errors import ConfigError
from vumi.application.base import ApplicationWorker, SESSION_NEW, SESSION_CLOSE
from vumi.message import TransportUserMessage, TransportEvent
from vumi.tests.utils import get_stubbed_worker
from vumi.application.tests.utils import ApplicationTestCase


class DummyApplicationWorker(ApplicationWorker):

    SEND_TO_TAGS = frozenset(['default', 'outbound1'])

    def __init__(self, *args, **kwargs):
        super(ApplicationWorker, self).__init__(*args, **kwargs)
        self.record = []

    def consume_unknown_event(self, event):
        self.record.append(('unknown_event', event))

    def consume_ack(self, event):
        self.record.append(('ack', event))

    def consume_nack(self, event):
        self.record.append(('nack', event))

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


class TestApplicationWorker(ApplicationTestCase):

    application_class = DummyApplicationWorker

    @inlineCallbacks
    def setUp(self):
        yield super(TestApplicationWorker, self).setUp()
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
        self.worker = yield self.get_application(self.config)

    # @inlineCallbacks
    # def send(self, msg, routing_suffix='inbound'):
    #     yield self.dispatch(msg, direction=routing_suffix)

    # @inlineCallbacks
    # def send_event(self, event):
    #     yield self.dispatch_event(event, 'event')

    # def recv(self, routing_suffix='outbound'):
    #     routing_key = "%s.%s" % (self.transport_name, routing_suffix)
    #     return self.broker.get_messages("vumi", routing_key)

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
            ('ack', self.mkmsg_ack(sent_message_id='remote-id',
                                   user_message_id='ack-uuid')),
            ('nack', self.mkmsg_nack(user_message_id='nack-uuid')),
            ('delivery_report', self.mkmsg_delivery(
                                        user_message_id='dr-uuid')),
            ]
        for name, event in events:
            yield self.dispatch_event(event)
            self.assertEqual(self.worker.record, [(name, event)])
            del self.worker.record[:]

    @inlineCallbacks
    def test_unknown_event_dispatch(self):
        # temporarily pretend the worker doesn't know about acks
        del self.worker._event_handlers['ack']
        bad_event = TransportEvent(event_type='ack',
                                   sent_message_id='remote-id',
                                   user_message_id='bad-uuid')
        yield self.dispatch_event(bad_event)
        self.assertEqual(self.worker.record, [('unknown_event', bad_event)])

    @inlineCallbacks
    def test_user_message_dispatch(self):
        messages = [
            ('user_message', FakeUserMessage()),
            ('new_session', FakeUserMessage(session_event=SESSION_NEW)),
            ('close_session', FakeUserMessage(session_event=SESSION_CLOSE)),
            ]
        for name, message in messages:
            yield self.dispatch(message)
            self.assertEqual(self.worker.record, [(name, message)])
            del self.worker.record[:]

    @inlineCallbacks
    def test_reply_to(self):
        msg = FakeUserMessage()
        yield self.worker.reply_to(msg, "More!")
        yield self.worker.reply_to(msg, "End!", False)
        replies = self.get_dispatched_messages()
        expecteds = [msg.reply("More!"), msg.reply("End!", False)]
        self.assert_msgs_match(replies, expecteds)

    @inlineCallbacks
    def test_reply_to_group(self):
        msg = FakeUserMessage()
        yield self.worker.reply_to_group(msg, "Group!")
        replies = self.get_dispatched_messages()
        expecteds = [msg.reply_group("Group!")]
        self.assert_msgs_match(replies, expecteds)

    @inlineCallbacks
    def test_send_to(self):
        sent_msg = yield self.worker.send_to('+12345', "Hi!")
        sends = self.get_dispatched_messages()
        expecteds = [TransportUserMessage.send('+12345', "Hi!",
                transport_name='default_transport')]
        self.assert_msgs_match(sends, expecteds)
        self.assert_msgs_match(sends, [sent_msg])

    @inlineCallbacks
    def test_send_to_with_options(self):
        sent_msg = yield self.worker.send_to('+12345', "Hi!",
                transport_type=TransportUserMessage.TT_USSD)
        sends = self.get_dispatched_messages()
        expecteds = [TransportUserMessage.send('+12345', "Hi!",
                transport_type=TransportUserMessage.TT_USSD,
                transport_name='default_transport')]
        self.assert_msgs_match(sends, expecteds)
        self.assert_msgs_match(sends, [sent_msg])

    @inlineCallbacks
    def test_send_to_with_tag(self):
        sent_msg = yield self.worker.send_to('+12345', "Hi!", "outbound1",
                transport_type=TransportUserMessage.TT_USSD)
        sends = self.get_dispatched_messages()
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
        worker.consume_ack(self.mkmsg_ack())
        worker.consume_nack(self.mkmsg_nack())
        worker.consume_delivery_report(self.mkmsg_delivery())
        worker.consume_unknown_event(FakeUserMessage())
        worker.consume_user_message(FakeUserMessage())
        worker.new_session(FakeUserMessage())
        worker.close_session(FakeUserMessage())


class TestApplicationMiddlewareHooks(ApplicationTestCase):

    transport_name = 'carrier_pigeon'
    application_class = ApplicationWorker

    TEST_MIDDLEWARE_CONFIG = {
       "middleware": [
            {"mw1": "vumi.middleware.tests.utils.RecordingMiddleware"},
            {"mw2": "vumi.middleware.tests.utils.RecordingMiddleware"},
            ],
        }

    @inlineCallbacks
    def test_middleware_for_inbound_messages(self):
        app = yield self.get_application(self.TEST_MIDDLEWARE_CONFIG)
        msgs = []
        app.consume_user_message = msgs.append
        orig_msg = self.mkmsg_in()
        orig_msg['timestamp'] = 0
        yield app.dispatch_user_message(orig_msg)
        [msg] = msgs
        self.assertEqual(msg['record'], [
            ('mw1', 'inbound', self.transport_name),
            ('mw2', 'inbound', self.transport_name),
            ])

    @inlineCallbacks
    def test_middleware_for_events(self):
        app = yield self.get_application(self.TEST_MIDDLEWARE_CONFIG)
        msgs = []
        app._event_handlers['ack'] = msgs.append
        orig_msg = self.mkmsg_ack()
        orig_msg['event_id'] = 1234
        orig_msg['timestamp'] = 0
        yield app.dispatch_event(orig_msg)
        [msg] = msgs
        self.assertEqual(msg['record'], [
            ('mw1', 'event', self.transport_name),
            ('mw2', 'event', self.transport_name),
            ])

    @inlineCallbacks
    def test_middleware_for_outbound_messages(self):
        app = yield self.get_application(self.TEST_MIDDLEWARE_CONFIG)
        orig_msg = self.mkmsg_out()
        yield app.reply_to(orig_msg, 'Hello!')
        msgs = self.get_dispatched_messages()
        [msg] = msgs
        self.assertEqual(msg['record'], [
            ['mw2', 'outbound', self.transport_name],
            ['mw1', 'outbound', self.transport_name],
            ])
