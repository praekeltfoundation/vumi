from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.application.base import ApplicationWorker, SESSION_NEW, SESSION_CLOSE
from vumi.message import TransportUserMessage

from vumi.application.tests.helpers import ApplicationHelper
from vumi.tests.helpers import VumiTestCase, WorkerHelper
from vumi.errors import InvalidEndpoint


class DummyApplicationWorker(ApplicationWorker):

    ALLOWED_ENDPOINTS = frozenset(['default', 'outbound1'])

    def __init__(self, *args, **kwargs):
        super(DummyApplicationWorker, self).__init__(*args, **kwargs)
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


class EchoApplicationWorker(ApplicationWorker):
    def consume_user_message(self, message):
        self.reply_to(message, message['content'])


class TestApplicationWorker(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.app_helper = self.add_helper(
            ApplicationHelper(DummyApplicationWorker))
        self.worker = yield self.app_helper.get_application({})

    def assert_msgs_match(self, msgs, expected_msgs):
        for key in ['timestamp', 'message_id']:
            for msg in msgs + expected_msgs:
                self.assertTrue(key in msg.payload)
                msg[key] = 'OVERRIDDEN_BY_TEST'
                if not msg.get('routing_metadata'):
                    msg['routing_metadata'] = {'endpoint_name': 'default'}

        for msg, expected_msg in zip(msgs, expected_msgs):
            self.assertEqual(msg, expected_msg)
        self.assertEqual(len(msgs), len(expected_msgs))

    @inlineCallbacks
    def test_event_dispatch(self):
        events = [
            ('ack', self.app_helper.make_ack()),
            ('nack', self.app_helper.make_nack()),
            ('delivery_report', self.app_helper.make_delivery_report()),
            ]
        for name, event in events:
            yield self.app_helper.dispatch_event(event)
            self.assertEqual(self.worker.record, [(name, event)])
            del self.worker.record[:]

    @inlineCallbacks
    def test_unknown_event_dispatch(self):
        # temporarily pretend the worker doesn't know about acks
        del self.worker._event_handlers['ack']
        bad_event = yield self.app_helper.make_dispatch_ack()
        self.assertEqual(self.worker.record, [('unknown_event', bad_event)])

    @inlineCallbacks
    def test_user_message_dispatch(self):
        messages = [
            ('user_message', self.app_helper.make_inbound("foo")),
            ('new_session', self.app_helper.make_inbound(
                "foo", session_event=SESSION_NEW)),
            ('close_session', self.app_helper.make_inbound(
                "foo", session_event=SESSION_CLOSE)),
            ]
        for name, message in messages:
            yield self.app_helper.dispatch_inbound(message)
            self.assertEqual(self.worker.record, [(name, message)])
            del self.worker.record[:]

    @inlineCallbacks
    def test_reply_to(self):
        msg = self.app_helper.make_inbound("foo")
        yield self.worker.reply_to(msg, "More!")
        yield self.worker.reply_to(msg, "End!", False)
        replies = self.app_helper.get_dispatched_outbound()
        expecteds = [msg.reply("More!"), msg.reply("End!", False)]
        self.assert_msgs_match(replies, expecteds)

    @inlineCallbacks
    def test_waiting_message(self):
        # Get rid of the old worker.
        yield self.app_helper.cleanup_worker(self.worker)
        self.worker = None

        # Stick a message on the queue before starting the worker so it will be
        # received as soon as the message consumer starts consuming.
        msg = yield self.app_helper.make_dispatch_inbound("Hello!")

        # Start the app and process stuff.
        self.worker = yield self.app_helper.get_application(
            {}, EchoApplicationWorker)

        replies = yield self.app_helper.wait_for_dispatched_outbound(1)

        expecteds = [msg.reply("Hello!")]
        self.assert_msgs_match(replies, expecteds)

    @inlineCallbacks
    def test_reply_to_group(self):
        msg = self.app_helper.make_inbound("foo")
        yield self.worker.reply_to_group(msg, "Group!")
        replies = self.app_helper.get_dispatched_outbound()
        expecteds = [msg.reply_group("Group!")]
        self.assert_msgs_match(replies, expecteds)

    @inlineCallbacks
    def test_send_to(self):
        sent_msg = yield self.worker.send_to(
            '+12345', "Hi!", endpoint="default")
        sends = self.app_helper.get_dispatched_outbound()
        expecteds = [TransportUserMessage.send(
            '+12345', "Hi!", transport_name=None)]
        self.assert_msgs_match(sends, expecteds)
        self.assert_msgs_match(sends, [sent_msg])

    @inlineCallbacks
    def test_send_to_with_different_endpoint(self):
        sent_msg = yield self.worker.send_to(
            '+12345', "Hi!", endpoint="outbound1",
            transport_type=TransportUserMessage.TT_USSD)
        sends = self.app_helper.get_dispatched_outbound()
        expecteds = [TransportUserMessage.send(
            '+12345', "Hi!", transport_type=TransportUserMessage.TT_USSD)]
        expecteds[0].set_routing_endpoint("outbound1")
        self.assert_msgs_match(sends, [sent_msg])
        self.assert_msgs_match(sends, expecteds)

    def test_subclassing_api(self):
        worker = WorkerHelper.get_worker_raw(
            ApplicationWorker, {'transport_name': 'test'})
        worker.consume_ack(self.app_helper.make_ack())
        worker.consume_nack(self.app_helper.make_nack())
        worker.consume_delivery_report(self.app_helper.make_delivery_report())
        worker.consume_unknown_event(self.app_helper.make_inbound("foo"))
        worker.consume_user_message(self.app_helper.make_inbound("foo"))
        worker.new_session(self.app_helper.make_inbound("foo"))
        worker.close_session(self.app_helper.make_inbound("foo"))

    def get_app_consumers(self, app):
        for connector in app.connectors.values():
            for consumer in connector._consumers.values():
                yield consumer

    @inlineCallbacks
    def test_application_prefetch_count_custom(self):
        app = yield self.app_helper.get_application({
            'transport_name': 'test',
            'amqp_prefetch_count': 10,
            })
        for consumer in self.get_app_consumers(app):
            self.assertEqual(consumer.channel.qos_prefetch_count, 10)

    @inlineCallbacks
    def test_application_prefetch_count_default(self):
        app = yield self.app_helper.get_application({
            'transport_name': 'test',
            })
        for consumer in self.get_app_consumers(app):
            self.assertEqual(consumer.channel.qos_prefetch_count, 20)

    @inlineCallbacks
    def test_application_prefetch_count_none(self):
        app = yield self.app_helper.get_application({
            'transport_name': 'test',
            'amqp_prefetch_count': None,
            })
        for consumer in self.get_app_consumers(app):
            self.assertFalse(consumer.channel.qos_prefetch_count)

    def assertNotRaises(self, error_class, f, *args, **kw):
        try:
            f(*args, **kw)
        except error_class as e:
            self.fail("%s unexpectedly raised: %s" % (error_class, e))

    @inlineCallbacks
    def test_check_endpoints(self):
        app = yield self.app_helper.get_application({})
        check = app.check_endpoint
        self.assertNotRaises(InvalidEndpoint, check, None, None)
        self.assertNotRaises(InvalidEndpoint, check, None, 'foo')
        self.assertNotRaises(InvalidEndpoint, check, ['default'], None)
        self.assertNotRaises(InvalidEndpoint, check, ['foo'], 'foo')
        self.assertRaises(InvalidEndpoint, check, [], None)
        self.assertRaises(InvalidEndpoint, check, ['foo'], 'bar')


class TestApplicationWorkerWithSendToConfig(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.app_helper = self.add_helper(
            ApplicationHelper(DummyApplicationWorker))
        self.worker = yield self.app_helper.get_application({
            'send_to': {
                'default': {
                    'transport_name': 'default_transport',
                },
                'outbound1': {
                    'transport_name': 'outbound1_transport',
                },
            },
        })

    def assert_msgs_match(self, msgs, expected_msgs):
        for key in ['timestamp', 'message_id']:
            for msg in msgs + expected_msgs:
                self.assertTrue(key in msg.payload)
                msg[key] = 'OVERRIDDEN_BY_TEST'
                if not msg.get('routing_metadata'):
                    msg['routing_metadata'] = {'endpoint_name': 'default'}

        for msg, expected_msg in zip(msgs, expected_msgs):
            self.assertEqual(msg, expected_msg)
        self.assertEqual(len(msgs), len(expected_msgs))

    @inlineCallbacks
    def send_to(self, *args, **kw):
        sent_msg = yield self.worker.send_to(*args, **kw)
        returnValue(sent_msg)

    @inlineCallbacks
    def test_send_to(self):
        sent_msg = yield self.send_to('+12345', "Hi!")
        sends = self.app_helper.get_dispatched_outbound()
        expecteds = [TransportUserMessage.send('+12345', "Hi!",
                transport_name='default_transport')]
        self.assert_msgs_match(sends, expecteds)
        self.assert_msgs_match(sends, [sent_msg])

    @inlineCallbacks
    def test_send_to_with_options(self):
        sent_msg = yield self.send_to(
            '+12345', "Hi!", transport_type=TransportUserMessage.TT_USSD)
        sends = self.app_helper.get_dispatched_outbound()
        expecteds = [TransportUserMessage.send('+12345', "Hi!",
                transport_type=TransportUserMessage.TT_USSD,
                transport_name='default_transport')]
        self.assert_msgs_match(sends, expecteds)
        self.assert_msgs_match(sends, [sent_msg])

    @inlineCallbacks
    def test_send_to_with_endpoint(self):
        sent_msg = yield self.send_to('+12345', "Hi!", "outbound1",
                transport_type=TransportUserMessage.TT_USSD)
        sends = self.app_helper.get_dispatched_outbound()
        expecteds = [TransportUserMessage.send('+12345', "Hi!",
                transport_type=TransportUserMessage.TT_USSD,
                transport_name='outbound1_transport')]
        expecteds[0].set_routing_endpoint("outbound1")
        self.assert_msgs_match(sends, expecteds)
        self.assert_msgs_match(sends, [sent_msg])

    @inlineCallbacks
    def test_send_to_with_bad_endpoint(self):
        yield self.assertFailure(
            self.send_to('+12345', "Hi!", "outbound_unknown"), InvalidEndpoint)


class TestApplicationMiddlewareHooks(VumiTestCase):

    TEST_MIDDLEWARE_CONFIG = {
        "middleware": [
            {"mw1": "vumi.middleware.tests.utils.RecordingMiddleware"},
            {"mw2": "vumi.middleware.tests.utils.RecordingMiddleware"},
        ],
    }

    def setUp(self):
        self.app_helper = self.add_helper(ApplicationHelper(ApplicationWorker))

    @inlineCallbacks
    def test_middleware_for_inbound_messages(self):
        app = yield self.app_helper.get_application(
            self.TEST_MIDDLEWARE_CONFIG)
        msgs = []
        app.consume_user_message = msgs.append
        yield self.app_helper.make_dispatch_inbound("hi")
        [msg] = msgs
        self.assertEqual(msg['record'], [
            ('mw1', 'inbound', self.app_helper.transport_name),
            ('mw2', 'inbound', self.app_helper.transport_name),
            ])

    @inlineCallbacks
    def test_middleware_for_events(self):
        app = yield self.app_helper.get_application(
            self.TEST_MIDDLEWARE_CONFIG)
        msgs = []
        app._event_handlers['ack'] = msgs.append
        yield self.app_helper.make_dispatch_ack()
        [msg] = msgs
        self.assertEqual(msg['record'], [
            ('mw1', 'event', self.app_helper.transport_name),
            ('mw2', 'event', self.app_helper.transport_name),
            ])

    @inlineCallbacks
    def test_middleware_for_outbound_messages(self):
        app = yield self.app_helper.get_application(
            self.TEST_MIDDLEWARE_CONFIG)
        orig_msg = self.app_helper.make_inbound("hi")
        yield app.reply_to(orig_msg, 'Hello!')
        msgs = self.app_helper.get_dispatched_outbound()
        [msg] = msgs
        self.assertEqual(msg['record'], [
            ['mw2', 'outbound', self.app_helper.transport_name],
            ['mw1', 'outbound', self.app_helper.transport_name],
            ])
