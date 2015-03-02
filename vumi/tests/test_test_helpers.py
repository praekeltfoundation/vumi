from copy import deepcopy
from datetime import datetime

from twisted.internet.defer import Deferred, succeed, inlineCallbacks
from twisted.trial.unittest import SkipTest, TestCase, FailTest

from vumi.blinkenlights.metrics import MetricMessage
from vumi.message import TransportUserMessage, TransportEvent
from vumi.tests.fake_amqp import FakeAMQPBroker, FakeAMQClient
from vumi.tests.helpers import (
    VumiTestCase, proxyable, generate_proxies, IHelper, import_skip,
    MessageHelper, WorkerHelper, MessageDispatchHelper, PersistenceHelper,
    success_result_of, RiakDisabledForTest)
from vumi.worker import BaseWorker


class TestHelperHelpers(TestCase):
    def test_proxyable(self):
        """
        @proxyable should set a `proxyable` attr on the func it decorates.
        """

        @proxyable
        def is_proxyable():
            pass
        self.assertTrue(hasattr(is_proxyable, 'proxyable'))

        def not_proxyable():
            pass
        self.assertFalse(hasattr(not_proxyable, 'proxyable'))

    def test_generate_proxies(self):
        """
        generate_proxies() should copy proxyable source attrs to target.
        """

        class Source(object):
            @proxyable
            def is_proxyable(self):
                return self

            def not_proxyable(self):
                pass

        class Target(object):
            pass

        source = Source()
        target = Target()

        self.assertFalse(hasattr(target, 'is_proxyable'))
        self.assertFalse(hasattr(target, 'not_proxyable'))

        generate_proxies(target, source)

        self.assertTrue(hasattr(target, 'is_proxyable'))
        self.assertFalse(hasattr(target, 'not_proxyable'))

        # `self` in both the original and proxied versions should be the source
        # rather than the target.
        self.assertEqual(source, source.is_proxyable())
        self.assertEqual(source, target.is_proxyable())

    def test_generate_proxies_multiple_sources(self):
        """
        generate_proxies() should copy attrs from multiple sources.
        """

        class Source1(object):
            @proxyable
            def is_proxyable_1(self):
                return self

        class Source2(object):
            @proxyable
            def is_proxyable_2(self):
                return self

        class Target(object):
            pass

        source1 = Source1()
        source2 = Source2()
        target = Target()

        self.assertFalse(hasattr(target, 'is_proxyable_1'))
        self.assertFalse(hasattr(target, 'is_proxyable_2'))

        generate_proxies(target, source1)
        generate_proxies(target, source2)

        self.assertTrue(hasattr(target, 'is_proxyable_1'))
        self.assertTrue(hasattr(target, 'is_proxyable_2'))

        # `self` in the proxied versions should be the appropriate source.
        self.assertEqual(source1, target.is_proxyable_1())
        self.assertEqual(source2, target.is_proxyable_2())

    def test_generate_proxies_multiple_sources_overlap(self):
        """
        generate_proxies() shouldn't copy proxyables with existing names.
        """

        class Source1(object):
            @proxyable
            def is_proxyable(self):
                return self

        class Source2(object):
            @proxyable
            def is_proxyable(self):
                return self

        class Target(object):
            pass

        source1 = Source1()
        source2 = Source2()
        target = Target()

        generate_proxies(target, source1)
        err = self.assertRaises(Exception, generate_proxies, target, source2)
        self.assertTrue('is_proxyable' in err.args[0])

    def test_import_skip_skips(self):
        """
        import_skip() should raise a SkipTest exception if given an ImportError
        referencing an expected module name.
        """
        try:
            import badmodule
            self.fail(
                "Expected ImportError for %r, nothing raised." % (badmodule,))
        except ImportError as import_error:
            self.assertRaises(SkipTest, import_skip, import_error, 'badmodule')

    def test_import_skip_reraises(self):
        """
        import_skip() should reraise the given ImportError if it does not
        reference an expected module name.
        """
        try:
            import badmodule
            self.fail(
                "Expected ImportError for %r, nothing raised." % (badmodule,))
        except ImportError as import_error:
            err = self.assertRaises(
                ImportError, import_skip, import_error, 'nothing')
            self.assertEqual(err, import_error)

    def test_success_result_of_no_result(self):
        """
        success_result_of() should raise a FailTest exception if the Deferred
        has no result.
        """
        d = Deferred()
        err = self.assertRaises(FailTest, success_result_of, d)
        self.assertEqual(
            err.args[0], "No result available for deferred: %r" % (d,))

    def test_success_result_of_failure(self):
        """
        success_result_of() should raise a FailTest exception if the Deferred
        has a failure result.
        """
        d = Deferred()
        d.errback(Exception())
        err = self.assertRaises(FailTest, success_result_of, d)
        self.assertTrue(err.args[0].startswith(
            "Expected success from deferred %r, got failure:" % (d,)))

    def test_success_result_of_success(self):
        """
        success_result_of() should raise a FailTest exception if the Deferred
        has no result.
        """
        d = Deferred()
        d.callback('foo')
        self.assertEqual(success_result_of(d), 'foo')


class TestMessageHelper(TestCase):
    def assert_message_fields(self, msg, field_dict):
        self.assertEqual(field_dict, dict(
            (k, v) for k, v in msg.payload.iteritems() if k in field_dict))

    def test_implements_IHelper(self):
        """
        MessageHelper instances should provide the IHelper interface.
        """
        self.assertTrue(IHelper.providedBy(MessageHelper()))

    def test_defaults(self):
        """
        MessageHelper instances should have the expected parameter defaults.
        """
        msg_helper = MessageHelper()
        self.assertEqual(msg_helper.transport_name, 'sphex')
        self.assertEqual(msg_helper.transport_type, 'sms')
        self.assertEqual(msg_helper.mobile_addr, '+41791234567')
        self.assertEqual(msg_helper.transport_addr, '9292')

    def test_setup_sync(self):
        """
        MessageHelper.setup() should return ``None``, not a Deferred.
        """
        msg_helper = MessageHelper()
        self.assertEqual(msg_helper.setup(), None)

    def test_cleanup_sync(self):
        """
        MessageHelper.cleanup() should return ``None``, not a Deferred.
        """
        msg_helper = MessageHelper()
        self.assertEqual(msg_helper.cleanup(), None)

    def test_make_inbound_defaults(self):
        """
        .make_inbound() should build a message with expected default values.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_inbound('inbound message')
        self.assert_message_fields(msg, {
            'content': 'inbound message',
            'from_addr': msg_helper.mobile_addr,
            'to_addr': msg_helper.transport_addr,
            'transport_type': msg_helper.transport_type,
            'transport_name': msg_helper.transport_name,
            'helper_metadata': {},
            'transport_metadata': {},
        })

    def test_make_inbound_with_addresses(self):
        """
        .make_inbound() should use overridden addresses if provided.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_inbound(
            'inbound message', from_addr='ib_from', to_addr='ib_to')
        self.assert_message_fields(msg, {
            'content': 'inbound message',
            'from_addr': 'ib_from',
            'to_addr': 'ib_to',
            'transport_type': msg_helper.transport_type,
            'transport_name': msg_helper.transport_name,
            'helper_metadata': {},
            'transport_metadata': {},
        })

    def test_make_inbound_with_helper_metadata(self):
        """
        .make_inbound() should use overridden helper_metadata if provided.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_inbound('inbound message', helper_metadata={
            'foo': {'bar': 'baz'},
            'quux': {},
        })
        self.assert_message_fields(msg, {
            'content': 'inbound message',
            'from_addr': msg_helper.mobile_addr,
            'to_addr': msg_helper.transport_addr,
            'transport_type': msg_helper.transport_type,
            'transport_name': msg_helper.transport_name,
            'helper_metadata': {
                'foo': {'bar': 'baz'},
                'quux': {},
            },
            'transport_metadata': {},
        })

    def test_make_outbound_defaults(self):
        """
        .make_outbound() should build a message with expected default values.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_outbound('outbound message')
        self.assert_message_fields(msg, {
            'content': 'outbound message',
            'from_addr': msg_helper.transport_addr,
            'to_addr': msg_helper.mobile_addr,
            'transport_type': msg_helper.transport_type,
            'transport_name': msg_helper.transport_name,
            'helper_metadata': {},
            'transport_metadata': {},
        })

    def test_make_outbound_with_addresses(self):
        """
        .make_outbound() should use overridden addresses if provided.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_outbound(
            'outbound message', from_addr='ob_from', to_addr='ob_to')
        self.assert_message_fields(msg, {
            'content': 'outbound message',
            'from_addr': 'ob_from',
            'to_addr': 'ob_to',
            'transport_type': msg_helper.transport_type,
            'transport_name': msg_helper.transport_name,
            'helper_metadata': {},
            'transport_metadata': {},
        })

    def test_make_outbound_with_helper_metadata(self):
        """
        .make_outbound() should use overridden helper_metadata if provided.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_outbound('outbound message', helper_metadata={
            'foo': {'bar': 'baz'},
            'quux': {},
        })
        self.assert_message_fields(msg, {
            'content': 'outbound message',
            'from_addr': msg_helper.transport_addr,
            'to_addr': msg_helper.mobile_addr,
            'transport_type': msg_helper.transport_type,
            'transport_name': msg_helper.transport_name,
            'helper_metadata': {
                'foo': {'bar': 'baz'},
                'quux': {},
            },
            'transport_metadata': {},
        })

    def test_make_user_message_defaults(self):
        """
        .make_user_message() should build a message with expected values.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_user_message('outbound message', 'from', 'to')
        expected_msg = TransportUserMessage(
            content='outbound message', from_addr='from', to_addr='to',
            transport_type=msg_helper.transport_type,
            transport_name=msg_helper.transport_name,
            transport_metadata={}, helper_metadata={},
            # These fields are generated in both messages, so copy them.
            message_id=msg['message_id'], timestamp=msg['timestamp'])
        self.assertEqual(expected_msg, msg)

    def test_make_user_message_all_fields(self):
        """
        .make_user_message() should build a message with all provided fields.
        """
        msg_helper = MessageHelper()
        msg_fields = {
            'content': 'outbound message',
            'from_addr': 'from',
            'from_addr_type': 'msisdn',
            'to_addr': 'to',
            'to_addr_type': 'mxit_id',
            'group': '#channel',
            'provider': 'MY-MNO',
            'session_event': TransportUserMessage.SESSION_NEW,
            'transport_type': 'irc',
            'transport_name': 'vuminet',
            'transport_metadata': {'foo': 'bar'},
            'helper_metadata': {'foo': {}},
            'in_reply_to': 'ccf9c2b9b1e94433be20d157e82786fe',
            'timestamp': datetime.utcnow(),
            'message_id': 'bbf9c2b9b1e94433be20d157e82786ed',
            'endpoint': 'foo_ep',
        }
        msg = msg_helper.make_user_message(**msg_fields)
        expected_fields = msg_fields.copy()
        expected_fields.update({
            'message_type': TransportUserMessage.MESSAGE_TYPE,
            'message_version': TransportUserMessage.MESSAGE_VERSION,
            'routing_metadata': {
                'endpoint_name': expected_fields.pop('endpoint'),
            }
        })
        self.assertEqual(expected_fields, msg.payload)

    def test_make_user_message_extra_fields(self):
        """
        .make_user_message() should build a message with extra fields.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_user_message(
            'outbound message', 'from', 'to', foo='bar', baz='quux')
        self.assert_message_fields(msg, {'foo': 'bar', 'baz': 'quux'})

    def test_make_event_defaults_ack(self):
        """
        .make_event() should build an ack event with expected values.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_event('ack', 'abc123', sent_message_id='sent')
        expected_event = TransportEvent(
            event_type='ack', user_message_id='abc123', sent_message_id='sent',
            transport_type=msg_helper.transport_type,
            transport_name=msg_helper.transport_name,
            transport_metadata={}, helper_metadata={},
            # These fields are generated in both messages, so copy them.
            event_id=event['event_id'], timestamp=event['timestamp'])
        self.assertEqual(expected_event, event)

    def test_make_event_defaults_nack(self):
        """
        .make_event() should build a nack event with expected values.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_event('nack', 'abc123', nack_reason='elves')
        expected_event = TransportEvent(
            event_type='nack', user_message_id='abc123', nack_reason='elves',
            transport_type=msg_helper.transport_type,
            transport_name=msg_helper.transport_name,
            transport_metadata={}, helper_metadata={},
            # These fields are generated in both messages, so copy them.
            event_id=event['event_id'], timestamp=event['timestamp'])
        self.assertEqual(expected_event, event)

    def test_make_event_defaults_dr(self):
        """
        .make_event() should build a delivery report with expected values.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_event(
            'delivery_report', 'abc123', delivery_status='pending')
        expected_event = TransportEvent(
            event_type='delivery_report', user_message_id='abc123',
            delivery_status='pending',
            transport_type=msg_helper.transport_type,
            transport_name=msg_helper.transport_name,
            transport_metadata={}, helper_metadata={},
            # These fields are generated in both messages, so copy them.
            event_id=event['event_id'], timestamp=event['timestamp'])
        self.assertEqual(expected_event, event)

    def test_make_event_all_fields(self):
        """
        .make_event() should build an event with all provided fields.
        """
        msg_helper = MessageHelper()
        event_fields = {
            'event_type': 'ack',
            'user_message_id': 'abc123',
            'sent_message_id': '123abc',
            'transport_type': 'irc',
            'transport_name': 'vuminet',
            'transport_metadata': {'foo': 'bar'},
            'helper_metadata': {'foo': {}},

            'timestamp': datetime.utcnow(),
            'event_id': 'e6b7efecda8e42988b1e6905ad40fae1',
            'endpoint': 'foo_ep',
        }
        event = msg_helper.make_event(**event_fields)
        expected_fields = event_fields.copy()
        expected_fields.update({
            'message_type': TransportEvent.MESSAGE_TYPE,
            'message_version': TransportEvent.MESSAGE_VERSION,
            'routing_metadata': {
                'endpoint_name': expected_fields.pop('endpoint'),
            }
        })
        self.assertEqual(expected_fields, event.payload)

    def test_make_event_extra_fields(self):
        """
        .make_event() should build an event with extra fields.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_event(
            'ack', 'abc123', sent_message_id='sent', foo='bar', baz='quux')
        self.assert_message_fields(event, {'foo': 'bar', 'baz': 'quux'})

    def test_make_ack_default(self):
        """
        .make_ack() should build an ack event with expected values.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_ack()
        self.assert_message_fields(event, {
            'event_type': 'ack',
            'sent_message_id': event['user_message_id'],
        })

    def test_make_ack_with_sent_message_id(self):
        """
        .make_ack() should build an ack with the provided sent_message_id.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_ack(sent_message_id='abc123')
        self.assert_message_fields(event, {
            'event_type': 'ack',
            'sent_message_id': 'abc123',
        })

    def test_make_ack_with_message(self):
        """
        .make_ack() should build an ack event for the provided message.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_outbound('test message')
        event = msg_helper.make_ack(msg)
        self.assert_message_fields(event, {
            'event_type': 'ack',
            'user_message_id': msg['message_id'],
            'sent_message_id': msg['message_id'],
        })

    def test_make_nack_default(self):
        """
        .make_nack() should build a nack event with expected values.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_nack()
        self.assert_message_fields(event, {
            'event_type': 'nack',
            'nack_reason': 'sunspots',
        })

    def test_make_nack_with_nack_reason(self):
        """
        .make_nack() should build a nack with the provided nack_reason.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_nack(nack_reason='bogon emissions')
        self.assert_message_fields(event, {
            'event_type': 'nack',
            'nack_reason': 'bogon emissions',
        })

    def test_make_nack_with_message(self):
        """
        .make_nack() should build a nack event for the provided message.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_outbound('test message')
        event = msg_helper.make_nack(msg)
        self.assert_message_fields(event, {
            'event_type': 'nack',
            'user_message_id': msg['message_id'],
            'nack_reason': 'sunspots',
        })

    def test_make_delivery_report_default(self):
        """
        .make_delivery_report() should build an event with expected values.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_delivery_report()
        self.assert_message_fields(event, {
            'event_type': 'delivery_report',
            'delivery_status': 'delivered',
        })

    def test_make_delivery_report_with_delivery_statuss(self):
        """
        .make_delivery_report() should build an event with the provided
        delivery_status.
        """
        msg_helper = MessageHelper()
        event = msg_helper.make_delivery_report(delivery_status='pending')
        self.assert_message_fields(event, {
            'event_type': 'delivery_report',
            'delivery_status': 'pending',
        })

    def test_make_delivery_report_with_message(self):
        """
        .make_delivery_report() should build an event for the provided message.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_outbound('test message')
        event = msg_helper.make_delivery_report(msg)
        self.assert_message_fields(event, {
            'event_type': 'delivery_report',
            'user_message_id': msg['message_id'],
            'delivery_status': 'delivered',
        })

    def test_make_reply(self):
        """
        .make_reply() should build a reply for the given message and content.
        """
        msg_helper = MessageHelper()
        msg = msg_helper.make_inbound('inbound')
        reply = msg_helper.make_reply(msg, 'reply content')
        self.assert_message_fields(reply, {
            'content': 'reply content',
            'to_addr': msg['from_addr'],
            'from_addr': msg['to_addr'],
            'in_reply_to': msg['message_id'],
        })


class FakeWorker(object):
    def __init__(self, stop_d=succeed(None)):
        self._stop_d = stop_d

    def stopWorker(self):
        return self._stop_d


class FakeBroker(object):
    def __init__(self, delivery_d=succeed(None)):
        self._delivery_d = delivery_d

    def wait_delivery(self):
        return self._delivery_d


class ToyWorker(BaseWorker):
    worker_started = False
    worker_stopped = False

    def setup_heartbeat(self):
        # Overriden to skip heartbeat setup.
        pass

    def setup_connectors(self):
        pass

    def setup_worker(self):
        self.worker_started = True

    def teardown_worker(self):
        self.worker_stopped = True


class TestWorkerHelper(VumiTestCase):
    def test_implements_IHelper(self):
        """
        WorkerHelper instances should provide the IHelper interface.
        """
        self.assertTrue(IHelper.providedBy(WorkerHelper()))

    def test_defaults(self):
        """
        WorkerHelper instances should have the expected parameter defaults.
        """
        worker_helper = WorkerHelper()
        self.assertEqual(worker_helper._connector_name, None)
        self.assertIsInstance(worker_helper.broker, FakeAMQPBroker)

    def test_all_params(self):
        """
        WorkerHelper should use the provided broker and connector name.
        """
        broker = FakeBroker()
        worker_helper = WorkerHelper("my_connector", broker)
        self.assertEqual(worker_helper._connector_name, "my_connector")
        self.assertEqual(worker_helper.broker, broker)

    def test_setup_sync(self):
        """
        WorkerHelper.setup() should return ``None``, not a Deferred.
        """
        worker_helper = WorkerHelper()
        self.assertEqual(worker_helper.setup(), None)

    def test_cleanup(self):
        """
        WorkerHelper.cleanup() should wait for broker delivery and stop all
        workers.
        """
        delivery_d = Deferred()
        worker_stop_d = Deferred()
        worker_helper = WorkerHelper(broker=FakeBroker(delivery_d=delivery_d))
        worker_helper._workers.append(FakeWorker(stop_d=worker_stop_d))
        d = worker_helper.cleanup()
        self.assertFalse(d.called)
        delivery_d.callback(None)
        self.assertFalse(d.called)
        worker_stop_d.callback(None)
        self.assertTrue(d.called)

    def test_cleanup_worker(self):
        """
        WorkerHelper.cleanup_worker() should remove the worker from its list
        and then stop it.
        """
        worker_stop_d = Deferred()
        worker = FakeWorker(stop_d=worker_stop_d)
        worker_helper = WorkerHelper()
        worker_helper._workers.append(worker)
        d = worker_helper.cleanup_worker(worker)
        self.assertEqual(worker_helper._workers, [])
        self.assertEqual(d, worker_stop_d)

    def test_get_fake_amqp_client(self):
        """
        WorkerHelper.get_fake_amqp_client() should return a FakeAMQClient
        wrapping the given broker.
        """
        broker = FakeBroker()
        client = WorkerHelper.get_fake_amqp_client(broker)
        self.assertIsInstance(client, FakeAMQClient)
        self.assertEqual(client.broker, broker)

    def test_get_worker_raw(self):
        """
        WorkerHelper.get_worker_raw() should create an instance of the given
        worker class with the given config.
        """
        broker = FakeBroker()
        worker = WorkerHelper.get_worker_raw(
            BaseWorker, {'foo': 'bar'}, broker)
        self.assertIsInstance(worker, BaseWorker)
        self.assertIsInstance(worker._amqp_client, FakeAMQClient)
        self.assertEqual(worker._amqp_client.broker, broker)
        self.assertEqual(worker.config, {
            'foo': 'bar',
            # worker_name is added for us if we don't provide one.
            'worker_name': 'unnamed',
        })

    def test_get_worker_raw_worker_name_configured(self):
        """
        WorkerHelper.get_worker_raw() should not overwrite worker_name.
        """
        broker = FakeBroker()
        worker = WorkerHelper.get_worker_raw(
            BaseWorker, {'worker_name': 'Gilbert'}, broker)
        self.assertIsInstance(worker, BaseWorker)
        self.assertIsInstance(worker._amqp_client, FakeAMQClient)
        self.assertEqual(worker._amqp_client.broker, broker)
        self.assertEqual(worker.config, {'worker_name': 'Gilbert'})

    def test_get_worker_raw_config_None(self):
        """
        WorkerHelper.get_worker_raw() can take ``None`` as a config.
        """
        broker = FakeBroker()
        worker = WorkerHelper.get_worker_raw(
            BaseWorker, None, broker)
        self.assertIsInstance(worker, BaseWorker)
        self.assertIsInstance(worker._amqp_client, FakeAMQClient)
        self.assertEqual(worker._amqp_client.broker, broker)
        self.assertEqual(worker.config, {})

    def test_get_worker(self):
        """
        WorkerHelper.get_worker() should create an instance of the given worker
        class with the given config, start it and return it.
        """
        worker_helper = WorkerHelper()
        worker_d = worker_helper.get_worker(ToyWorker, {'foo': 'bar'})
        worker = success_result_of(worker_d)
        self.assertIsInstance(worker, ToyWorker)
        self.assertIsInstance(worker._amqp_client, FakeAMQClient)
        self.assertEqual(worker._amqp_client.broker, worker_helper.broker)
        self.assertEqual(worker.config, {
            'foo': 'bar',
            # worker_name is added for us if we don't provide one.
            'worker_name': 'unnamed',
        })
        self.assertTrue(worker.worker_started)

    def test_get_worker_no_start(self):
        """
        WorkerHelper.get_worker() should not start the worker if asked not to.
        """
        worker_helper = WorkerHelper()
        worker_d = worker_helper.get_worker(ToyWorker, {}, start=False)
        worker = success_result_of(worker_d)
        self.assertIsInstance(worker, ToyWorker)
        self.assertIsInstance(worker._amqp_client, FakeAMQClient)
        self.assertEqual(worker._amqp_client.broker, worker_helper.broker)
        self.assertFalse(worker.worker_started)

    def _add_to_dispatched(self, broker, rkey, msg, kick=False):
        broker.exchange_declare('vumi', 'direct')
        broker.publish_message('vumi', rkey, msg)
        if kick:
            return broker.kick_delivery()

    def _add_to_dispatched_metrics(self, broker, msg):
        broker.exchange_declare('vumi.metrics', 'direct')
        broker.publish_message('vumi.metrics', 'vumi.metrics', msg)

    def test_get_dispatched(self):
        """
        WorkerHelper.get_dispatched() should get messages dispatched by the
        broker.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        dispatched = worker_helper.get_dispatched(
            'fooconn', 'inbound', TransportUserMessage)
        self.assertEqual(dispatched, [])
        msg = msg_helper.make_inbound('message')
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.inbound', msg)
        dispatched = worker_helper.get_dispatched(
            'fooconn', 'inbound', TransportUserMessage)
        self.assertEqual(dispatched, [msg])

    def test_get_dispatched_None_connector(self):
        """
        WorkerHelper.get_dispatched() should use the default connector if
        `None` is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        dispatched = worker_helper.get_dispatched(
            None, 'inbound', TransportUserMessage)
        self.assertEqual(dispatched, [])
        msg = msg_helper.make_inbound('message')
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.inbound', msg)
        dispatched = worker_helper.get_dispatched(
            None, 'inbound', TransportUserMessage)
        self.assertEqual(dispatched, [msg])

    def test_clear_all_dispatched(self):
        """
        WorkerHelper.clear_all_dispatched() should clear all messages
        dispatched by the broker.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        broker = worker_helper.broker
        msg = msg_helper.make_inbound('message')
        self._add_to_dispatched(broker, 'fooconn.inbound', msg)
        self._add_to_dispatched_metrics(broker, MetricMessage())
        self.assertNotEqual(broker.dispatched['vumi'], {})
        self.assertNotEqual(broker.dispatched['vumi.metrics'], {})
        worker_helper.clear_all_dispatched()
        self.assertEqual(broker.dispatched['vumi'], {})
        self.assertEqual(broker.dispatched['vumi.metrics'], {})

    def test_clear_all_dispatched_no_messages(self):
        """
        WorkerHelper.clear_all_dispatched() should succeed even if no messages
        have ever been dispatched.
        """
        worker_helper = WorkerHelper()
        broker = worker_helper.broker
        self.assertEqual(broker.dispatched, {})
        worker_helper.clear_all_dispatched()
        self.assertEqual(broker.dispatched, {})

    def test_get_dispatched_events(self):
        """
        WorkerHelper.get_dispatched_events() should get events dispatched by
        the broker.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        dispatched = worker_helper.get_dispatched_events('fooconn')
        self.assertEqual(dispatched, [])
        msg = msg_helper.make_ack()
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.event', msg)
        dispatched = worker_helper.get_dispatched_events('fooconn')
        self.assertEqual(dispatched, [msg])

    def test_get_dispatched_events_no_connector(self):
        """
        WorkerHelper.get_dispatched_events() should use the default connector
        if none is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        dispatched = worker_helper.get_dispatched_events()
        self.assertEqual(dispatched, [])
        msg = msg_helper.make_ack()
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.event', msg)
        dispatched = worker_helper.get_dispatched_events()
        self.assertEqual(dispatched, [msg])

    def test_get_dispatched_inbound(self):
        """
        WorkerHelper.get_dispatched_inbound() should get inbound messages
        dispatched by the broker.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        dispatched = worker_helper.get_dispatched_inbound('fooconn')
        self.assertEqual(dispatched, [])
        msg = msg_helper.make_inbound('message')
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.inbound', msg)
        dispatched = worker_helper.get_dispatched_inbound('fooconn')
        self.assertEqual(dispatched, [msg])

    def test_get_dispatched_inbound_no_connector(self):
        """
        WorkerHelper.get_dispatched_inbound() should use the default connector
        if none is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        dispatched = worker_helper.get_dispatched_inbound()
        self.assertEqual(dispatched, [])
        msg = msg_helper.make_inbound('message')
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.inbound', msg)
        dispatched = worker_helper.get_dispatched_inbound()
        self.assertEqual(dispatched, [msg])

    def test_get_dispatched_outbound(self):
        """
        WorkerHelper.get_dispatched_outbound() should get outbound messages
        dispatched by the broker.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        dispatched = worker_helper.get_dispatched_outbound('fooconn')
        self.assertEqual(dispatched, [])
        msg = msg_helper.make_outbound('message')
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.outbound', msg)
        dispatched = worker_helper.get_dispatched_outbound('fooconn')
        self.assertEqual(dispatched, [msg])

    def test_get_dispatched_outbound_no_connector(self):
        """
        WorkerHelper.get_dispatched_outbound() should use the default connector
        if none is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        dispatched = worker_helper.get_dispatched_outbound()
        self.assertEqual(dispatched, [])
        msg = msg_helper.make_outbound('message')
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.outbound', msg)
        dispatched = worker_helper.get_dispatched_outbound()
        self.assertEqual(dispatched, [msg])

    @inlineCallbacks
    def test_wait_for_dispatched_events(self):
        """
        WorkerHelper.wait_for_dispatched_events() should wait for events
        dispatched by the broker.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        d = worker_helper.wait_for_dispatched_events(1, 'fooconn')
        self.assertEqual(d.called, False)
        msg = msg_helper.make_ack()
        yield self._add_to_dispatched(
            worker_helper.broker, 'fooconn.event', msg, kick=True)
        dispatched = success_result_of(d)
        self.assertEqual(dispatched, [msg])

    @inlineCallbacks
    def test_wait_for_dispatched_events_no_connector(self):
        """
        WorkerHelper.wait_for_dispatched_events() should get use the default
        connector if none is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        d = worker_helper.wait_for_dispatched_events(1)
        self.assertEqual(d.called, False)
        msg = msg_helper.make_ack()
        yield self._add_to_dispatched(
            worker_helper.broker, 'fooconn.event', msg, kick=True)
        dispatched = success_result_of(d)
        self.assertEqual(dispatched, [msg])

    @inlineCallbacks
    def test_wait_for_dispatched_inbound(self):
        """
        WorkerHelper.wait_for_dispatched_inbound() should wait for
        inbound messages dispatched by the broker.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        d = worker_helper.wait_for_dispatched_inbound(1, 'fooconn')
        msg = msg_helper.make_inbound('message')
        yield self._add_to_dispatched(
            worker_helper.broker, 'fooconn.inbound', msg, kick=True)
        dispatched = success_result_of(d)
        self.assertEqual(dispatched, [msg])

    @inlineCallbacks
    def test_wait_for_dispatched_inbound_no_connector(self):
        """
        WorkerHelper.wait_for_dispatched_inbound() should use the default
        connector if none is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        d = worker_helper.wait_for_dispatched_inbound(1)
        msg = msg_helper.make_inbound('message')
        yield self._add_to_dispatched(
            worker_helper.broker, 'fooconn.inbound', msg, kick=True)
        dispatched = success_result_of(d)
        self.assertEqual(dispatched, [msg])

    @inlineCallbacks
    def test_wait_for_dispatched_outbound(self):
        """
        WorkerHelper.wait_for_dispatched_outbound() should wait for outbound
        messages dispatched by the broker.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        d = worker_helper.wait_for_dispatched_outbound(1, 'fooconn')
        msg = msg_helper.make_outbound('message')
        yield self._add_to_dispatched(
            worker_helper.broker, 'fooconn.outbound', msg, kick=True)
        dispatched = success_result_of(d)
        self.assertEqual(dispatched, [msg])

    @inlineCallbacks
    def test_wait_for_dispatched_outbound_no_connector(self):
        """
        WorkerHelper.wait_for_dispatched_outbound() should use the default
        connector if none is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        d = worker_helper.wait_for_dispatched_outbound(1)
        msg = msg_helper.make_outbound('message')
        yield self._add_to_dispatched(
            worker_helper.broker, 'fooconn.outbound', msg, kick=True)
        dispatched = success_result_of(d)
        self.assertEqual(dispatched, [msg])

    def test_clear_dispatched_events(self):
        """
        WorkerHelper.clear_dispatched_events() should clear events messages
        dispatched to a particular endpoint from the broker.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        msg = msg_helper.make_ack()
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.event', msg)
        self.assertNotEqual(
            worker_helper.broker.dispatched['vumi']['fooconn.event'], [])
        worker_helper.clear_dispatched_events('fooconn')
        self.assertEqual(
            worker_helper.broker.dispatched['vumi']['fooconn.event'], [])

    def test_clear_dispatched_events_no_connector(self):
        """
        WorkerHelper.clear_dispatched_events() should use the default connector
        if none is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        msg = msg_helper.make_ack()
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.event', msg)
        self.assertNotEqual(
            worker_helper.broker.dispatched['vumi']['fooconn.event'], [])
        worker_helper.clear_dispatched_events()
        self.assertEqual(
            worker_helper.broker.dispatched['vumi']['fooconn.event'], [])

    def test_clear_dispatched_inbound(self):
        """
        WorkerHelper.clear_dispatched_inbound() should clear inbound messages
        dispatched to a particular endpoint from the broker.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        msg = msg_helper.make_inbound('message')
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.inbound', msg)
        self.assertNotEqual(
            worker_helper.broker.dispatched['vumi']['fooconn.inbound'], [])
        worker_helper.clear_dispatched_inbound('fooconn')
        self.assertEqual(
            worker_helper.broker.dispatched['vumi']['fooconn.inbound'], [])

    def test_clear_dispatched_inbound_no_connector(self):
        """
        WorkerHelper.clear_dispatched_inbound() should use the default
        connector if none is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        msg = msg_helper.make_inbound('message')
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.inbound', msg)
        self.assertNotEqual(
            worker_helper.broker.dispatched['vumi']['fooconn.inbound'], [])
        worker_helper.clear_dispatched_inbound()
        self.assertEqual(
            worker_helper.broker.dispatched['vumi']['fooconn.inbound'], [])

    def test_clear_dispatched_outbound(self):
        """
        WorkerHelper.clear_dispatched_outbound() should clear outbound messages
        dispatched to a particular endpoint from the broker.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        msg = msg_helper.make_outbound('message')
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.outbound', msg)
        self.assertNotEqual(
            worker_helper.broker.dispatched['vumi']['fooconn.outbound'], [])
        worker_helper.clear_dispatched_outbound('fooconn')
        self.assertEqual(
            worker_helper.broker.dispatched['vumi']['fooconn.outbound'], [])

    def test_clear_dispatched_outbound_no_connector(self):
        """
        WorkerHelper.clear_dispatched_outbound() should use the default
        connector if none is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        msg = msg_helper.make_outbound('message')
        self._add_to_dispatched(
            worker_helper.broker, 'fooconn.outbound', msg)
        self.assertNotEqual(
            worker_helper.broker.dispatched['vumi']['fooconn.outbound'], [])
        worker_helper.clear_dispatched_outbound()
        self.assertEqual(
            worker_helper.broker.dispatched['vumi']['fooconn.outbound'], [])

    @inlineCallbacks
    def test_dispatch_raw(self):
        """
        WorkerHelper.dispatch_raw() should dispatch a message.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        broker = worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.foo'), [])
        msg = msg_helper.make_inbound('message')
        yield worker_helper.dispatch_raw('fooconn.foo', msg)
        self.assertEqual(broker.get_messages('vumi', 'fooconn.foo'), [msg])

    @inlineCallbacks
    def test_dispatch_raw_with_exchange(self):
        """
        WorkerHelper.dispatch_raw() should dispatch a message on the specified
        exchange.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        broker = worker_helper.broker
        broker.exchange_declare('blah', 'direct')
        self.assertEqual(broker.get_messages('blah', 'fooconn.foo'), [])
        msg = msg_helper.make_inbound('message')
        yield worker_helper.dispatch_raw('fooconn.foo', msg, exchange='blah')
        self.assertEqual(broker.get_messages('blah', 'fooconn.foo'), [msg])

    @inlineCallbacks
    def test_dispatch_event(self):
        """
        WorkerHelper.dispatch_event() should dispatch an event message.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        broker = worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.event'), [])
        msg = msg_helper.make_ack()
        yield worker_helper.dispatch_event(msg, 'fooconn')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.event'), [msg])

    @inlineCallbacks
    def test_dispatch_event_no_connector(self):
        """
        WorkerHelper.dispatch_event() should use the default connector if
        none is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        broker = worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.event'), [])
        msg = msg_helper.make_ack()
        yield worker_helper.dispatch_event(msg)
        self.assertEqual(broker.get_messages('vumi', 'fooconn.event'), [msg])

    @inlineCallbacks
    def test_dispatch_inbound(self):
        """
        WorkerHelper.dispatch_inbound() should dispatch an inbound message.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        broker = worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.inbound'), [])
        msg = msg_helper.make_inbound('message')
        yield worker_helper.dispatch_inbound(msg, 'fooconn')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.inbound'), [msg])

    @inlineCallbacks
    def test_dispatch_inbound_no_connector(self):
        """
        WorkerHelper.dispatch_inbound() should use the default connector if
        none is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        broker = worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.inbound'), [])
        msg = msg_helper.make_inbound('message')
        yield worker_helper.dispatch_inbound(msg)
        self.assertEqual(broker.get_messages('vumi', 'fooconn.inbound'), [msg])

    @inlineCallbacks
    def test_dispatch_outbound(self):
        """
        WorkerHelper.dispatch_outbound() should dispatch an outbound message.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper()
        broker = worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.outbound'), [])
        msg = msg_helper.make_outbound('message')
        yield worker_helper.dispatch_outbound(msg, 'fooconn')
        self.assertEqual(
            broker.get_messages('vumi', 'fooconn.outbound'), [msg])

    @inlineCallbacks
    def test_dispatch_outbound_no_connector(self):
        """
        WorkerHelper.dispatch_outbound() should use the default connector if
        none is passed in.
        """
        msg_helper = MessageHelper()
        worker_helper = WorkerHelper(connector_name='fooconn')
        broker = worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.outbound'), [])
        msg = msg_helper.make_outbound('message')
        yield worker_helper.dispatch_outbound(msg)
        self.assertEqual(
            broker.get_messages('vumi', 'fooconn.outbound'), [msg])

    def test_get_dispatched_metrics(self):
        """
        WorkerHelper.get_dispatched_metrics() should get metrics dispatched by
        the broker.
        """
        worker_helper = WorkerHelper()
        dispatched = worker_helper.get_dispatched_metrics()
        self.assertEqual(dispatched, [])

        msg = MetricMessage()
        self._add_to_dispatched_metrics(worker_helper.broker, msg)
        dispatched = worker_helper.get_dispatched_metrics()
        self.assertEqual(dispatched, [[]])

        msg = MetricMessage()
        msg.append('fake metric 1')
        msg.append('fake metric 2')
        self._add_to_dispatched_metrics(worker_helper.broker, msg)
        dispatched = worker_helper.get_dispatched_metrics()
        self.assertEqual(dispatched, [[], ['fake metric 1', 'fake metric 2']])

    def test_clear_dispatched_metrics(self):
        """
        WorkerHelper.clear_dispatched_metrics() should clear metrics messages
        dispatched from the broker.
        """
        worker_helper = WorkerHelper()
        self._add_to_dispatched_metrics(worker_helper.broker, MetricMessage())
        self.assertNotEqual(
            worker_helper.broker.dispatched['vumi.metrics'], {})
        worker_helper.clear_dispatched_metrics()
        self.assertEqual(
            worker_helper.broker.dispatched['vumi.metrics'], {})


class TestMessageDispatchHelper(VumiTestCase):
    def assert_message_fields(self, msg, field_dict):
        self.assertEqual(field_dict, dict(
            (k, v) for k, v in msg.payload.iteritems() if k in field_dict))

    def test_implements_IHelper(self):
        """
        MessageDispatchHelper instances should provide the IHelper interface.
        """
        self.assertTrue(IHelper.providedBy(MessageDispatchHelper(None, None)))

    def test_setup_sync(self):
        """
        MessageDispatchHelper.setup() should return ``None``, not a Deferred.
        """
        md_helper = MessageDispatchHelper(None, None)
        self.assertEqual(md_helper.setup(), None)

    def test_cleanup_sync(self):
        """
        MessageDispatchHelper.cleanup() should return ``None``, not a Deferred.
        """
        md_helper = MessageDispatchHelper(None, None)
        self.assertEqual(md_helper.cleanup(), None)

    @inlineCallbacks
    def test_make_dispatch_inbound_defaults(self):
        """
        .make_dispatch_inbound() should build and dispatch an inbound message.
        """
        md_helper = MessageDispatchHelper(
            MessageHelper(), WorkerHelper('fooconn'))
        broker = md_helper.worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.inbound'), [])
        msg = yield md_helper.make_dispatch_inbound('inbound message')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.inbound'), [msg])
        self.assert_message_fields(msg, {
            'content': 'inbound message',
            'from_addr': md_helper.msg_helper.mobile_addr,
            'to_addr': md_helper.msg_helper.transport_addr,
            'transport_type': md_helper.msg_helper.transport_type,
            'transport_name': md_helper.msg_helper.transport_name,
            'helper_metadata': {},
            'transport_metadata': {},
        })

    @inlineCallbacks
    def test_make_dispatch_inbound_with_addresses(self):
        """
        .make_dispatch_inbound() should build and dispatch an inbound message
        with non-default parameters.
        """
        md_helper = MessageDispatchHelper(
            MessageHelper(), WorkerHelper('fooconn'))
        broker = md_helper.worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.inbound'), [])
        msg = yield md_helper.make_dispatch_inbound(
            'inbound message', from_addr='ib_from', to_addr='ib_to')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.inbound'), [msg])
        self.assert_message_fields(msg, {
            'content': 'inbound message',
            'from_addr': 'ib_from',
            'to_addr': 'ib_to',
            'transport_type': md_helper.msg_helper.transport_type,
            'transport_name': md_helper.msg_helper.transport_name,
            'helper_metadata': {},
            'transport_metadata': {},
        })

    @inlineCallbacks
    def test_make_dispatch_outbound_defaults(self):
        """
        .make_dispatch_outbound() should build and dispatch an outbound
        message.
        """
        md_helper = MessageDispatchHelper(
            MessageHelper(), WorkerHelper('fooconn'))
        broker = md_helper.worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.outbound'), [])
        msg = yield md_helper.make_dispatch_outbound('outbound message')
        self.assertEqual(
            broker.get_messages('vumi', 'fooconn.outbound'), [msg])
        self.assert_message_fields(msg, {
            'content': 'outbound message',
            'from_addr': md_helper.msg_helper.transport_addr,
            'to_addr': md_helper.msg_helper.mobile_addr,
            'transport_type': md_helper.msg_helper.transport_type,
            'transport_name': md_helper.msg_helper.transport_name,
            'helper_metadata': {},
            'transport_metadata': {},
        })

    @inlineCallbacks
    def test_make_dispatch_outbound_with_addresses(self):
        """
        .make_dispatch_outbound() should build and dispatch an outbound message
        with non-default parameters.
        """
        md_helper = MessageDispatchHelper(
            MessageHelper(), WorkerHelper('fooconn'))
        broker = md_helper.worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.outbound'), [])
        msg = yield md_helper.make_dispatch_outbound(
            'outbound message', from_addr='ob_from', to_addr='ob_to')
        self.assertEqual(
            broker.get_messages('vumi', 'fooconn.outbound'), [msg])
        self.assert_message_fields(msg, {
            'content': 'outbound message',
            'from_addr': 'ob_from',
            'to_addr': 'ob_to',
            'transport_type': md_helper.msg_helper.transport_type,
            'transport_name': md_helper.msg_helper.transport_name,
            'helper_metadata': {},
            'transport_metadata': {},
        })

    @inlineCallbacks
    def test_make_dispatch_ack_default(self):
        """
        .make_dispatch_ack() should build and dispatch an ack event.
        """
        md_helper = MessageDispatchHelper(
            MessageHelper(), WorkerHelper('fooconn'))
        broker = md_helper.worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.event'), [])
        event = yield md_helper.make_dispatch_ack()
        self.assertEqual(
            broker.get_messages('vumi', 'fooconn.event'), [event])
        self.assert_message_fields(event, {
            'event_type': 'ack',
            'sent_message_id': event['user_message_id'],
        })

    @inlineCallbacks
    def test_make_ack_with_sent_message_id(self):
        """
        .make_dispatch_ack() should build and dispatch an ack event with
        non-default parameters.
        """
        md_helper = MessageDispatchHelper(
            MessageHelper(), WorkerHelper('fooconn'))
        broker = md_helper.worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.event'), [])
        msg = md_helper.msg_helper.make_outbound('test message')
        event = yield md_helper.make_dispatch_ack(
            msg, sent_message_id='abc123')
        self.assertEqual(
            broker.get_messages('vumi', 'fooconn.event'), [event])
        self.assert_message_fields(event, {
            'event_type': 'ack',
            'user_message_id': msg['message_id'],
            'sent_message_id': 'abc123',
        })

    @inlineCallbacks
    def test_make_dispatch_nack_default(self):
        """
        .make_dispatch_nack() should build and dispatch a nack event.
        """
        md_helper = MessageDispatchHelper(
            MessageHelper(), WorkerHelper('fooconn'))
        broker = md_helper.worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.event'), [])
        event = yield md_helper.make_dispatch_nack()
        self.assertEqual(
            broker.get_messages('vumi', 'fooconn.event'), [event])
        self.assert_message_fields(event, {
            'event_type': 'nack',
            'nack_reason': 'sunspots',
        })

    @inlineCallbacks
    def test_make_nack_with_sent_message_id(self):
        """
        .make_dispatch_nack() should build and dispatch a nack event with
        non-default parameters.
        """
        md_helper = MessageDispatchHelper(
            MessageHelper(), WorkerHelper('fooconn'))
        broker = md_helper.worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.event'), [])
        msg = md_helper.msg_helper.make_outbound('test message')
        event = yield md_helper.make_dispatch_nack(
            msg, nack_reason='bogon emissions')
        self.assertEqual(
            broker.get_messages('vumi', 'fooconn.event'), [event])
        self.assert_message_fields(event, {
            'event_type': 'nack',
            'user_message_id': msg['message_id'],
            'nack_reason': 'bogon emissions',
        })

    @inlineCallbacks
    def test_make_dispatch_delivery_report_default(self):
        """
        .make_dispatch_delivery_report() should build and dispatch a
        delivery_report.
        """
        md_helper = MessageDispatchHelper(
            MessageHelper(), WorkerHelper('fooconn'))
        broker = md_helper.worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.event'), [])
        event = yield md_helper.make_dispatch_delivery_report()
        self.assertEqual(
            broker.get_messages('vumi', 'fooconn.event'), [event])
        self.assert_message_fields(event, {
            'event_type': 'delivery_report',
            'delivery_status': 'delivered',
        })

    @inlineCallbacks
    def test_make_delivery_report_with_sent_message_id(self):
        """
        .make_dispatch_delivery_report() should build and dispatch a delivery
        report with non-default parameters.
        """
        md_helper = MessageDispatchHelper(
            MessageHelper(), WorkerHelper('fooconn'))
        broker = md_helper.worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.event'), [])
        msg = md_helper.msg_helper.make_outbound('test message')
        event = yield md_helper.make_dispatch_delivery_report(
            msg, delivery_status='pending')
        self.assertEqual(
            broker.get_messages('vumi', 'fooconn.event'), [event])
        self.assert_message_fields(event, {
            'event_type': 'delivery_report',
            'user_message_id': msg['message_id'],
            'delivery_status': 'pending',
        })

    @inlineCallbacks
    def test_make_reply(self):
        """
        .make_dispatch_reply() should build and dispatch a reply message.
        """
        md_helper = MessageDispatchHelper(
            MessageHelper(), WorkerHelper('fooconn'))
        broker = md_helper.worker_helper.broker
        broker.exchange_declare('vumi', 'direct')
        self.assertEqual(broker.get_messages('vumi', 'fooconn.outbound'), [])
        msg = md_helper.msg_helper.make_inbound('inbound')
        reply = yield md_helper.make_dispatch_reply(msg, 'reply content')
        self.assertEqual(
            broker.get_messages('vumi', 'fooconn.outbound'), [reply])
        self.assert_message_fields(reply, {
            'content': 'reply content',
            'to_addr': msg['from_addr'],
            'from_addr': msg['to_addr'],
            'in_reply_to': msg['message_id'],
        })


class FakeRiakManagerForCleanup(object):
    purged = False

    def __init__(self, bucket_prefix, conns=None):
        self.bucket_prefix = bucket_prefix
        self.fake_conns = conns or []

    def purge_all(self):
        self.purged = True
        return 'maybe async'

    def close_manager(self):
        for conn in self.fake_conns:
            conn.close()
        self.fake_conns = []


class FakeRiakClientConnection(object):
    closed = False

    def close(self):
        self.closed = True


class FakeRedisManagerForCleanup(object):
    purged = False
    connected = True

    def __init__(self, key_prefix):
        self._key_prefix = key_prefix

    def _purge_all(self):
        if not self.connected:
            raise RuntimeError('Not connected')
        self.purged = True

    def close_manager(self):
        self.connected = False


class TestPersistenceHelper(VumiTestCase):
    @property
    def _RiakManager(self):
        try:
            from vumi.persist.riak_manager import RiakManager
        except ImportError, e:
            import_skip(e, 'riak')
        return RiakManager

    @property
    def _TxRiakManager(self):
        try:
            from vumi.persist.txriak_manager import TxRiakManager
        except ImportError, e:
            import_skip(e, 'riak')
        return TxRiakManager

    @property
    def _RedisManager(self):
        try:
            from vumi.persist.redis_manager import RedisManager
        except ImportError, e:
            import_skip(e, 'redis')
        return RedisManager

    @property
    def _TxRedisManager(self):
        from vumi.persist.txredis_manager import TxRedisManager
        return TxRedisManager

    def test_implements_IHelper(self):
        """
        PersistenceHelper instances should provide the IHelper interface.
        """
        self.assertTrue(IHelper.providedBy(PersistenceHelper()))

    def test_defaults(self):
        """
        PersistenceHelper instances should have the expected parameter
        defaults.
        """
        persistence_helper = PersistenceHelper()
        self.assertEqual(persistence_helper.use_riak, False)
        self.assertEqual(persistence_helper.is_sync, False)

    def test_all_params(self):
        """
        PersistenceHelper instances should accept ``use_riak`` and ``is_sync``
        params.
        """
        persistence_helper = PersistenceHelper(use_riak=True, is_sync=True)
        self.assertEqual(persistence_helper.use_riak, True)
        self.assertEqual(persistence_helper.is_sync, True)

    def get_manager_inits(self):
        return (
            self._RiakManager.__init__,
            self._TxRiakManager.__init__,
            self._RedisManager.__init__,
            self._TxRedisManager.__init__,
        )

    def test_setup_applies_patches(self):
        """
        PersistenceHelper.setup() should apply patches to the persistence
        managers and return ``None``, not a Deferred.
        """
        manager_inits = self.get_manager_inits()
        persistence_helper = PersistenceHelper()
        self.assertEqual(persistence_helper._patches_applied, False)
        self.assertEqual(manager_inits, self.get_manager_inits())

        self.assertEqual(persistence_helper.setup(), None)
        self.assertEqual(persistence_helper._patches_applied, True)
        self.assertNotEqual(manager_inits, self.get_manager_inits())

        # Clean up after ourselves.
        persistence_helper._unpatch()
        self.assertEqual(persistence_helper._patches_applied, False)
        self.assertEqual(manager_inits, self.get_manager_inits())

    def test_cleanup_restores_patches(self):
        """
        PersistenceHelper.cleanup() should restore any patches applied by
        PersistenceHelper.setup().
        """
        manager_inits = self.get_manager_inits()
        persistence_helper = PersistenceHelper()
        self.assertEqual(persistence_helper.setup(), None)
        self.assertEqual(persistence_helper._patches_applied, True)
        self.assertNotEqual(manager_inits, self.get_manager_inits())

        success_result_of(persistence_helper.cleanup())
        self.assertEqual(persistence_helper._patches_applied, False)
        self.assertEqual(manager_inits, self.get_manager_inits())

    def test_get_riak_manager_unpatched(self):
        """
        .get_riak_manager() should fail if .setup() has not been called.
        """
        persistence_helper = PersistenceHelper()
        err = self.assertRaises(Exception, persistence_helper.get_riak_manager)
        self.assertTrue('setup() must be called' in str(err))

    def test_get_redis_manager_unpatched(self):
        """
        .get_redis_manager() should fail if .setup() has not been called.
        """
        persistence_helper = PersistenceHelper()
        err = self.assertRaises(
            Exception, persistence_helper.get_redis_manager)
        self.assertTrue('setup() must be called' in str(err))

    def test_mk_config_unpatched(self):
        """
        .mk_config() should fail if .setup() has not been called.
        """
        persistence_helper = PersistenceHelper()
        err = self.assertRaises(Exception, persistence_helper.mk_config, {})
        self.assertTrue('setup() must be called' in str(err))

    def test_get_riak_manager_no_riak(self):
        """
        .get_riak_manager() should fail if ``use_riak`` is ``False``.
        """
        persistence_helper = self.add_helper(PersistenceHelper())
        err = self.assertRaises(Exception, persistence_helper.get_riak_manager)
        self.assertTrue(
            'Use of Riak has been disabled for this test.' in str(err))

    def test_get_riak_manager_sync(self):
        """
        .get_riak_manager() should return a RiakManager if ``is_sync`` is
        ``True``.
        """
        persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True, is_sync=True))
        manager = persistence_helper.get_riak_manager()
        self.assertIsInstance(manager, self._RiakManager)
        self.assertEqual(persistence_helper._riak_managers, [manager])

    def test_get_riak_manager_async(self):
        """
        .get_riak_manager() should return a TxRiakManager if ``is_sync`` is
        ``False``.
        """
        persistence_helper = self.add_helper(PersistenceHelper(use_riak=True))
        manager = persistence_helper.get_riak_manager()
        self.assertIsInstance(manager, self._TxRiakManager)
        self.assertEqual(persistence_helper._riak_managers, [manager])

    def test_get_redis_manager_sync(self):
        """
        .get_redis_manager() should return a RedisManager if ``is_sync`` is
        ``True``.
        """
        persistence_helper = self.add_helper(PersistenceHelper(is_sync=True))
        manager = persistence_helper.get_redis_manager()
        self.assertIsInstance(manager, self._RedisManager)
        self.assertEqual(persistence_helper._redis_managers, [manager])

    @inlineCallbacks
    def test_get_redis_manager_async(self):
        """
        .get_redis_manager() should return a Deferred that fires with a
        TxRedisManager if ``is_sync`` is ``False``.
        """
        persistence_helper = self.add_helper(PersistenceHelper())
        manager_d = persistence_helper.get_redis_manager()
        self.assertIsInstance(manager_d, Deferred)
        manager = yield manager_d
        self.assertIsInstance(manager, self._TxRedisManager)
        self.assertEqual(persistence_helper._redis_managers, [manager])

    def test_mk_config(self):
        """
        .mk_config() should return a copy of the provided config with
        riak_manager and redis_manager fields overridden.
        """
        persistence_helper = self.add_helper(PersistenceHelper())
        config = {}
        new_config = persistence_helper.mk_config(config)
        self.assertEqual(
            ['redis_manager', 'riak_manager'], sorted(new_config.keys()))
        self.assertEqual(config, {})

    def test_deepcopy_RiakDisabledForTest(self):
        """
        RiakDisabledForTest should be deepcopy()able.
        """
        rdft = RiakDisabledForTest()
        self.assertEqual(rdft, deepcopy(rdft))

    def test__get_riak_managers_for_cleanup(self):
        """
        ._get_riak_managers_for_cleanup() should return the known Riak managers
        in reverse order with appropriate boolean determining whether they
        should be purged or not.
        """
        persistence_helper = PersistenceHelper()
        managers = [
            FakeRiakManagerForCleanup('bucket1'),
            FakeRiakManagerForCleanup('bucket2'),
            FakeRiakManagerForCleanup('bucket2'),
            FakeRiakManagerForCleanup('bucket1'),
            FakeRiakManagerForCleanup('bucket3'),
        ]
        persistence_helper._riak_managers.extend(managers)
        self.assertEqual(
            list(persistence_helper._get_riak_managers_for_cleanup()),
            list(reversed(zip([True, True, False, False, True], managers))))

    def test__get_redis_managers_for_cleanup(self):
        """
        ._get_redis_managers_for_cleanup() should return the known Redis
        managers in reverse order with appropriate boolean determining whether
        they should be purged or not.
        """
        persistence_helper = PersistenceHelper()
        managers = [
            FakeRedisManagerForCleanup('prefix1'),
            FakeRedisManagerForCleanup('prefix2'),
            FakeRedisManagerForCleanup('prefix2'),
            FakeRedisManagerForCleanup('prefix1'),
            FakeRedisManagerForCleanup('prefix3'),
        ]
        persistence_helper._redis_managers.extend(managers)
        self.assertEqual(
            list(persistence_helper._get_redis_managers_for_cleanup()),
            list(reversed(zip([True, True, False, False, True], managers))))

    def test__purge_riak(self):
        """
        ._purge_riak() should call manager.purge_all().
        """
        persistence_helper = PersistenceHelper()
        manager = FakeRiakManagerForCleanup('prefix1')
        self.assertEqual(manager.purged, False)
        self.assertEqual(
            persistence_helper._purge_riak(manager), 'maybe async')
        self.assertEqual(manager.purged, True)

    def test__purge_redis(self):
        """
        ._purge_redis() should call manager._purge_all() and remain connected.
        """
        persistence_helper = PersistenceHelper()
        manager = FakeRedisManagerForCleanup('prefix1')
        self.assertEqual(manager.purged, False)
        self.assertEqual(manager.connected, True)
        success_result_of(persistence_helper._purge_redis(manager))
        self.assertEqual(manager.purged, True)
        self.assertEqual(manager.connected, True)

    def test__purge_redis_not_connected(self):
        """
        ._purge_redis() should ignore riak managers that aren't connected.
        """
        persistence_helper = PersistenceHelper()
        manager = FakeRedisManagerForCleanup('prefix1')
        manager.close_manager()
        self.assertEqual(manager.purged, False)
        success_result_of(persistence_helper._purge_redis(manager))
        self.assertEqual(manager.purged, False)

    def test_cleanup_purges_managers(self):
        """
        .cleanup() should purge the Riak and Redis managers that need purging.
        """
        persistence_helper = PersistenceHelper()
        riak_purge = FakeRiakManagerForCleanup('bucket1')
        riak_nopurge = FakeRiakManagerForCleanup('bucket1')
        redis_purge = FakeRedisManagerForCleanup('prefix1')
        redis_nopurge = FakeRedisManagerForCleanup('prefix1')
        persistence_helper._riak_managers.extend([riak_purge, riak_nopurge])
        persistence_helper._redis_managers.extend([redis_purge, redis_nopurge])

        success_result_of(persistence_helper.cleanup())
        self.assertEqual(
            [True, False], [riak_purge.purged, riak_nopurge.purged])
        self.assertEqual(
            [True, False], [redis_purge.purged, redis_nopurge.purged])

    def test_cleanup_closes_riak_managers(self):
        """
        .cleanup() should close Riak client connections.
        """
        persistence_helper = PersistenceHelper()
        conn1 = FakeRiakClientConnection()
        conn2 = FakeRiakClientConnection()
        manager = FakeRiakManagerForCleanup('bucket1', [conn1, conn2])
        persistence_helper._riak_managers.append(manager)
        self.assertEqual(manager.fake_conns, [conn1, conn2])
        self.assertEqual([conn1.closed, conn2.closed], [False, False])
        success_result_of(persistence_helper.cleanup())
        self.assertEqual(manager.fake_conns, [])
        self.assertEqual([conn1.closed, conn2.closed], [True, True])
