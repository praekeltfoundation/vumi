from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.connectors import (
    BaseConnector, ReceiveInboundConnector, ReceiveOutboundConnector,
    IgnoreMessage)
from vumi.tests.utils import VumiWorkerTestCase, LogCatcher
from vumi.worker import BaseWorker
from vumi.message import TransportUserMessage
from vumi.middleware.tests.utils import RecordingMiddleware


class DummyWorker(BaseWorker):
    def setup_connectors(self):
        pass

    def setup_worker(self):
        pass

    def teardown_worker(self):
        pass


class BaseConnectorTestCase(VumiWorkerTestCase):

    connector_class = None

    @inlineCallbacks
    def mk_connector(self, worker=None, connector_name=None,
                     prefetch_count=None, middlewares=None, setup=False):
        if worker is None:
            worker = yield self.get_worker({}, DummyWorker)
        if connector_name is None:
            connector_name = "dummy_connector"
        connector = self.connector_class(worker, connector_name,
                                         prefetch_count=prefetch_count,
                                         middlewares=middlewares)
        if setup:
            yield connector.setup()
        returnValue(connector)

    @inlineCallbacks
    def mk_consumer(self, *args, **kwargs):
        conn = yield self.mk_connector(*args, **kwargs)
        consumer = yield conn._setup_consumer('inbound', TransportUserMessage,
                                              lambda msg: None)
        returnValue((conn, consumer))


class TestBaseConnector(BaseConnectorTestCase):

    connector_class = BaseConnector

    @inlineCallbacks
    def test_creation(self):
        conn = yield self.mk_connector(connector_name="foo")
        self.assertEqual(conn.name, "foo")
        self.assertTrue(isinstance(conn.worker, BaseWorker))

    @inlineCallbacks
    def test_middlewares_consume(self):
        worker = yield self.get_worker({}, DummyWorker)
        middlewares = [RecordingMiddleware(str(i), {}, worker)
                       for i in range(3)]
        conn, consumer = yield self.mk_consumer(
            worker=worker, connector_name='foo', middlewares=middlewares)
        consumer.unpause()
        msgs = []
        conn._set_default_endpoint_handler('inbound', msgs.append)
        msg = self.mkmsg_in()
        yield self.dispatch_inbound(msg, connector_name='foo')
        record = msgs[0].payload.pop('record')
        self.assertEqual(record,
                         [(str(i), 'inbound', 'foo')
                          for i in range(3)])

    @inlineCallbacks
    def test_middlewares_publish(self):
        worker = yield self.get_worker({}, DummyWorker)
        middlewares = [RecordingMiddleware(str(i), {}, worker)
                       for i in range(3)]
        conn = yield self.mk_connector(
            worker=worker, connector_name='foo', middlewares=middlewares)
        yield conn._setup_publisher('outbound')
        msg = self.mkmsg_out()
        yield conn._publish_message('outbound', msg, 'dummy_endpoint')
        msgs = yield self.get_dispatched_outbound(connector_name='foo')
        record = msgs[0].payload.pop('record')
        self.assertEqual(record,
                         [[str(i), 'outbound', 'foo']
                          for i in range(2, -1, -1)])

    @inlineCallbacks
    def test_pretech_count(self):
        conn, consumer = yield self.mk_consumer(prefetch_count=10)
        self.assertEqual(consumer.channel.qos_prefetch_count, 10)

    @inlineCallbacks
    def test_setup_raises(self):
        conn = yield self.mk_connector()
        self.assertRaises(NotImplementedError, conn.setup)

    @inlineCallbacks
    def test_teardown(self):
        conn, consumer = yield self.mk_consumer()
        self.assertTrue(consumer.keep_consuming)
        yield conn.teardown()
        self.assertFalse(consumer.keep_consuming)

    @inlineCallbacks
    def test_paused(self):
        conn, consumer = yield self.mk_consumer()
        consumer.pause()
        self.assertTrue(conn.paused)
        consumer.unpause()
        self.assertFalse(conn.paused)

    @inlineCallbacks
    def test_pause(self):
        conn, consumer = yield self.mk_consumer()
        consumer.unpause()
        self.assertFalse(consumer.paused)
        conn.pause()
        self.assertTrue(consumer.paused)

    @inlineCallbacks
    def test_unpause(self):
        conn, consumer = yield self.mk_consumer()
        consumer.pause()
        self.assertTrue(consumer.paused)
        conn.unpause()
        self.assertFalse(consumer.paused)

    @inlineCallbacks
    def test_setup_publisher(self):
        conn = yield self.mk_connector(connector_name='foo')
        publisher = yield conn._setup_publisher('outbound')
        self.assertEqual(publisher.routing_key, 'foo.outbound')

    @inlineCallbacks
    def test_setup_consumer(self):
        conn, consumer = yield self.mk_consumer(connector_name='foo')
        self.assertTrue(consumer.paused)
        self.assertEqual(consumer.routing_key, 'foo.inbound')
        self.assertEqual(consumer.message_class, TransportUserMessage)

    @inlineCallbacks
    def test_set_endpoint_handler(self):
        conn, consumer = yield self.mk_consumer(connector_name='foo')
        consumer.unpause()
        msgs = []
        conn._set_endpoint_handler('inbound', msgs.append, 'dummy_endpoint')
        msg = self.mkmsg_in()
        msg.set_routing_endpoint('dummy_endpoint')
        yield self.dispatch_inbound(msg, connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_set_none_endpoint_handler(self):
        conn, consumer = yield self.mk_consumer(connector_name='foo')
        consumer.unpause()
        msgs = []
        conn._set_endpoint_handler('inbound', msgs.append, None)
        msg = self.mkmsg_in()
        yield self.dispatch_inbound(msg, connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_set_default_endpoint_handler(self):
        conn, consumer = yield self.mk_consumer(connector_name='foo')
        consumer.unpause()
        msgs = []
        conn._set_default_endpoint_handler('inbound', msgs.append)
        msg = self.mkmsg_in()
        yield self.dispatch_inbound(msg, connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_publish_message_with_endpoint(self):
        conn = yield self.mk_connector(connector_name='foo')
        yield conn._setup_publisher('outbound')
        msg = self.mkmsg_out()
        yield conn._publish_message('outbound', msg, 'dummy_endpoint')
        msgs = yield self.get_dispatched_outbound(connector_name='foo')
        self.assertEqual(msgs, [msg])


class TestReceiveInboundConnector(BaseConnectorTestCase):

    connector_class = ReceiveInboundConnector

    @inlineCallbacks
    def test_setup(self):
        conn = yield self.mk_connector(connector_name='foo')
        yield conn.setup()
        conn.unpause()

        with LogCatcher() as lc:
            msg = self.mkmsg_in()
            yield self.dispatch_inbound(msg, connector_name='foo')
            [msg_log] = lc.messages()
            self.assertTrue(msg_log.startswith("No inbound handler for 'foo'"))

        with LogCatcher() as lc:
            event = self.mkmsg_ack()
            yield self.dispatch_event(event, connector_name='foo')
            [event_log] = lc.messages()
            self.assertTrue(event_log.startswith("No event handler for 'foo'"))

        msg = self.mkmsg_out()
        yield conn.publish_outbound(msg)
        msgs = yield self.get_dispatched_outbound(connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_default_inbound_handler(self):
        conn = yield self.mk_connector(connector_name='foo', setup=True)
        with LogCatcher() as lc:
            conn.default_inbound_handler(self.mkmsg_in())
            [log] = lc.messages()
            self.assertTrue(log.startswith("No inbound handler for 'foo'"))

    @inlineCallbacks
    def test_default_event_handler(self):
        conn = yield self.mk_connector(connector_name='foo', setup=True)
        with LogCatcher() as lc:
            conn.default_event_handler(self.mkmsg_ack())
            [log] = lc.messages()
            self.assertTrue(log.startswith("No event handler for 'foo'"))

    @inlineCallbacks
    def test_set_inbound_handler(self):
        msgs = []
        conn = yield self.mk_connector(connector_name='foo', setup=True)
        conn.unpause()
        conn.set_inbound_handler(msgs.append)
        msg = self.mkmsg_in()
        yield self.dispatch_inbound(msg, connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_set_default_inbound_handler(self):
        msgs = []
        conn = yield self.mk_connector(connector_name='foo', setup=True)
        conn.unpause()
        conn.set_default_inbound_handler(msgs.append)
        msg = self.mkmsg_in()
        yield self.dispatch_inbound(msg, connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_set_event_handler(self):
        msgs = []
        conn = yield self.mk_connector(connector_name='foo', setup=True)
        conn.unpause()
        conn.set_event_handler(msgs.append)
        msg = self.mkmsg_ack()
        yield self.dispatch_event(msg, connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_set_default_event_handler(self):
        msgs = []
        conn = yield self.mk_connector(connector_name='foo', setup=True)
        conn.unpause()
        conn.set_default_event_handler(msgs.append)
        msg = self.mkmsg_ack()
        yield self.dispatch_event(msg, connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_publish_outbound(self):
        conn = yield self.mk_connector(connector_name='foo', setup=True)
        msg = self.mkmsg_out()
        yield conn.publish_outbound(msg)
        msgs = yield self.get_dispatched_outbound(connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_inbound_handler_ignore_message(self):
        def im_handler(msg):
            raise IgnoreMessage()

        conn = yield self.mk_connector(connector_name='foo', setup=True)
        conn.unpause()
        conn.set_default_inbound_handler(im_handler)
        msg = self.mkmsg_in()
        with LogCatcher() as lc:
            yield self.dispatch_inbound(msg, connector_name='foo')
            [log] = lc.messages()
            self.assertTrue(log.startswith(
                "Ignoring msg due to IgnoreMessage(): <Message"))


class TestReceiveOutboundConnector(BaseConnectorTestCase):

    connector_class = ReceiveOutboundConnector

    @inlineCallbacks
    def test_setup(self):
        conn = yield self.mk_connector(connector_name='foo')
        yield conn.setup()
        conn.unpause()

        with LogCatcher() as lc:
            msg = self.mkmsg_out()
            yield self.dispatch_outbound(msg, connector_name='foo')
            [log] = lc.messages()
            self.assertTrue(log.startswith("No outbound handler for 'foo'"))

        msg = self.mkmsg_in()
        yield conn.publish_inbound(msg)
        msgs = yield self.get_dispatched_inbound(connector_name='foo')
        self.assertEqual(msgs, [msg])

        msg = self.mkmsg_ack()
        yield conn.publish_event(msg)
        msgs = yield self.get_dispatched_events(connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_default_outbound_handler(self):
        conn = yield self.mk_connector(connector_name='foo', setup=True)
        with LogCatcher() as lc:
            conn.default_outbound_handler(self.mkmsg_out())
            [log] = lc.messages()
            self.assertTrue(log.startswith("No outbound handler for 'foo'"))

    @inlineCallbacks
    def test_set_outbound_handler(self):
        msgs = []
        conn = yield self.mk_connector(connector_name='foo', setup=True)
        conn.unpause()
        conn.set_outbound_handler(msgs.append)
        msg = self.mkmsg_out()
        yield self.dispatch_outbound(msg, connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_set_default_outbound_handler(self):
        msgs = []
        conn = yield self.mk_connector(connector_name='foo', setup=True)
        conn.unpause()
        conn.set_default_outbound_handler(msgs.append)
        msg = self.mkmsg_out()
        yield self.dispatch_outbound(msg, connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_publish_inbound(self):
        conn = yield self.mk_connector(connector_name='foo', setup=True)
        msg = self.mkmsg_in()
        yield conn.publish_inbound(msg)
        msgs = yield self.get_dispatched_inbound(connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_publish_event(self):
        conn = yield self.mk_connector(connector_name='foo', setup=True)
        msg = self.mkmsg_ack()
        yield conn.publish_event(msg)
        msgs = yield self.get_dispatched_events(connector_name='foo')
        self.assertEqual(msgs, [msg])

    @inlineCallbacks
    def test_outbound_handler_nack_message(self):
        def im_handler(msg):
            raise IgnoreMessage()

        conn = yield self.mk_connector(connector_name='foo', setup=True)
        conn.unpause()
        conn.set_default_outbound_handler(im_handler)
        msg = self.mkmsg_in()
        with LogCatcher() as lc:
            yield self.dispatch_outbound(msg, connector_name='foo')
            [log] = lc.messages()
            self.assertTrue(log.startswith(
                "Ignoring msg (with NACK) due to IgnoreMessage(): <Message"))
        [event] = yield self.get_dispatched_events(connector_name='foo')
        self.assertEqual(event['event_type'], 'nack')
