from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.connectors import (
    BaseConnector, ReceiveInboundConnector, ReceiveOutboundConnector)
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
                     prefetch_count=None, middlewares=None):
        if worker is None:
            worker = yield self.get_worker({}, DummyWorker)
        if connector_name is None:
            connector_name = "dummy_connector"
        connector = self.connector_class(worker, connector_name,
                                         prefetch_count=prefetch_count,
                                         middlewares=middlewares)
        returnValue(connector)

    @inlineCallbacks
    def mk_consumer(self, *args, **kwargs):
        conn = yield self.mk_connector(*args, **kwargs)
        consumer = yield conn._setup_consumer('inbound', TransportUserMessage)
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
    def test_no_inbound_handler(self):
        conn = yield self.mk_connector(connector_name='foo')
        yield conn.setup()
        conn.unpause()
        with LogCatcher() as lc:
            yield self.dispatch_inbound(self.mkmsg_in(), connector_name='foo')
            [log] = lc.logs
        self.assertTrue(log['message'][0].startswith(
            "No inbound handler for 'foo': "))

    @inlineCallbacks
    def test_no_event_handler(self):
        conn = yield self.mk_connector(connector_name='foo')
        yield conn.setup()
        conn.unpause()
        with LogCatcher() as lc:
            yield self.dispatch_event(self.mkmsg_ack(), connector_name='foo')
            [log] = lc.logs
        self.assertTrue(log['message'][0].startswith(
            "No event handler for 'foo': "))


class TestReceiveOutboundConnector(BaseConnectorTestCase):
    connector_class = ReceiveOutboundConnector

    @inlineCallbacks
    def test_no_outbound_handler(self):
        conn = yield self.mk_connector(connector_name='foo')
        yield conn.setup()
        conn.unpause()
        with LogCatcher() as lc:
            yield self.dispatch_outbound(
                self.mkmsg_out(), connector_name='foo')
            [log] = lc.logs
        self.assertTrue(log['message'][0].startswith(
            "No outbound handler for 'foo': "))
