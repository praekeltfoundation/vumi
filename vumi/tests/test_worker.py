from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.worker import BaseConfig, BaseWorker
from vumi.connectors import ReceiveInboundConnector, ReceiveOutboundConnector
from vumi.tests.utils import VumiWorkerTestCase, LogCatcher, get_stubbed_worker
from vumi.message import TransportUserMessage
from vumi.middleware.tests.utils import RecordingMiddleware


class DummyWorker(BaseWorker):
    def setup_connectors(self):
        pass

    def setup_worker(self):
        pass

    def teardown_worker(self):
        pass


class TestBaseConfig(TestCase):
    def test_no_amqp_prefetch(self):
        config = BaseConfig({})
        self.assertEqual(config.amqp_prefetch_count, 20)

    def test_amqp_prefetch(self):
        config = BaseConfig({'amqp_prefetch_count': 10})
        self.assertEqual(config.amqp_prefetch_count, 10)


class TestBaseWorker(VumiWorkerTestCase):

    @inlineCallbacks
    def setUp(self):
        yield super(TestBaseWorker, self).setUp()
        self.worker = yield self.get_worker({}, DummyWorker)

    # TODO: complete tests

    def test_start_worker(self):
        pass

    def test_stop_worker(self):
        pass

    def test_setup_connectors_raises(self):
        worker = get_stubbed_worker(BaseWorker, {}, None)  # None -> dummy AMQP
        self.assertRaises(NotImplementedError, worker.setup_connectors)

    def test_teardown_connectors(self):
        pass

    def test_setup_worker_raises(self):
        worker = get_stubbed_worker(BaseWorker, {}, None)  # None -> dummy AMQP
        self.assertRaises(NotImplementedError, worker.setup_worker)

    def test_teardown_worker_raises(self):
        worker = get_stubbed_worker(BaseWorker, {}, None)  # None -> dummy AMQP
        self.assertRaises(NotImplementedError, worker.teardown_worker)

    def test_setup_middleware(self):
        pass

    def test_teardown_middleware(self):
        pass

    def test_get_static_config(self):
        pass

    def test_get_config(self):
        pass

    def test__validate_config(self):
        pass

    def test_validate_config(self):
        pass

    def test_setup_connector(self):
        pass

    def test_teardown_connector(self):
        pass

    @inlineCallbacks
    def test_setup_ri_connector(self):
        connector = yield self.worker.setup_ri_connector('foo')
        self.assertTrue(isinstance(connector, ReceiveInboundConnector))
        self.assertEqual(connector.name, 'foo')

    @inlineCallbacks
    def test_setup_ro_connector(self):
        connector = yield self.worker.setup_ro_connector('foo')
        self.assertTrue(isinstance(connector, ReceiveOutboundConnector))
        self.assertEqual(connector.name, 'foo')

    @inlineCallbacks
    def test_pause_connectors(self):
        connector = yield self.worker.setup_ri_connector('foo')
        connector.unpause()
        self.worker.pause_connectors()
        self.assertTrue(connector.paused)

    @inlineCallbacks
    def test_unpause_connectors(self):
        connector = yield self.worker.setup_ri_connector('foo')
        connector.pause()
        self.worker.unpause_connectors()
        self.assertFalse(connector.paused)
