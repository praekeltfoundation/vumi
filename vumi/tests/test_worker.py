from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, succeed

from vumi.worker import BaseConfig, BaseWorker
from vumi.connectors import ReceiveInboundConnector, ReceiveOutboundConnector
from vumi.tests.utils import VumiWorkerTestCase, LogCatcher, get_stubbed_worker
from vumi.middleware.base import BaseMiddleware


class DummyWorker(BaseWorker):
    def setup_connectors(self):
        pass

    def setup_worker(self):
        pass

    def teardown_worker(self):
        pass


class DummyMiddleware(BaseMiddleware):
    setup_called = False
    teardown_called = False

    def setup_middleware(self):
        self.setup_called = True
        return succeed(None)

    def teardown_middleware(self):
        self.teardown_called = True
        return succeed(None)


class CallRecorder(object):
    def __init__(self, func, calls=None):
        self.func = func
        self.calls = calls if calls is not None else []

    def __call__(self, *args, **kwargs):
        self.calls.append((self.func.__name__, args, kwargs))
        return self.func(*args, **kwargs)


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
        self.worker = yield self.get_worker({}, DummyWorker, False)

    @inlineCallbacks
    def test_start_worker(self):
        worker, calls = self.worker, []
        worker.setup_heartbeat = CallRecorder(worker.setup_heartbeat, calls)
        worker.setup_middleware = CallRecorder(worker.setup_middleware, calls)
        worker.setup_connectors = CallRecorder(worker.setup_connectors, calls)
        worker.setup_worker = CallRecorder(worker.setup_worker, calls)
        with LogCatcher() as lc:
            yield worker.startWorker()
            self.assertEqual(lc.messages(),
                             ['Starting a DummyWorker worker with config: '
                              "{'worker_name': 'unnamed'}",
                              'Starting HeartBeat publisher with '
                              'worker_name=unnamed',
                              'Started the publisher'])
        self.assertEqual(calls, [
            ('setup_heartbeat', (), {}),
            ('setup_middleware', (), {}),
            ('setup_connectors', (), {}),
            ('setup_worker', (), {}),
        ])

    @inlineCallbacks
    def test_stop_worker(self):
        worker, calls = self.worker, []
        worker.teardown_heartbeat = CallRecorder(worker.teardown_heartbeat,
                                                 calls)
        worker.teardown_middleware = CallRecorder(worker.teardown_middleware,
                                                  calls)
        worker.teardown_connectors = CallRecorder(worker.teardown_connectors,
                                                  calls)
        worker.teardown_worker = CallRecorder(worker.teardown_worker, calls)
        yield worker.startWorker()
        with LogCatcher() as lc:
            yield worker.stopWorker()
            self.assertEqual(lc.messages(), ['Stopping a DummyWorker worker.'])
        self.assertEqual(calls, [
            ('teardown_worker', (), {}),
            ('teardown_connectors', (), {}),
            ('teardown_middleware', (), {}),
            ('teardown_heartbeat', (), {}),
        ])

    def test_setup_connectors_raises(self):
        worker = get_stubbed_worker(BaseWorker, {}, None)  # None -> dummy AMQP
        self.assertRaises(NotImplementedError, worker.setup_connectors)

    @inlineCallbacks
    def test_teardown_connectors(self):
        connector = yield self.worker.setup_ri_connector('foo')
        yield self.worker.teardown_connectors()
        self.assertTrue('foo' not in self.worker.connectors)
        self.assertFalse(connector._consumers['inbound'].keep_consuming)

    def test_setup_worker_raises(self):
        worker = get_stubbed_worker(BaseWorker, {}, None)  # None -> dummy AMQP
        self.assertRaises(NotImplementedError, worker.setup_worker)

    def test_teardown_worker_raises(self):
        worker = get_stubbed_worker(BaseWorker, {}, None)  # None -> dummy AMQP
        self.assertRaises(NotImplementedError, worker.teardown_worker)

    @inlineCallbacks
    def test_setup_middleware(self):
        worker = get_stubbed_worker(DummyWorker, {
            'middleware': [{'mw': 'vumi.tests.test_worker'
                                  '.DummyMiddleware'}],
        })
        yield worker.setup_middleware()
        self.assertEqual([mw.name for mw in worker.middlewares], ['mw'])
        self.assertTrue(worker.middlewares[0].setup_called)

    @inlineCallbacks
    def test_teardown_middleware(self):
        worker = get_stubbed_worker(DummyWorker, {
            'middleware': [{'mw': 'vumi.tests.test_worker'
                                  '.DummyMiddleware'}],
        })
        yield worker.setup_middleware()
        yield worker.teardown_middleware()
        self.assertTrue(worker.middlewares[0].teardown_called)

    def test_get_static_config(self):
        cfg = self.worker.get_static_config()
        self.assertEqual([f.name for f in cfg.fields], ['amqp_prefetch_count'])
        self.assertEqual(cfg.amqp_prefetch_count, 20)

    @inlineCallbacks
    def test_get_config(self):
        msg = self.mkmsg_in()
        cfg = yield self.worker.get_config(msg)
        self.assertEqual([f.name for f in cfg.fields], ['amqp_prefetch_count'])
        self.assertEqual(cfg.amqp_prefetch_count, 20)

    def test__validate_config(self):
        # should call .validate_config()
        self.worker.validate_config = CallRecorder(self.worker.validate_config)
        self.worker._validate_config()
        self.assertEqual(self.worker.validate_config.calls, [
            ('validate_config', (), {})
        ])

    def test_validate_config(self):
        # should just be callable and not raise
        self.worker.validate_config()

    @inlineCallbacks
    def test_setup_connector(self):
        connector = yield self.worker.setup_connector(ReceiveInboundConnector,
                                                      'foo')
        self.assertTrue('foo' in self.worker.connectors)
        self.assertTrue(isinstance(connector, ReceiveInboundConnector))
        # test setup happened
        self.assertTrue(connector._consumers['inbound'].keep_consuming)

    @inlineCallbacks
    def test_teardown_connector(self):
        connector = yield self.worker.setup_connector(ReceiveInboundConnector,
                                                      'foo')
        yield self.worker.teardown_connector('foo')
        self.assertFalse('foo' in self.worker.connectors)
        # test teardown happened
        self.assertFalse(connector._consumers['inbound'].keep_consuming)

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
