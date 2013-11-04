import yaml
import itertools

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.middleware.base import (BaseMiddleware, MiddlewareStack,
                                  create_middlewares_from_config,
                                  setup_middlewares_from_config)
from vumi.tests.helpers import VumiTestCase


class ToyMiddleware(BaseMiddleware):

    # simple attribute to check that setup_middleware is called
    _setup_done = False
    _teardown_count = itertools.count(1)

    def _handle(self, direction, message, connector_name):
        message = '%s.%s' % (message, self.name)
        self.worker.processed(self.name, direction, message, connector_name)
        return message

    def setup_middleware(self):
        self._setup_done = True
        self._teardown_done = False

    def teardown_middleware(self):
        self._teardown_done = next(self._teardown_count)

    def handle_inbound(self, message, connector_name):
        return self._handle('inbound', message, connector_name)

    def handle_outbound(self, message, connector_name):
        return self._handle('outbound', message, connector_name)

    def handle_event(self, message, connector_name):
        return self._handle('event', message, connector_name)

    def handle_failure(self, message, connector_name):
        return self._handle('failure', message, connector_name)


class MiddlewareStackTestCase(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.stack = MiddlewareStack([
                (yield self.mkmiddleware('mw1')),
                (yield self.mkmiddleware('mw2')),
                (yield self.mkmiddleware('mw3')),
                ])
        self.processed_messages = []

    @inlineCallbacks
    def mkmiddleware(self, name):
        mw = ToyMiddleware(name, {}, self)
        yield mw.setup_middleware()
        returnValue(mw)

    def processed(self, name, direction, message, connector_name):
        self.processed_messages.append(
            (name, direction, message, connector_name))

    def assert_processed(self, expected):
        self.assertEqual(expected, self.processed_messages)

    @inlineCallbacks
    def test_apply_consume(self):
        self.assert_processed([])
        yield self.stack.apply_consume('inbound', 'dummy_msg', 'end_foo')
        self.assert_processed([
                ('mw1', 'inbound', 'dummy_msg.mw1', 'end_foo'),
                ('mw2', 'inbound', 'dummy_msg.mw1.mw2', 'end_foo'),
                ('mw3', 'inbound', 'dummy_msg.mw1.mw2.mw3', 'end_foo'),
                ])

    @inlineCallbacks
    def test_apply_publish(self):
        self.assert_processed([])
        yield self.stack.apply_publish('inbound', 'dummy_msg', 'end_foo')
        self.assert_processed([
                ('mw3', 'inbound', 'dummy_msg.mw3', 'end_foo'),
                ('mw2', 'inbound', 'dummy_msg.mw3.mw2', 'end_foo'),
                ('mw1', 'inbound', 'dummy_msg.mw3.mw2.mw1', 'end_foo'),
                ])

    @inlineCallbacks
    def test_teardown_in_reverse_order(self):

        def get_teardown_timestamps():
            return [mw._teardown_done for mw in self.stack.middlewares]

        self.assertFalse(any(get_teardown_timestamps()))
        yield self.stack.teardown()
        self.assertTrue(all(get_teardown_timestamps()))
        teardown_order = sorted(self.stack.middlewares,
            key=lambda mw: mw._teardown_done)
        self.assertEqual([mw.name for mw in teardown_order],
            ['mw3', 'mw2', 'mw1'])


class UtilityFunctionsTestCase(VumiTestCase):

    TEST_CONFIG_1 = {
        "middleware": [
            {"mw1": "vumi.middleware.tests.test_base.ToyMiddleware"},
            {"mw2": "vumi.middleware.tests.test_base.ToyMiddleware"},
            ],
        "mw1": {
            "param_foo": 1,
            "param_bar": 2,
            }
        }

    TEST_YAML = """
        middleware:
          - mw1: vumi.middleware.tests.test_base.ToyMiddleware
          - mw2: vumi.middleware.tests.test_base.ToyMiddleware
        """

    def test_create_middleware_from_config(self):
        worker = object()
        middlewares = create_middlewares_from_config(worker,
                                                     self.TEST_CONFIG_1)
        self.assertEqual([type(mw) for mw in middlewares],
                         [ToyMiddleware, ToyMiddleware])
        self.assertEqual([mw._setup_done for mw in middlewares],
                         [False, False])
        self.assertEqual(middlewares[0].config,
                         {"param_foo": 1, "param_bar": 2})
        self.assertEqual(middlewares[1].config, {})

    def test_setup_middleware_from_config(self):
        worker = object()
        middlewares = yield setup_middlewares_from_config(worker,
                                                          self.TEST_CONFIG_1)
        self.assertEqual([type(mw) for mw in middlewares],
                         [ToyMiddleware, ToyMiddleware])
        self.assertEqual([mw._setup_done for mw in middlewares],
                         [True, True])
        self.assertEqual(middlewares[0].config,
                         {"param_foo": 1, "param_bar": 2})
        self.assertEqual(middlewares[1].config, {})

    def test_parse_yaml(self):
        # this test is here to ensure the YAML one has to
        # type looks nice
        worker = object()
        config = yaml.safe_load(self.TEST_YAML)
        middlewares = create_middlewares_from_config(worker, config)
        self.assertEqual([type(mw) for mw in middlewares],
                         [ToyMiddleware, ToyMiddleware])
        self.assertEqual([mw._setup_done for mw in middlewares],
                         [False, False])
