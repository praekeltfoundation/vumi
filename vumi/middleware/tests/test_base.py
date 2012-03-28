
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial.unittest import TestCase

from vumi.middleware.base import (BaseMiddleware, MiddlewareStack,
                                  create_middlewares_from_config,
                                  setup_middlewares_from_config)


class ToyMiddleware(BaseMiddleware):

    # simple attribute to check that setup_middleware is called
    _setup_done = False

    def _handle(self, direction, message, endpoint):
        message = '%s.%s' % (message, self.name)
        self.worker.processed(self.name, direction, message, endpoint)
        return message

    def setup_middleware(self):
        self._setup_done = True

    def handle_inbound(self, message, endpoint):
        return self._handle('inbound', message, endpoint)

    def handle_outbound(self, message, endpoint):
        return self._handle('outbound', message, endpoint)

    def handle_event(self, message, endpoint):
        return self._handle('event', message, endpoint)

    def handle_failure(self, message, endpoint):
        return self._handle('failure', message, endpoint)


class MiddlewareStackTestCase(TestCase):

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

    def processed(self, name, direction, message, endpoint):
        self.processed_messages.append((name, direction, message, endpoint))

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


class UtilityFunctionsTestCase(TestCase):

    TEST_CONFIG_1 = {
        "middleware": [
            {"name": "mw1",
             "cls": "vumi.middleware.tests.test_base.ToyMiddleware"},
            {"name": "mw2",
             "cls": "vumi.middleware.tests.test_base.ToyMiddleware"},
            ],
        "mw1": {
            "param_foo": 1,
            "param_bar": 2,
            }
        }

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
