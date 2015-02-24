import yaml
import itertools

from confmodel.fields import ConfigInt
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.middleware.base import (
    BaseMiddleware, MiddlewareStack, create_middlewares_from_config,
    setup_middlewares_from_config, BaseMiddlewareConfig)
from vumi.tests.helpers import VumiTestCase


class ToyMiddlewareConfig(BaseMiddlewareConfig):
    """
    Config for the toy middleware.
    """
    param_foo = ConfigInt("Foo parameter", static=True)
    param_bar = ConfigInt("Bar parameter", static=True)


class ToyMiddleware(BaseMiddleware):
    CONFIG_CLASS = ToyMiddlewareConfig
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


class ToyAsymmetricMiddleware(ToyMiddleware):

    def _handle(self, direction, message, connector_name):
        message = '%s.%s' % (message, self.name)
        self.worker.processed(self.name, direction, message, connector_name)
        return message

    def handle_consume_inbound(self, message, connector_name):
        return self._handle('consume_inbound', message, connector_name)

    def handle_publish_inbound(self, message, connector_name):
        return self._handle('publish_inbound', message, connector_name)

    def handle_consume_outbound(self, message, connector_name):
        return self._handle('consume_outbound', message, connector_name)

    def handle_publish_outbound(self, message, connector_name):
        return self._handle('publish_outbound', message, connector_name)

    def handle_consume_event(self, message, connector_name):
        return self._handle('consume_event', message, connector_name)

    def handle_publish_event(self, message, connector_name):
        return self._handle('publish_event', message, connector_name)

    def handle_consume_failure(self, message, connector_name):
        return self._handle('consume_failure', message, connector_name)

    def handle_publish_failure(self, message, connector_name):
        return self._handle('publish_failure', message, connector_name)


class TestMiddlewareStack(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.stack = MiddlewareStack([
                (yield self.mkmiddleware('mw1', ToyMiddleware)),
                (yield self.mkmiddleware('mw2', ToyAsymmetricMiddleware)),
                (yield self.mkmiddleware('mw3', ToyMiddleware)),
                ])
        self.processed_messages = []

    @inlineCallbacks
    def mkmiddleware(self, name, mw_class):
        mw = mw_class(
            name, {'consume_priority': 0, 'publish_priority': 0}, self)
        yield mw.setup_middleware()
        returnValue(mw)

    @inlineCallbacks
    def mk_priority_middleware(self, name, mw_class, consume_pri, publish_pri):
        mw = mw_class(
            name, {
                'consume_priority': consume_pri,
                'publish_priority': publish_pri,
                }, self)
        yield mw.setup_middleware()
        returnValue(mw)

    def processed(self, name, direction, message, connector_name):
        self.processed_messages.append(
            (name, direction, message, connector_name))

    def assert_processed(self, expected):
        self.assertEqual(expected, self.processed_messages)

    @inlineCallbacks
    def test_apply_consume_inbound(self):
        self.assert_processed([])
        yield self.stack.apply_consume('inbound', 'dummy_msg', 'end_foo')
        self.assert_processed([
                ('mw1', 'inbound', 'dummy_msg.mw1', 'end_foo'),
                ('mw2', 'consume_inbound', 'dummy_msg.mw1.mw2', 'end_foo'),
                ('mw3', 'inbound', 'dummy_msg.mw1.mw2.mw3', 'end_foo'),
                ])

    @inlineCallbacks
    def test_apply_publish_inbound(self):
        self.assert_processed([])
        yield self.stack.apply_publish('inbound', 'dummy_msg', 'end_foo')
        self.assert_processed([
                ('mw3', 'inbound', 'dummy_msg.mw3', 'end_foo'),
                ('mw2', 'publish_inbound', 'dummy_msg.mw3.mw2', 'end_foo'),
                ('mw1', 'inbound', 'dummy_msg.mw3.mw2.mw1', 'end_foo'),
                ])

    @inlineCallbacks
    def test_apply_consume_outbound(self):
        self.assert_processed([])
        yield self.stack.apply_consume('outbound', 'dummy_msg', 'end_foo')
        self.assert_processed([
                ('mw1', 'outbound', 'dummy_msg.mw1', 'end_foo'),
                ('mw2', 'consume_outbound', 'dummy_msg.mw1.mw2', 'end_foo'),
                ('mw3', 'outbound', 'dummy_msg.mw1.mw2.mw3', 'end_foo'),
                ])

    @inlineCallbacks
    def test_apply_publish_outbound(self):
        self.assert_processed([])
        yield self.stack.apply_publish('outbound', 'dummy_msg', 'end_foo')
        self.assert_processed([
                ('mw3', 'outbound', 'dummy_msg.mw3', 'end_foo'),
                ('mw2', 'publish_outbound', 'dummy_msg.mw3.mw2', 'end_foo'),
                ('mw1', 'outbound', 'dummy_msg.mw3.mw2.mw1', 'end_foo'),
                ])

    @inlineCallbacks
    def test_apply_consume_event(self):
        self.assert_processed([])
        yield self.stack.apply_consume('event', 'dummy_msg', 'end_foo')
        self.assert_processed([
                ('mw1', 'event', 'dummy_msg.mw1', 'end_foo'),
                ('mw2', 'consume_event', 'dummy_msg.mw1.mw2', 'end_foo'),
                ('mw3', 'event', 'dummy_msg.mw1.mw2.mw3', 'end_foo'),
                ])

    @inlineCallbacks
    def test_apply_publish_event(self):
        self.assert_processed([])
        yield self.stack.apply_publish('event', 'dummy_msg', 'end_foo')
        self.assert_processed([
                ('mw3', 'event', 'dummy_msg.mw3', 'end_foo'),
                ('mw2', 'publish_event', 'dummy_msg.mw3.mw2', 'end_foo'),
                ('mw1', 'event', 'dummy_msg.mw3.mw2.mw1', 'end_foo'),
                ])

    @inlineCallbacks
    def test_teardown_in_reverse_order(self):

        def get_teardown_timestamps():
            return [mw._teardown_done for mw in self.stack.consume_middlewares]

        self.assertFalse(any(get_teardown_timestamps()))
        yield self.stack.teardown()
        self.assertTrue(all(get_teardown_timestamps()))
        teardown_order = sorted(self.stack.consume_middlewares,
            key=lambda mw: mw._teardown_done)
        self.assertEqual([mw.name for mw in teardown_order],
            ['mw3', 'mw2', 'mw1'])

    @inlineCallbacks
    def test_middleware_priority_ordering(self):
        self.stack = MiddlewareStack([
            (yield self.mk_priority_middleware('p2', ToyMiddleware, 3, 3)),
            (yield self.mkmiddleware('pn', ToyMiddleware)),
            (yield self.mk_priority_middleware('p1_1', ToyMiddleware, 2, 2)),
            (yield self.mk_priority_middleware('p1_2', ToyMiddleware, 2, 2)),
            (yield self.mk_priority_middleware('pasym', ToyMiddleware, 1, 4)),
            ])
        # test consume
        self.assert_processed([])
        yield self.stack.apply_consume('event', 'dummy_msg', 'end_foo')
        self.assert_processed([
            ('pn', 'event', 'dummy_msg.pn', 'end_foo'),
            ('pasym', 'event', 'dummy_msg.pn.pasym', 'end_foo'),
            ('p1_1', 'event', 'dummy_msg.pn.pasym.p1_1', 'end_foo'),
            ('p1_2', 'event', 'dummy_msg.pn.pasym.p1_1.p1_2', 'end_foo'),
            ('p2', 'event', 'dummy_msg.pn.pasym.p1_1.p1_2.p2', 'end_foo'),
        ])
        # test publish
        self.processed_messages = []
        yield self.stack.apply_publish('event', 'dummy_msg', 'end_foo')
        self.assert_processed([
            ('pn', 'event', 'dummy_msg.pn', 'end_foo'),
            ('p1_2', 'event', 'dummy_msg.pn.p1_2', 'end_foo'),
            ('p1_1', 'event', 'dummy_msg.pn.p1_2.p1_1', 'end_foo'),
            ('p2', 'event', 'dummy_msg.pn.p1_2.p1_1.p2', 'end_foo'),
            ('pasym', 'event', 'dummy_msg.pn.p1_2.p1_1.p2.pasym', 'end_foo'),
        ])


class TestUtilityFunctions(VumiTestCase):

    TEST_CONFIG_1 = {
        "middleware": [
            {"mw1": "vumi.middleware.tests.test_base.ToyMiddleware"},
            {"mw2": {
                'class': "vumi.middleware.tests.test_base.ToyMiddleware",
                'consume_priority': 1,
                'publish_priority': -1}},
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
        self.assertEqual(middlewares[0].config.param_foo, 1)
        self.assertEqual(middlewares[0].config.param_bar, 2)
        self.assertEqual(middlewares[0].consume_priority, 0)
        self.assertEqual(middlewares[0].publish_priority, 0)
        self.assertEqual(middlewares[1].consume_priority, 1)
        self.assertEqual(middlewares[1].publish_priority, -1)

    @inlineCallbacks
    def test_setup_middleware_from_config(self):
        worker = object()
        middlewares = yield setup_middlewares_from_config(worker,
                                                          self.TEST_CONFIG_1)
        self.assertEqual([type(mw) for mw in middlewares],
                         [ToyMiddleware, ToyMiddleware])
        self.assertEqual([mw._setup_done for mw in middlewares],
                         [True, True])
        self.assertEqual(middlewares[0].config.param_foo, 1)
        self.assertEqual(middlewares[0].config.param_bar, 2)
        self.assertEqual(middlewares[0].consume_priority, 0)
        self.assertEqual(middlewares[0].publish_priority, 0)
        self.assertEqual(middlewares[1].consume_priority, 1)
        self.assertEqual(middlewares[1].publish_priority, -1)

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

    @inlineCallbacks
    def test_sort_by_priority(self):
        priority2 = ToyMiddleware('priority2', {}, self)
        priority2.priority = 2
        priority1_1 = ToyMiddleware('priority1_1', {}, self)
        priority1_1.priority = 1
        priority1_2 = ToyMiddleware('priority1_2', {}, self)
        priority1_2.priority = 1
        middlewares = [priority2, priority1_1, priority1_2]
        for mw in middlewares:
            yield mw.setup_middleware()
        mw_sorted = MiddlewareStack._sort_by_priority(middlewares, 'priority')
        self.assertEqual(
            mw_sorted, [priority1_1, priority1_2, priority2])
