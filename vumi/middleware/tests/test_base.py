
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial.unittest import TestCase

from vumi.middleware.base import BaseMiddleware, MiddlewareStack


class ToyMiddleware(BaseMiddleware):
    def setup_middleware(self):
        self.name = self.config['name']

    def _handle(self, direction, message, endpoint_name):
        message = '%s.%s' % (message, self.name)
        self.worker.processed(self.name, direction, message, endpoint_name)
        return message

    def handle_inbound(self, message, endpoint_name):
        return self._handle('inbound', message, endpoint_name)

    def handle_outbound(self, message, endpoint_name):
        return self._handle('outbound', message, endpoint_name)

    def handle_event(self, message, endpoint_name):
        return self._handle('event', message, endpoint_name)

    def handle_failure(self, message, endpoint_name):
        return self._handle('failure', message, endpoint_name)


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
        mw = ToyMiddleware(self, {'name': name})
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
