from twisted.internet.defer import inlineCallbacks

from vumi.tests.helpers import VumiTestCase
from vumi.application.tests.helpers import ApplicationHelper
from vumi.demos.calculator import CalculatorApp
from vumi.message import TransportUserMessage


class TestCalculatorApp(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.app_helper = self.add_helper(ApplicationHelper(CalculatorApp))
        self.worker = yield self.app_helper.get_application({})

    @inlineCallbacks
    def test_session_start(self):
        yield self.app_helper.make_dispatch_inbound(
            None, session_event=TransportUserMessage.SESSION_NEW)
        [resp] = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertEqual(
            resp['content'],
            'What would you like to do?\n'
            '1. Add\n'
            '2. Subtract\n'
            '3. Multiply')

    @inlineCallbacks
    def test_first_number(self):
        yield self.app_helper.make_dispatch_inbound(
            '1', session_event=TransportUserMessage.SESSION_RESUME)
        [resp] = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertEqual(resp['content'], 'What is the first number?')

    @inlineCallbacks
    def test_second_number(self):
        self.worker.save_session('+41791234567', {
            'action': 1,
        })
        yield self.app_helper.make_dispatch_inbound(
            '1', session_event=TransportUserMessage.SESSION_RESUME)
        [resp] = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertEqual(resp['content'], 'What is the second number?')

    @inlineCallbacks
    def test_action(self):
        self.worker.save_session('+41791234567', {
            'action': 0,  # add
            'first_number': 2,
        })
        yield self.app_helper.make_dispatch_inbound(
            '2', session_event=TransportUserMessage.SESSION_RESUME)
        [resp] = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertEqual(resp['content'], 'The result is: 4.')
        self.assertEqual(resp['session_event'],
                         TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def test_invalid_input(self):
        self.worker.save_session('+41791234567', {
            'action': 0,  # add
        })
        yield self.app_helper.make_dispatch_inbound(
            'not-an-int', session_event=TransportUserMessage.SESSION_RESUME)
        [resp] = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertEqual(resp['content'], 'Sorry invalid input!')
        self.assertEqual(resp['session_event'],
                         TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def test_invalid_action(self):
        yield self.app_helper.make_dispatch_inbound(
            'not-an-option', session_event=TransportUserMessage.SESSION_RESUME)
        [resp] = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertTrue(
            resp['content'].startswith('Sorry invalid input!'))

    @inlineCallbacks
    def test_user_cancellation(self):
        self.worker.save_session('+41791234567', {'foo': 'bar'})
        yield self.app_helper.make_dispatch_inbound(
            None, session_event=TransportUserMessage.SESSION_CLOSE)
        self.assertEqual(self.worker.get_session('+41791234567'), {})

    @inlineCallbacks
    def test_none_input_on_session_resume(self):
        yield self.app_helper.make_dispatch_inbound(
            None, session_event=TransportUserMessage.SESSION_RESUME)
        [resp] = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertEqual(resp['content'], 'Sorry invalid input!')
