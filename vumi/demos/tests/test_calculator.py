from twisted.internet.defer import inlineCallbacks

from vumi.tests.helpers import VumiTestCase
from vumi.application.tests.helpers import ApplicationHelper
from vumi.demos.calculator import CalculatorApp
from vumi.message import TransportUserMessage


class TestCalculatorApp(VumiTestCase):

    timeout = 1

    @inlineCallbacks
    def setUp(self):
        self.app_helper = ApplicationHelper(CalculatorApp)
        self.add_cleanup(self.app_helper.cleanup)
        self.worker = yield self.app_helper.get_application({})

    @inlineCallbacks
    def test_session_start(self):
        msg = yield self.app_helper.make_dispatch_inbound(
            None, session_event=TransportUserMessage.SESSION_NEW)
        [resp] = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertTrue(
            resp['content'].startswith('What would you like to do?'))

    @inlineCallbacks
    def test_first_number(self):
        msg = yield self.app_helper.make_dispatch_inbound(
            '1', session_event=TransportUserMessage.SESSION_RESUME)
        [resp] = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertEqual(resp['content'], 'What is the first number?')

    @inlineCallbacks
    def test_second_number(self):
        self.worker.save_session('+41791234567', {
            'action': 1,
        })
        msg = yield self.app_helper.make_dispatch_inbound(
            '1', session_event=TransportUserMessage.SESSION_RESUME)
        [resp] = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertEqual(resp['content'], 'What is the second number?')

    @inlineCallbacks
    def test_action(self):
        self.worker.save_session('+41791234567', {
            'action': 0,  # add
            'first_number': 2,
        })
        msg = yield self.app_helper.make_dispatch_inbound(
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
        msg = yield self.app_helper.make_dispatch_inbound(
            'not-an-int', session_event=TransportUserMessage.SESSION_RESUME)
        [resp] = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertEqual(resp['content'], 'Sorry invalid input!')
        self.assertEqual(resp['session_event'],
                         TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def test_invalid_action(self):
        msg = yield self.app_helper.make_dispatch_inbound(
            'not-an-option', session_event=TransportUserMessage.SESSION_RESUME)
        [resp] = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertTrue(
            resp['content'].startswith('Sorry invalid input!'))
