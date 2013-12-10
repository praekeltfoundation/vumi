from datetime import date

from twisted.internet.defer import inlineCallbacks

from vumi.application.tests.helpers import ApplicationHelper
from vumi.demos.static_reply import StaticReplyApplication
from vumi.tests.helpers import VumiTestCase


class TestStaticReplyApplication(VumiTestCase):
    def setUp(self):
        self.app_helper = self.add_helper(
            ApplicationHelper(StaticReplyApplication))

    @inlineCallbacks
    def test_receive_message_custom(self):
        yield self.app_helper.get_application({
            'reply_text': 'Hi {user}',
        })
        yield self.app_helper.make_dispatch_inbound(
            "Hello", from_addr='from_addr')
        [reply] = self.app_helper.get_dispatched_outbound()
        self.assertEqual('Hi from_addr', reply['content'])
        self.assertEqual(u'close', reply['session_event'])

    @inlineCallbacks
    def test_receive_message(self):
        yield self.app_helper.get_application({})
        yield self.app_helper.make_dispatch_inbound(
            "Hello", from_addr='from_addr')
        [reply] = self.app_helper.get_dispatched_outbound()
        self.assertEqual('Hello from_addr at %s.' % (date.today(),),
                         reply['content'])
        self.assertEqual(u'close', reply['session_event'])
