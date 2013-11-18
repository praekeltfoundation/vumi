from twisted.internet.defer import inlineCallbacks

from vumi.application.tests.helpers import ApplicationHelper
from vumi.demos.static_reply import StaticReplyApplication
from vumi.tests.helpers import VumiTestCase


class TestStaticReplyApplication(VumiTestCase):
    def setUp(self):
        self.app_helper = ApplicationHelper(StaticReplyApplication)
        self.add_cleanup(self.app_helper.cleanup)

    @inlineCallbacks
    def test_receive_message(self):
        yield self.app_helper.get_application({
            'reply_text': 'Your message is important to us.',
        })
        yield self.app_helper.make_dispatch_inbound(
            "Hello", from_addr='from_addr')
        [reply] = self.app_helper.get_dispatched_outbound()
        self.assertEqual('Your message is important to us. from_addr',
                         reply['content'])
        self.assertEqual(u'close', reply['session_event'])
