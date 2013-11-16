from twisted.internet.defer import inlineCallbacks

from vumi.application.tests.utils import ApplicationTestCase
from vumi.demos.static_reply import StaticReplyApplication


class TestStaticReplyApplication(ApplicationTestCase):
    application_class = StaticReplyApplication

    @inlineCallbacks
    def test_receive_message(self):
        yield self.get_application(config={
            'reply_text': 'Your message is important to us.',
        })
        yield self.dispatch(self.mkmsg_in())
        [reply] = yield self.get_dispatched_messages()
        self.assertEqual('Your message is important to us.', reply['content'])
        self.assertEqual(u'close', reply['session_event'])

    @inlineCallbacks
    def test_receive_message_no_reply(self):
        yield self.get_application(config={})
        yield self.dispatch(self.mkmsg_in())
        self.assertEqual([], (yield self.get_dispatched_messages()))
