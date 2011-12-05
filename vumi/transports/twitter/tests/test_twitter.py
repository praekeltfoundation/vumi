from twisted.internet.defer import inlineCallbacks
from vumi.tests.utils import FakeRedis
from vumi.transports.twitter import TwitterTransport
from vumi.transports.tests.test_base import TransportTestCase
from vumi.message import TransportUserMessage


class Thing(object):
    """This is what ruby taught me"""
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __getattr__(self, attr):
        return self.kwargs[attr]


class FakeTwitter(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def track(self, delegate, terms):
        self.track_delegate = delegate
        self.track_terms = terms

    def replies(self, delegate, params={}, extra_args=None):
        self.replies_delegate = delegate
        self.replies_params = params
        self.replies_extra_args = extra_args

    def send_fake_replies(self, *replies):
        for reply in replies:
            self.replies_delegate(reply)

    def send_fake_track(self, *tracks):
        for status in tracks:
            self.track_delegate(status)


class TwitterTransportTestCase(TransportTestCase):

    transport_name = 'test_twitter'
    transport_class = TwitterTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TwitterTransportTestCase, self).setUp()
        self.config = {
            'app_name': 'testapp',
            'consumer_key': 'consumer1',
            'consumer_secret': 'consumersecret1',
            'access_token': 'token1',
            'access_token_secret': 'tokensecret1',
            'terms': ['some', 'trending', 'topic'],
        }
        self.transport = yield self.get_transport(self.config, start=False)
        self.transport.validate_config()
        self.transport.r_server = FakeRedis()
        self.transport.transport_name = self.transport_name
        self.transport.concurrent_sends = None
        self.transport._consumers = []
        self.transport.twitter = FakeTwitter()
        yield self.transport.start_tracking_terms()
        self.transport.start_checking_for_replies()

        yield self.transport._setup_failure_publisher()
        yield self.transport._setup_message_publisher()
        yield self.transport._setup_event_publisher()

    def test_handle_replies(self):
        reply = Thing(
            id=1,
            published=1,
            text='@tweeter hi there',
            title='tweeter',
            author=Thing(screen_name='replier'),
            raw={
                'some': 'raw',
                'fields': 'and values',
            }
        )
        self.transport.twitter.send_fake_replies(reply)
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['from_addr'], 'replier')
        self.assertEqual(msg['to_addr'], 'tweeter')
        self.assertEqual(msg['content'], '@tweeter hi there')
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)
        self.assertEqual(msg['message_id'], 1)
        last_reply_timestamp = self.transport.r_server.get(
            '%s:last_reply_timestamp' % self.transport.r_prefix)
        self.assertEqual(last_reply_timestamp, '1')

    def test_handle_track(self):
        status = Thing(
            id=1,
            text='text',
            in_reply_to_screen_name='@reply_to',
            user=Thing(screen_name='@screen_name'),
            raw={
                'some': 'raw',
                'fields': 'and values',
            }
        )
        self.transport.twitter.send_fake_track(status)
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['from_addr'], '@screen_name')
        self.assertEqual(msg['to_addr'], '@reply_to')
        self.assertEqual(msg['content'], 'text')
        self.assertEqual(msg['message_id'], 1)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NONE)
