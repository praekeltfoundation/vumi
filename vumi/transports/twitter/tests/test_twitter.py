from twisted.internet.defer import inlineCallbacks, succeed
from txtwitter.twitter import TwitterClient

from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.transports.twitter import TwitterTransport
from vumi.transports.tests.helpers import TransportHelper


class FakeTwitterStreamService(object):
    def __init__(self, delegate):
        self.delegate = delegate

    def startService(self):
        pass

    def stopService(self):
        pass

    def respond_with(self, data):
        self.delegate(data)


class FakeTwitterClient(TwitterClient):
    def __init__(self, *a, **kw):
        super(FakeTwitterClient, self).__init__(*a, **kw)
        self.updates = []
        self.update_response = None
        self.stream_filters = []

    def set_update_response(self, resp):
        self.update_response = resp

    def update(self, content):
        self.updates.append(content)
        return succeed(self.update_response)

    def stream_filter(self, delegate, track=None):
        return FakeTwitterStreamService(delegate)


class StubbedTwitterTransport(TwitterTransport):
    CLIENT_CLS = FakeTwitterClient


class TestTwitterTransport(VumiTestCase):
    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(
            TransportHelper(StubbedTwitterTransport))

        self.transport = yield self.tx_helper.get_transport({
            'consumer_key': 'consumer1',
            'consumer_secret': 'consumersecret1',
            'access_token': 'token1',
            'access_token_secret': 'tokensecret1',
            'terms': ['some', 'trending', 'topic'],
        })

    @inlineCallbacks
    def test_handle_track(self):
        self.transport.track_stream.respond_with({
            'id_str': 1,
            'text': 'text',
            'in_reply_to_screen_name': '@reply_to',
            'user': {'screen_name': '@screen_name'},
        })

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], '@screen_name')
        self.assertEqual(msg['to_addr'], '@reply_to')
        self.assertEqual(msg['content'], 'text')
        self.assertEqual(msg['message_id'], '1')
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NONE)
