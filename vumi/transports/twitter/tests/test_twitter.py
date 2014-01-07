from twisted.internet.defer import inlineCallbacks, succeed
from txtwitter.twitter import TwitterClient

from vumi.tests.utils import LogCatcher
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
    timeout = 1

    def __init__(self, *a, **kw):
        super(FakeTwitterClient, self).__init__(*a, **kw)
        self.stream_filters = []
        self.status_updates = []
        self.status_update_response = None
        self.status_update_error = None

    def set_status_update_response(self, resp):
        self.status_update_response = resp

    def set_status_update_to_fail(self, e):
        self.status_update_error = e

    def get_status_updates(self):
        return self.status_updates

    def statuses_update(self, content):
        if self.status_update_error is not None:
            raise self.status_update_error

        self.status_updates.append(content)
        return succeed(self.status_update_response)

    def stream_filter(self, delegate, track=None):
        return FakeTwitterStreamService(delegate)

    def userstream_user(self, delegate, with_='followings'):
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
    def test_sending(self):
        self.transport.client.set_status_update_response({'id_str': '1'})

        msg = yield self.tx_helper.make_dispatch_outbound('adnap das')
        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(
            self.transport.client.get_status_updates(),
            ['adnap das'])

        self.assertEqual(ack['user_message_id'], msg['message_id'])
        self.assertEqual(ack['sent_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_sending_failure(self):
        error = Exception(':(')
        self.transport.client.set_status_update_to_fail(error)

        msg = yield self.tx_helper.make_dispatch_outbound('adnap das')
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(self.transport.client.get_status_updates(), [])
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'], '%r' % (error,))

    @inlineCallbacks
    def test_tracking_messages(self):
        self.transport.track_stream.respond_with({
            'id_str': '1',
            'text': 'hello',
            'in_reply_to_status_id_str': None,
            'in_reply_to_screen_name': None,
            'user': {'screen_name': 'someone'},
            'entities': {
                'user_mentions': [{'screen_name': 'someone_else'}]
            }
        })

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], 'someone')
        self.assertEqual(msg['to_addr'], '')
        self.assertEqual(msg['content'], 'hello')
        self.assertEqual(msg['transport_metadata'], {'id': '1'})

        self.assertEqual(msg['helper_metadata'], {
            'in_reply_to_status_id': None,
            'in_reply_to_screen_name': None,
            'user_mentions': [{'screen_name': u'someone_else'}]})

    @inlineCallbacks
    def test_tracking_reply_messages(self):
        self.transport.track_stream.respond_with({
            'id_str': '2',
            'text': 'hello',
            'in_reply_to_status_id_str': '1',
            'in_reply_to_screen_name': 'someone_else',
            'user': {'screen_name': 'someone'},
            'entities': {
                'user_mentions': [{'screen_name': 'someone_else'}]
            }
        })

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], 'someone')
        self.assertEqual(msg['to_addr'], 'someone_else')
        self.assertEqual(msg['content'], 'hello')
        self.assertEqual(msg['transport_metadata'], {'id': '2'})

        self.assertEqual(msg['helper_metadata'], {
            'in_reply_to_status_id': '1',
            'in_reply_to_screen_name': 'someone_else',
            'user_mentions': [{'screen_name': u'someone_else'}]})

    @inlineCallbacks
    def test_inbound_user_message(self):
        self.transport.user_stream.respond_with({
            'id_str': '1',
            'text': 'hello',
            'in_reply_to_screen_name': None,
            'in_reply_to_status_id_str': None,
            'user': {'screen_name': 'someone'},
            'entities': {
                'user_mentions': [{'screen_name': 'me'}]
            }
        })

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], 'someone')
        self.assertEqual(msg['to_addr'], '')
        self.assertEqual(msg['content'], 'hello')
        self.assertEqual(msg['helper_metadata'], {
            'in_reply_to_status_id': None,
            'in_reply_to_screen_name': None,
            'user_mentions': [{'screen_name': 'me'}]})

    @inlineCallbacks
    def test_inbound_user_reply_message(self):
        self.transport.user_stream.respond_with({
            'id_str': '2',
            'text': 'hello',
            'in_reply_to_status_id_str': '1',
            'in_reply_to_screen_name': 'me',
            'user': {'screen_name': 'someone'},
            'entities': {
                'user_mentions': [{'screen_name': 'me'}]
            }
        })

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], 'someone')
        self.assertEqual(msg['to_addr'], 'me')
        self.assertEqual(msg['content'], 'hello')
        self.assertEqual(msg['helper_metadata'], {
            'in_reply_to_status_id': '1',
            'in_reply_to_screen_name': 'me',
            'user_mentions': [{'screen_name': 'me'}]})

    def test_track_stream_for_non_tweet(self):
        with LogCatcher() as lc:
            self.transport.track_stream.respond_with({'foo': 'bar'})
            self.assertEqual(
                lc.messages(),
                ["Received non-tweet from tracking stream: {'foo': 'bar'}"])

    def test_user_stream_for_non_reply_tweet(self):
        with LogCatcher() as lc:
            self.transport.user_stream.respond_with({'foo': 'bar'})
            self.assertEqual(
                lc.messages(),
                ["Received non-tweet from user stream: {'foo': 'bar'}"])
