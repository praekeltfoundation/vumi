from twisted.internet.defer import inlineCallbacks
from txtwitter.tests.fake_twitter import FakeTwitter

from vumi.tests.utils import LogCatcher
from vumi.tests.helpers import VumiTestCase
from vumi.config import Config
from vumi.errors import ConfigError
from vumi.transports.twitter import (
    ConfigTwitterEndpoints, TwitterTransport)
from vumi.transports.tests.helpers import TransportHelper


class TestTwitterEndpointsConfig(VumiTestCase):
    def test_clean_no_endpoints(self):
        class ToyConfig(Config):
            endpoints = ConfigTwitterEndpoints("test endpoints")

        self.assertRaises(ConfigError, ToyConfig, {'endpoints': {}})

    def test_clean_same_endpoints(self):
        class ToyConfig(Config):
            endpoints = ConfigTwitterEndpoints("test endpoints")

        self.assertRaises(ConfigError, ToyConfig, {'endpoints': {
            'dms': 'default',
            'tweets': 'default'
        }})


class TestTwitterTransport(VumiTestCase):
    @inlineCallbacks
    def setUp(self):
        self.twitter = FakeTwitter()
        self.user = self.twitter.new_user('me', 'me')
        self.client = self.twitter.get_client(self.user.id_str)

        self.patch(
            TwitterTransport, 'get_client', lambda *a, **kw: self.client)

        self.tx_helper = self.add_helper(TransportHelper(TwitterTransport))

        self.transport = yield self.tx_helper.get_transport({
            'screen_name': 'me',
            'consumer_key': 'consumer1',
            'consumer_secret': 'consumersecret1',
            'access_token': 'token1',
            'access_token_secret': 'tokensecret1',
            'terms': ['arnold', 'the', 'term'],
        })

    @inlineCallbacks
    def test_tracking_messages(self):
        someone = self.twitter.new_user('someone', 'someone')
        tweet = self.twitter.new_tweet('arnold', someone.id_str)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], '@someone')
        self.assertEqual(msg['to_addr'], 'NO_USER')
        self.assertEqual(msg['content'], 'arnold')

        self.assertEqual(
            msg['transport_metadata'],
            {'twitter': {'status_id': tweet.id_str}})

        self.assertEqual(msg['helper_metadata'], {
            'twitter': {
                'in_reply_to_status_id': None,
                'in_reply_to_screen_name': None,
                'user_mentions': []
            }
        })

    @inlineCallbacks
    def test_tracking_reply_messages(self):
        someone = self.twitter.new_user('someone', 'someone')
        someone_else = self.twitter.new_user('someone_else', 'someone_else')
        tweet1 = self.twitter.new_tweet('@someone_else hello', someone.id_str)
        tweet2 = self.twitter.new_tweet(
            '@someone arnold', someone_else.id_str, reply_to=tweet1.id_str)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], '@someone_else')
        self.assertEqual(msg['to_addr'], '@someone')
        self.assertEqual(msg['content'], 'arnold')

        self.assertEqual(
            msg['transport_metadata'],
            {'twitter': {'status_id': tweet2.id_str}})

        self.assertEqual(msg['helper_metadata'], {
            'twitter': {
                'in_reply_to_status_id': tweet1.id_str,
                'in_reply_to_screen_name': 'someone',
                'user_mentions': [{
                    'id_str': someone.id_str,
                    'id': int(someone.id_str),
                    'indices': [0, 8],
                    'screen_name': someone.screen_name,
                    'name': someone.name,
                }]
            }
        })

    def test_tracking_own_messages(self):
        with LogCatcher() as lc:
            tweet = self.twitter.new_tweet('arnold', self.user.id_str)
            tweet = tweet.to_dict(self.twitter)

            self.assertTrue(any(
                "Tracked own tweet:" in msg for msg in lc.messages()))

    @inlineCallbacks
    def test_inbound_user_message(self):
        someone = self.twitter.new_user('someone', 'someone')
        tweet = self.twitter.new_tweet('@me hello', someone.id_str)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], '@someone')
        self.assertEqual(msg['to_addr'], '@me')
        self.assertEqual(msg['content'], 'hello')

        self.assertEqual(
            msg['transport_metadata'],
            {'twitter': {'status_id': tweet.id_str}})

        self.assertEqual(msg['helper_metadata'], {
            'twitter': {
                'in_reply_to_status_id': None,
                'in_reply_to_screen_name': 'me',
                'user_mentions': [{
                    'id_str': self.user.id_str,
                    'id': int(self.user.id_str),
                    'indices': [0, 3],
                    'screen_name': self.user.screen_name,
                    'name': self.user.name,
                }]
            }
        })

    @inlineCallbacks
    def test_inbound_user_reply_message(self):
        someone = self.twitter.new_user('someone', 'someone')
        tweet1 = self.twitter.new_tweet('@someone hello', self.user.id_str)
        tweet2 = self.twitter.new_tweet(
            '@me goodbye', someone.id_str, reply_to=tweet1.id_str)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], '@someone')
        self.assertEqual(msg['to_addr'], '@me')
        self.assertEqual(msg['content'], 'goodbye')

        self.assertEqual(
            msg['transport_metadata'],
            {'twitter': {'status_id': tweet2.id_str}})

        self.assertEqual(msg['helper_metadata'], {
            'twitter': {
                'in_reply_to_status_id': tweet1.id_str,
                'in_reply_to_screen_name': 'me',
                'user_mentions': [{
                    'id_str': self.user.id_str,
                    'id': int(self.user.id_str),
                    'indices': [0, 3],
                    'screen_name': self.user.screen_name,
                    'name': self.user.name,
                }]
            }
        })

    def test_inbound_user_messages_own_messages(self):
        with LogCatcher() as lc:
            self.twitter.new_tweet('hello', self.user.id_str)

            self.assertTrue(any(
                "Received own tweet on user stream" in msg
                for msg in lc.messages()))

    @inlineCallbacks
    def test_sending(self):
        self.twitter.new_user('someone', 'someone')
        msg = yield self.tx_helper.make_dispatch_outbound(
            'hello', to_addr='@someone')
        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(ack['user_message_id'], msg['message_id'])

        tweet = self.twitter.get_tweet(ack['sent_message_id'])
        self.assertEqual(tweet.text, '@someone hello')
        self.assertEqual(tweet.reply_to, None)

    @inlineCallbacks
    def test_reply_sending(self):
        tweet1 = self.twitter.new_tweet('hello', self.user.id_str)

        inbound_msg = self.tx_helper.make_inbound(
            'hello',
            from_addr='@someone',
            transport_metadata={
                'twitter': {'status_id': tweet1.id_str}
            })

        msg = yield self.tx_helper.make_dispatch_reply(inbound_msg, "goodbye")
        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(ack['user_message_id'], msg['message_id'])

        tweet2 = self.twitter.get_tweet(ack['sent_message_id'])
        self.assertEqual(tweet2.text, '@someone goodbye')
        self.assertEqual(tweet2.reply_to, tweet1.id_str)

    @inlineCallbacks
    def test_sending_failure(self):
        def fail(*a, **kw):
            raise Exception(':(')

        self.patch(self.client, 'statuses_update', fail)

        msg = yield self.tx_helper.make_dispatch_outbound('hello')
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'], ':(')

    def test_track_stream_for_non_tweet(self):
        with LogCatcher() as lc:
            self.transport.handle_track_stream({'foo': 'bar'})

            self.assertEqual(
                lc.messages(),
                ["Received non-tweet from tracking stream: {'foo': 'bar'}"])

    def test_user_stream_for_non_reply_tweet(self):
        with LogCatcher() as lc:
            self.transport.handle_user_stream({'foo': 'bar'})

            self.assertEqual(
                lc.messages(),
                ["Received non-tweet from user stream: {'foo': 'bar'}"])

    def test_tweet_content_with_mention_at_start(self):
        self.assertEqual('hello', self.transport.tweet_content({
            'id_str': '12345',
            'text': '@fakeuser hello',
            'user': {},
            'entities': {
                'user_mentions': [{
                    'id_str': '123',
                    'screen_name': 'fakeuser',
                    'name': 'Fake User',
                    'indices': [0, 8]
                }]
            },
        }))

    def test_tweet_content_with_mention_not_at_start(self):
        self.assertEqual('hello @fakeuser!', self.transport.tweet_content({
            'id_str': '12345',
            'text': 'hello @fakeuser!',
            'user': {},
            'entities': {
                'user_mentions': [{
                    'id_str': '123',
                    'screen_name': 'fakeuser',
                    'name': 'Fake User',
                    'indices': [6, 14]
                }]
            },
        }))

    def test_tweet_content_with_no_mention(self):
        self.assertEqual('hello', self.transport.tweet_content({
            'id_str': '12345',
            'text': 'hello',
            'user': {},
            'entities': {
                'user_mentions': []
            },
        }))

    def test_tweet_content_with_no_user_in_text(self):
        self.assertEqual('NO_USER hello', self.transport.tweet_content({
            'id_str': '12345',
            'text': 'NO_USER hello',
            'user': {},
            'entities': {
                'user_mentions': []
            },
        }))
