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

        self.config = {
            'screen_name': 'me',
            'consumer_key': 'consumer1',
            'consumer_secret': 'consumersecret1',
            'access_token': 'token1',
            'access_token_secret': 'tokensecret1',
            'terms': ['arnold', 'the', 'term'],
            'endpoints': {
                'tweets': 'tweet_endpoint',
                'dms': 'dm_endpoint'
            }
        }

        self.transport = yield self.tx_helper.get_transport(self.config)

    def test_config_endpoints_default(self):
        del self.config['endpoints']
        self.config['transport_name'] = 'twitter'
        config = TwitterTransport.CONFIG_CLASS(self.config)
        self.assertEqual(config.endpoints, {'tweets': 'default'})

    @inlineCallbacks
    def test_config_no_tracking_stream(self):
        self.config['terms'] = []
        transport = yield self.tx_helper.get_transport(self.config)
        self.assertEqual(transport.track_stream, None)

    @inlineCallbacks
    def test_tracking_tweets(self):
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
    def test_tracking_reply_tweets(self):
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
    def test_inbound_tweet(self):
        someone = self.twitter.new_user('someone', 'someone')
        tweet = self.twitter.new_tweet('@me hello', someone.id_str)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], '@someone')
        self.assertEqual(msg['to_addr'], '@me')
        self.assertEqual(msg['content'], 'hello')
        self.assertEqual(msg.get_routing_endpoint(), 'tweet_endpoint')

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
    def test_inbound_tweet_reply(self):
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

    def test_inbound_own_tweet(self):
        with LogCatcher() as lc:
            self.twitter.new_tweet('hello', self.user.id_str)

            self.assertTrue(any(
                "Received own tweet on user stream" in msg
                for msg in lc.messages()))

    @inlineCallbacks
    def test_inbound_tweet_no_endpoint(self):
        self.config['endpoints'] = {'dms': 'default'}
        yield self.tx_helper.get_transport(self.config)
        someone = self.twitter.new_user('someone', 'someone')

        with LogCatcher() as lc:
            self.twitter.new_tweet('@me hello', someone.id_str)

            self.assertTrue(any(
                "Discarding tweet received on user stream, no endpoint "
                "configured for tweets" in msg
                for msg in lc.messages()))

    @inlineCallbacks
    def test_inbound_dm(self):
        someone = self.twitter.new_user('someone', 'someone')
        dm = self.twitter.new_dm('hello @me', someone.id_str, self.user.id_str)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], '@someone')
        self.assertEqual(msg['to_addr'], '@me')
        self.assertEqual(msg['content'], 'hello @me')
        self.assertEqual(msg.get_routing_endpoint(), 'dm_endpoint')

        self.assertEqual(msg['helper_metadata'], {
            'dm_twitter': {
                'id': dm.id_str,
                'user_mentions': [{
                    'id_str': self.user.id_str,
                    'id': int(self.user.id_str),
                    'indices': [6, 9],
                    'screen_name': self.user.screen_name,
                    'name': self.user.name,
                }]
            }
        })

    def test_inbound_own_dm(self):
        with LogCatcher() as lc:
            someone = self.twitter.new_user('someone', 'someone')
            self.twitter.new_dm('hello', self.user.id_str, someone.id_str)

            self.assertTrue(any(
                "Received own DM on user stream" in msg
                for msg in lc.messages()))

    @inlineCallbacks
    def test_inbound_dm_no_endpoint(self):
        self.config['endpoints'] = {'tweets': 'default'}
        yield self.tx_helper.get_transport(self.config)
        someone = self.twitter.new_user('someone', 'someone')

        with LogCatcher() as lc:
            self.twitter.new_dm('hello @me', someone.id_str, self.user.id_str)

            self.assertTrue(any(
                "Discarding DM received on user stream, no endpoint "
                "configured for DMs" in msg
                for msg in lc.messages()))

    @inlineCallbacks
    def test_auto_following(self):
        self.config['autofollow'] = True
        yield self.tx_helper.get_transport(self.config)

        with LogCatcher() as lc:
            someone = self.twitter.new_user('someone', 'someone')
            self.twitter.add_follow(someone.id_str, self.user.id_str)

            self.assertTrue(any(
                "Received follow on user stream" in msg
                for msg in lc.messages()))

            self.assertTrue(any(
                "Auto-following '@someone'" in msg
                for msg in lc.messages()))

        follow = self.twitter.get_follow(self.user.id_str, someone.id_str)
        self.assertEqual(follow.source_id, self.user.id_str)
        self.assertEqual(follow.target_id, someone.id_str)

    @inlineCallbacks
    def test_auto_following_disabled(self):
        self.config['autofollow'] = False
        yield self.tx_helper.get_transport(self.config)

        with LogCatcher() as lc:
            someone = self.twitter.new_user('someone', 'someone')
            self.twitter.add_follow(someone.id_str, self.user.id_str)

            self.assertTrue(any(
                "Received follow on user stream" in msg
                for msg in lc.messages()))

        follow = self.twitter.get_follow(self.user.id_str, someone.id_str)
        self.assertTrue(follow is None)

    def test_inbound_own_follow(self):
        with LogCatcher() as lc:
            someone = self.twitter.new_user('someone', 'someone')
            self.twitter.add_follow(self.user.id_str, someone.id_str)

            self.assertTrue(any(
                "Received own follow on user stream" in msg
                for msg in lc.messages()))

    @inlineCallbacks
    def test_tweet_sending(self):
        self.twitter.new_user('someone', 'someone')
        msg = yield self.tx_helper.make_dispatch_outbound(
            'hello', to_addr='@someone', endpoint='tweet_endpoint')
        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(ack['user_message_id'], msg['message_id'])

        tweet = self.twitter.get_tweet(ack['sent_message_id'])
        self.assertEqual(tweet.text, '@someone hello')
        self.assertEqual(tweet.reply_to, None)

    @inlineCallbacks
    def test_tweet_reply_sending(self):
        tweet1 = self.twitter.new_tweet(
            'hello', self.user.id_str, endpoint='tweet_endpoint')

        inbound_msg = self.tx_helper.make_inbound(
            'hello',
            from_addr='@someone',
            endpoint='tweet_endpoint',
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
    def test_tweet_sending_failure(self):
        def fail(*a, **kw):
            raise Exception(':(')

        self.patch(self.client, 'statuses_update', fail)

        with LogCatcher() as lc:
            msg = yield self.tx_helper.make_dispatch_outbound(
                'hello', endpoint='tweet_endpoint')

            self.assertEqual(
                [e['message'][0] for e in lc.errors],
                ["'Outbound twitter message failed: :('"])

        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'], ':(')

    @inlineCallbacks
    def test_dm_sending(self):
        self.twitter.new_user('someone', 'someone')

        msg = yield self.tx_helper.make_dispatch_outbound(
            'hello', to_addr='@someone', endpoint='dm_endpoint')
        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(ack['user_message_id'], msg['message_id'])

        dm = self.twitter.get_dm(ack['sent_message_id'])
        sender = self.twitter.get_user(dm.sender_id_str)
        recipient = self.twitter.get_user(dm.recipient_id_str)

        self.assertEqual(dm.text, 'hello')
        self.assertEqual(sender.screen_name, 'me')
        self.assertEqual(recipient.screen_name, 'someone')

    @inlineCallbacks
    def test_dm_sending_failure(self):
        def fail(*a, **kw):
            raise Exception(':(')

        self.patch(self.client, 'direct_messages_new', fail)

        with LogCatcher() as lc:
            msg = yield self.tx_helper.make_dispatch_outbound(
                'hello', endpoint='dm_endpoint')

            self.assertEqual(
                [e['message'][0] for e in lc.errors],
                ["'Outbound twitter message failed: :('"])

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

    def test_user_stream_for_unsupported_message(self):
        with LogCatcher() as lc:
            self.transport.handle_user_stream({'foo': 'bar'})

            self.assertEqual(
                lc.messages(),
                ["Received a user stream message that we do not handle: "
                 "{'foo': 'bar'}"])

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
