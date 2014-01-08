# -*- coding: utf-8 -*-

from twisted.internet.defer import inlineCallbacks
from txtwitter.tests.fake_twitter import FakeTwitter

from vumi.tests.utils import LogCatcher
from vumi.tests.helpers import VumiTestCase
from vumi.transports.twitter import TwitterTransport
from vumi.transports.tests.helpers import TransportHelper


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
            'terms': ['some', 'trending', 'topic'],
        })

    '''
    @inlineCallbacks
    def test_tracking_messages(self):
        self.transport.track_stream.respond_with({
            'id_str': '1',
            'text': 'hello',
            'in_reply_to_status_id_str': None,
            'in_reply_to_screen_name': None,
            'user': {
                'id_str': '0',
                'screen_name': 'someone'
            },
            'entities': {
                'user_mentions': [{'screen_name': 'someone_else'}]
            }
        })

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], 'someone')
        self.assertEqual(msg['to_addr'], '')
        self.assertEqual(msg['content'], 'hello')

        self.assertEqual(
            msg['transport_metadata'],
            {'twitter': {'status_id': '1'}})

        self.assertEqual(msg['helper_metadata'], {
            'twitter': {
                'in_reply_to_status_id': None,
                'in_reply_to_screen_name': None,
                'user_mentions': [{'screen_name': 'someone_else'}]
            }
        })

    @inlineCallbacks
    def test_tracking_reply_messages(self):
        self.transport.track_stream.respond_with({
            'id_str': '2',
            'text': 'hello',
            'in_reply_to_status_id_str': '1',
            'in_reply_to_screen_name': 'someone_else',
            'user': {
                'id_str': '0',
                'screen_name': 'someone'
            },
            'entities': {
                'user_mentions': [{'screen_name': 'someone_else'}]
            }
        })

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], 'someone')
        self.assertEqual(msg['to_addr'], 'someone_else')
        self.assertEqual(msg['content'], 'hello')

        self.assertEqual(
            msg['transport_metadata'],
            {'twitter': {'status_id': '2'}})

        self.assertEqual(msg['helper_metadata'], {
            'twitter': {
                'in_reply_to_status_id': '1',
                'in_reply_to_screen_name': 'someone_else',
                'user_mentions': [{'screen_name': 'someone_else'}]
            }
        })

    @inlineCallbacks
    def test_tracking_message_decoding(self):
        self.transport.track_stream.respond_with({
            'id_str': '2',
            'text': 'hëllo',
            'in_reply_to_status_id_str': '1',
            'in_reply_to_screen_name': 'somëone_else',
            'user': {
                'id_str': '0',
                'screen_name': 'somëone'
            },
            'entities': {
                'user_mentions': [{'screen_name': 'somëone_else'}]
            }
        })

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], u'somëone')
        self.assertEqual(msg['to_addr'], u'somëone_else')
        self.assertEqual(msg['content'], u'hëllo')

        self.assertEqual(
            msg['transport_metadata'],
            {'twitter': {'status_id': '2'}})

        self.assertEqual(msg['helper_metadata'], {
            'twitter': {
                'in_reply_to_status_id': '1',
                'in_reply_to_screen_name': u'somëone_else',
                'user_mentions': [{'screen_name': u'somëone_else'}]
            }
        })

    def test_tracking_own_messages(self):
        tweet = {
            'id_str': '1',
            'text': 'hello',
            'in_reply_to_status_id_str': None,
            'in_reply_to_screen_name': None,
            'user': {
                'id_str': '0',
                'screen_name': 'me'
            },
            'entities': {
                'user_mentions': [{'screen_name': 'someone_else'}]
            }
        }

        with LogCatcher() as lc:
            self.transport.track_stream.respond_with(tweet)

            self.assertEqual(
                lc.messages(),
                ["Tracked own tweet: %r" % tweet])

    @inlineCallbacks
    def test_inbound_user_message(self):
        self.transport.user_stream.respond_with({
            'id_str': '1',
            'text': 'hello',
            'in_reply_to_screen_name': None,
            'in_reply_to_status_id_str': None,
            'user': {
                'id_str': '0',
                'screen_name': 'someone'
            },
            'entities': {
                'user_mentions': [{'screen_name': 'me'}]
            }
        })

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], 'someone')
        self.assertEqual(msg['to_addr'], '')
        self.assertEqual(msg['content'], 'hello')

        self.assertEqual(
            msg['transport_metadata'],
            {'twitter': {'status_id': '1'}})

        self.assertEqual(msg['helper_metadata'], {
            'twitter': {
                'in_reply_to_status_id': None,
                'in_reply_to_screen_name': None,
                'user_mentions': [{'screen_name': 'me'}]
            }
        })

    @inlineCallbacks
    def test_inbound_user_reply_message(self):
        self.transport.user_stream.respond_with({
            'id_str': '2',
            'text': 'hello',
            'in_reply_to_status_id_str': '1',
            'in_reply_to_screen_name': 'me',
            'user': {
                'id_str': '0',
                'screen_name': 'someone'
            },
            'entities': {
                'user_mentions': [{'screen_name': 'me'}]
            }
        })

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], 'someone')
        self.assertEqual(msg['to_addr'], 'me')
        self.assertEqual(msg['content'], 'hello')

        self.assertEqual(
            msg['transport_metadata'],
            {'twitter': {'status_id': '2'}})

        self.assertEqual(msg['helper_metadata'], {
            'twitter': {
                'in_reply_to_status_id': '1',
                'in_reply_to_screen_name': 'me',
                'user_mentions': [{'screen_name': 'me'}]
            }
        })

    @inlineCallbacks
    def test_inbound_user_message_decoding(self):
        self.transport.user_stream.respond_with({
            'id_str': '2',
            'text': 'hëllo',
            'in_reply_to_status_id_str': '1',
            'in_reply_to_screen_name': 'somëone_else',
            'user': {
                'id_str': '0',
                'screen_name': 'somëone'
            },
            'entities': {
                'user_mentions': [{'screen_name': 'somëone_else'}]
            }
        })

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], u'somëone')
        self.assertEqual(msg['to_addr'], u'somëone_else')
        self.assertEqual(msg['content'], u'hëllo')

        self.assertEqual(
            msg['transport_metadata'],
            {'twitter': {'status_id': '2'}})

        self.assertEqual(msg['helper_metadata'], {
            'twitter': {
                'in_reply_to_status_id': '1',
                'in_reply_to_screen_name': u'somëone_else',
                'user_mentions': [{'screen_name': u'somëone_else'}]
            }
        })

    def test_inbound_user_messages_own_messages(self):
        tweet = {
            'id_str': '1',
            'text': 'hello',
            'in_reply_to_status_id_str': None,
            'in_reply_to_screen_name': None,
            'user': {
                'id_str': '0',
                'screen_name': 'me'
            },
            'entities': {
                'user_mentions': [{'screen_name': 'someone_else'}]
            }
        }

        with LogCatcher() as lc:
            self.transport.user_stream.respond_with(tweet)

            self.assertEqual(
                lc.messages(),
                ["Received own tweet on user stream: %r" % tweet])
    '''

    @inlineCallbacks
    def test_sending(self):
        msg = yield self.tx_helper.make_dispatch_outbound('hello')
        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(ack['user_message_id'], msg['message_id'])

        tweet = self.twitter.get_tweet(ack['sent_message_id'])
        self.assertEqual(tweet.text, 'hello')
        self.assertEqual(tweet.reply_to, None)

    @inlineCallbacks
    def test_reply_sending(self):
        inbound_tweet = self.twitter.new_tweet('hello', self.user.id_str)

        inbound_msg = yield self.tx_helper.make_dispatch_inbound(
            'hello', transport_metadata={
                'twitter': {'status_id': inbound_tweet.id_str}
            })

        msg = yield self.tx_helper.make_dispatch_reply(inbound_msg, "goodbye")
        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(ack['user_message_id'], msg['message_id'])

        tweet = self.twitter.get_tweet(ack['sent_message_id'])
        self.assertEqual(tweet.text, 'goodbye')
        self.assertEqual(tweet.reply_to, inbound_tweet.id_str)

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

    @inlineCallbacks
    def test_sending_message_encoding(self):
        msg = yield self.tx_helper.make_dispatch_outbound(u'hëllo')
        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(ack['user_message_id'], msg['message_id'])

        tweet = self.twitter.get_tweet(ack['sent_message_id'])
        self.assertEqual(tweet.text, 'hëllo')
        self.assertEqual(tweet.reply_to, None)

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
