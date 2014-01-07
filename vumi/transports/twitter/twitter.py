# -*- test-case-name: vumi.transports.twitter.tests.test_twitter -*-

from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from txtwitter.twitter import TwitterClient
from txtwitter import messagetools

from vumi.transports.base import Transport
from vumi.config import ConfigText, ConfigList


class TwitterTransportConfig(Transport.CONFIG_CLASS):
    consumer_key = ConfigText(
        "The OAuth consumer key for the twitter account",
        required=True, static=True)
    consumer_secret = ConfigText(
        "The OAuth consumer secret for the twitter account",
        required=True, static=True)
    access_token = ConfigText(
        "The OAuth access token for the twitter account",
        required=True, static=True)
    access_token_secret = ConfigText(
        "The OAuth access token secret for the twitter account",
        required=True, static=True)
    terms = ConfigList(
        "A list of terms to be tracked by the transport",
        default=[], static=True)


class TwitterTransport(Transport):
    """Twitter transport."""

    transport_type = 'twitter'

    CONFIG_CLASS = TwitterTransportConfig
    ENCODING = 'utf8'
    CLIENT_CLS = TwitterClient

    def setup_transport(self):
        config = self.get_static_config()
        self.terms = config.terms

        self.client = self.CLIENT_CLS(
            config.access_token,
            config.access_token_secret,
            config.consumer_key,
            config.consumer_secret)

        self.track_stream = self.client.stream_filter(
            self.handle_track_stream, self.terms)
        if self.terms:
            self.track_stream.startService()

        self.user_stream = self.client.userstream_user(self.handle_user_stream)
        self.user_stream.startService()

    @inlineCallbacks
    def teardown_transport(self):
        yield self.user_stream.stopService()
        yield self.track_stream.stopService()

    @classmethod
    def decode(cls, value):
        if value is None:
            return value

        if not isinstance(value, basestring):
            value = str(value)

        return value.decode(cls.ENCODING)

    @classmethod
    def encode(cls, value):
        if value is None:
            return value

        if not isinstance(value, basestring):
            value = str(value)

        return value.encode(cls.ENCODING)

    def publish_tweet_message(self, tweet):
        in_reply_to_screen_name = tweet.get('in_reply_to_screen_name')

        return self.publish_message(
            content=self.decode(messagetools.tweet_text(tweet)),
            to_addr=self.decode(in_reply_to_screen_name or ''),
            from_addr=self.decode(tweet['user'].get('screen_name', '')),
            transport_type=self.transport_type,
            transport_metadata={
                self.transport_type: {
                    'status_id': self.decode(tweet.get('id_str'))
                }
            },
            helper_metadata={
                'in_reply_to_status_id': self.decode(
                    messagetools.tweet_in_reply_to_id(tweet)),
                'in_reply_to_screen_name': self.decode(
                    in_reply_to_screen_name),
                'user_mentions': messagetools.tweet_user_mentions(tweet),
            })

    @inlineCallbacks
    def handle_outbound_message(self, message):
        # TODO Use in_reply_to_status_id once txTwitter supports it
        log.msg("Twitter transport sending %r" % (message,))

        try:
            yield self.client.statuses_update(self.encode(message['content']))

            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'])
        except Exception, e:
            yield self.publish_nack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                reason='%r' % (e,))

    def handle_track_stream(self, message):
        if messagetools.is_tweet(message):
            log.msg("Tracked a tweet: %r" % (message,))
            self.publish_tweet_message(message)
        else:
            log.msg("Received non-tweet from tracking stream: %r" % message)

    def handle_user_stream(self, message):
        if messagetools.is_tweet(message):
            log.msg("Received tweet on user stream: %r" % (message,))
            self.publish_tweet_message(message)
        else:
            log.msg("Received non-tweet from user stream: %r" % message)
