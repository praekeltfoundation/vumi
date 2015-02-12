# -*- test-case-name: vumi.transports.twitter.tests.test_twitter -*-
from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from txtwitter.twitter import TwitterClient
from txtwitter import messagetools

from vumi.transports.base import Transport
from vumi.config import ConfigBool, ConfigText, ConfigList, ConfigDict


class ConfigTwitterEndpoints(ConfigDict):
    field_type = 'twitter_endpoints'

    def clean(self, value):
        endpoints_dict = super(ConfigTwitterEndpoints, self).clean(value)

        if 'dms' not in endpoints_dict and 'tweets' not in endpoints_dict:
            self.raise_config_error(
                "needs configuration for either dms, tweets or both")

        if endpoints_dict.get('dms') == endpoints_dict.get('tweets'):
            self.raise_config_error(
                "has the same endpoint for dms and tweets: '%s'"
                % endpoints_dict['dms'])

        return endpoints_dict


class TwitterTransportConfig(Transport.CONFIG_CLASS):
    screen_name = ConfigText(
        "The screen name for the twitter account",
        required=True, static=True)
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
    endpoints = ConfigTwitterEndpoints(
        "Which endpoints to use for dms and tweets",
        default={'tweets': 'default'}, static=True)
    terms = ConfigList(
        "A list of terms to be tracked by the transport",
        default=[], static=True)
    autofollow = ConfigBool(
        "Determines whether the transport will follow users that follow the "
        "transport's user",
        default=False, static=True)


class TwitterTransport(Transport):
    """Twitter transport."""

    transport_type = 'twitter'

    CONFIG_CLASS = TwitterTransportConfig
    NO_USER_ADDR = 'NO_USER'

    OUTBOUND_HANDLERS = {
        'tweets': 'handle_outbound_tweet',
        'dms': 'handle_outbound_dm',
    }

    def get_client(self, *a, **kw):
        return TwitterClient(*a, **kw)

    def setup_transport(self):
        config = self.get_static_config()
        self.screen_name = config.screen_name

        self.autofollow = config.autofollow

        self.client = self.get_client(
            config.access_token,
            config.access_token_secret,
            config.consumer_key,
            config.consumer_secret)

        self.endpoints = config.endpoints

        for msg_type, endpoint in self.endpoints.iteritems():
            handler = getattr(self, self.OUTBOUND_HANDLERS[msg_type])
            handler = self.make_outbound_handler(handler)
            self.add_outbound_handler(handler, endpoint_name=endpoint)

        if config.terms:
            self.track_stream = self.client.stream_filter(
                self.handle_track_stream, track=config.terms)
            self.track_stream.startService()
        else:
            self.track_stream = None

        self.user_stream = self.client.userstream_user(
            self.handle_user_stream, with_='user')
        self.user_stream.startService()

    @inlineCallbacks
    def teardown_transport(self):
        if self.track_stream is not None:
            yield self.track_stream.stopService()

        yield self.user_stream.stopService()

    def make_outbound_handler(self, twitter_handler):
        @inlineCallbacks
        def handler(message):
            try:
                twitter_message = yield twitter_handler(message)

                yield self.publish_ack(
                    user_message_id=message['message_id'],
                    sent_message_id=twitter_message['id_str'])
            except Exception, e:
                reason = '%s' % (e,)
                log.err('Outbound twitter message failed: %s' % (reason,))

                yield self.publish_nack(
                    user_message_id=message['message_id'],
                    sent_message_id=message['message_id'],
                    reason=reason)

        return handler

    @classmethod
    def screen_name_as_addr(cls, screen_name):
        return u'@%s' % (screen_name,)

    @classmethod
    def addr_as_screen_name(cls, addr):
        return addr[1:] if addr.startswith('@') else addr

    def is_own_tweet(self, message):
        user = messagetools.tweet_user(message)
        return self.screen_name == messagetools.user_screen_name(user)

    def is_own_dm(self, message):
        sender = messagetools.dm_sender(message)
        return self.screen_name == messagetools.user_screen_name(sender)

    def is_own_follow(self, message):
        source_screen_name = messagetools.user_screen_name(message['source'])
        return source_screen_name == self.screen_name

    @classmethod
    def tweet_to_addr(cls, tweet):
        mentions = messagetools.tweet_user_mentions(tweet)
        to_addr = cls.NO_USER_ADDR

        if mentions:
            mention = mentions[0]
            [start_index, end_index] = mention['indices']

            if start_index == 0:
                to_addr = cls.screen_name_as_addr(mention['screen_name'])

        return to_addr

    @classmethod
    def tweet_from_addr(cls, tweet):
        user = messagetools.tweet_user(tweet)
        return cls.screen_name_as_addr(messagetools.user_screen_name(user))

    @classmethod
    def tweet_content(cls, tweet):
        to_addr = cls.tweet_to_addr(tweet)
        content = messagetools.tweet_text(tweet)

        if to_addr != cls.NO_USER_ADDR and content.startswith(to_addr):
            content = content[len(to_addr):].lstrip()

        return content

    def publish_tweet(self, tweet):
        return self.publish_message(
            content=self.tweet_content(tweet),
            to_addr=self.tweet_to_addr(tweet),
            from_addr=self.tweet_from_addr(tweet),
            transport_type=self.transport_type,
            routing_metadata={
                'endpoint_name': self.endpoints['tweets']
            },
            transport_metadata={
                'twitter': {
                    'status_id': messagetools.tweet_id(tweet)
                }
            },
            helper_metadata={
                'twitter': {
                    'in_reply_to_status_id': (
                        messagetools.tweet_in_reply_to_id(tweet)),
                    'in_reply_to_screen_name': (
                        messagetools.tweet_in_reply_to_screen_name(tweet)),
                    'user_mentions': messagetools.tweet_user_mentions(tweet),
                }
            })

    def publish_dm(self, dm):
        sender = messagetools.dm_sender(dm)
        recipient = messagetools.dm_recipient(dm)

        return self.publish_message(
            content=messagetools.dm_text(dm),
            to_addr=self.screen_name_as_addr(recipient['screen_name']),
            from_addr=self.screen_name_as_addr(sender['screen_name']),
            transport_type=self.transport_type,
            routing_metadata={
                'endpoint_name': self.endpoints['dms']
            },
            helper_metadata={
                'dm_twitter': {
                    'id': messagetools.dm_id(dm),
                    'user_mentions': messagetools.dm_user_mentions(dm),
                }
            })

    def handle_track_stream(self, message):
        if messagetools.is_tweet(message):
            if self.is_own_tweet(message):
                log.msg("Tracked own tweet: %r" % (message,))
            else:
                log.msg("Tracked a tweet: %r" % (message,))
                self.publish_tweet(message)
        else:
            log.msg("Received non-tweet from tracking stream: %r" % message)

    def handle_user_stream(self, message):
        if messagetools.is_tweet(message):
            return self.handle_inbound_tweet(message)
        elif messagetools.is_dm(message.get('direct_message', {})):
            return self.handle_inbound_dm(message['direct_message'])
        elif message.get('event') == 'follow':
            return self.handle_follow(message)

        log.msg(
            "Received a user stream message that we do not handle: %r" %
            message)

    def handle_follow(self, follow):
        if self.is_own_follow(follow):
            log.msg("Received own follow on user stream: %r" % (follow,))
            return

        log.msg("Received follow on user stream: %r" % (follow,))

        if self.autofollow:
            screen_name = messagetools.user_screen_name(follow['source'])
            log.msg("Auto-following '%s'" %
                    (self.screen_name_as_addr(screen_name,)))
            return self.client.friendships_create(screen_name=screen_name)

    def handle_inbound_dm(self, dm):
        if self.is_own_dm(dm):
            log.msg("Received own DM on user stream: %r" % (dm,))
        elif 'dms' not in self.endpoints:
            log.msg(
                "Discarding DM received on user stream, no endpoint "
                "configured for DMs: %r" % (dm,))
        else:
            log.msg("Received DM on user stream: %r" % (dm,))
            self.publish_dm(dm)

    def handle_inbound_tweet(self, tweet):
        if self.is_own_tweet(tweet):
            log.msg("Received own tweet on user stream: %r" % (tweet,))
        elif 'tweets' not in self.endpoints:
            log.msg(
                "Discarding tweet received on user stream, no endpoint "
                "configured for tweets: %r" % (tweet,))
        else:
            log.msg("Received tweet on user stream: %r" % (tweet,))
            self.publish_tweet(tweet)

    def handle_outbound_dm(self, message):
        return self.client.direct_messages_new(
            screen_name=self.addr_as_screen_name(message['to_addr']),
            text=message['content'])

    def handle_outbound_tweet(self, message):
        log.msg("Twitter transport sending tweet %r" % (message,))

        metadata = message['transport_metadata'].get(self.transport_type, {})
        in_reply_to_status_id = metadata.get('status_id')

        content = message['content']
        if message['to_addr'] != self.NO_USER_ADDR:
            content = "%s %s" % (message['to_addr'], content)

        return self.client.statuses_update(
            content, in_reply_to_status_id=in_reply_to_status_id)
