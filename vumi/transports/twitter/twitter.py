# -*- test-case-name: vumi.transports.twitter.tests.test_twitter -*-

from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet import task
from txtwitter.twitter import TwitterClient
from txtwitter.messagetools import is_tweet

from vumi.transports.base import Transport
from vumi.config import ConfigText, ConfigList, ConfigInt
from vumi.message import TransportUserMessage


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
    check_replies_interval = ConfigInt(
        "How often (in seconds) to poll for replies to status updates",
        default=0, static=True)


class TwitterTransport(Transport):
    """Twitter transport."""

    transport_type = 'twitter'

    CONFIG_CLASS = TwitterTransportConfig
    ENCODING = 'utf8'
    CLIENT_CLS = TwitterClient

    @classmethod
    def decode(cls, s):
        if not isinstance(s, basestring):
            s = str(s)
        return s.decode(cls.ENCODING)

    def setup_transport(self):
        config = self.get_static_config()
        self.terms = config.terms

        self.client = self.CLIENT_CLS(
            config.access_token,
            config.access_token_secret,
            config.consumer_key,
            config.consumer_secret)

        self.track_stream = self.client.stream_filter(
            self.handle_track, self.terms)
        if self.terms:
            self.track_stream.startService()

        self.check_replies = task.LoopingCall(self.check_for_replies)
        if config.check_replies_interval > 0:
            self.check_replies.start(config.check_replies_interval)

    @inlineCallbacks
    def teardown_transport(self):
        if self.check_replies.running:
            self.check_replies.stop()

        yield self.track_stream.stopService()

    def check_for_replies(self):
        #TODO
        pass

    @inlineCallbacks
    def handle_outbound_message(self, message):
        # TODO Use in_reply_to_status_id once txTwitter supports it
        log.msg("Twitter transport sending %r" % (message,))

        try:
            response = yield self.client.update(message['content'])

            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=response['id_str'])
        except Exception, e:
            yield self.publish_nack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                reason='%r' % (e,))

    def handle_track(self, msg):
        """
        Gets called with a status update whenever a tweet matching
        a term being tracked is detected. Attached the SESSION_NONE
        event type as these messages aren't necessarily part of a
        conversation.
        """
        if is_tweet(msg):
            self.publish_message(
                message_id=self.decode(msg['id_str']),
                content=self.decode(msg['text']),
                to_addr=self.decode(msg.get('in_reply_to_screen_name', '')),
                from_addr=self.decode(msg['user']['screen_name']),
                session_event=TransportUserMessage.SESSION_NONE,
                transport_type=self.transport_type,
                transport_metadata=msg)
        else:
            log.msg("Received non-tweet from tracking stream: %r" % msg)
