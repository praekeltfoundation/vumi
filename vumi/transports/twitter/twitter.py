# -*- test-case-name: vumi.transports.twitter.tests.test_twitter -*-
import redis
from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet import task
from twittytwister import twitter
from oauth import oauth
from vumi.transports.base import Transport
from vumi.message import TransportUserMessage
from vumi.utils import get_deploy_int


class TwitterTransport(Transport):

    transport_type = 'twitter'

    def validate_config(self):
        self.consumer_key = self.config['consumer_key']
        self.consumer_secret = self.config['consumer_secret']
        self.access_token = self.config['access_token']
        self.access_token_secret = self.config['access_token_secret']
        self.r_config = self.config.get('redis', {})
        self.r_prefix = "%(transport_name)s@%(app_name)s:replies" % self.config
        self.terms = set(self.config.get('terms'))
        self.check_replies_interval = int(self.config.get(
                            'check_replies_interval', 60))

    @inlineCallbacks
    def setup_transport(self):
        # TODO: get_deploy_int must die
        dbindex = get_deploy_int(self._amqp_client.vhost)
        self.r_server = yield redis.Redis(db=dbindex, **self.r_config)
        consumer = oauth.OAuthConsumer(self.consumer_key, self.consumer_secret)
        token = oauth.OAuthToken(self.access_token, self.access_token_secret)
        self.twitter = twitter.TwitterFeed(consumer=consumer, token=token)
        yield self.start_tracking_terms()
        self.start_checking_for_replies()

    @inlineCallbacks
    def start_tracking_terms(self):
        if self.terms:
            self.stream = yield self.twitter.track(self.handle_track,
                                                   self.terms)

    def start_checking_for_replies(self):
        self.check_replies = task.LoopingCall(self.check_for_replies)
        self.check_replies.start(self.check_replies_interval)

    def teardown_transport(self):
        if self.check_replies.running:
            self.check_replies.stop()

    @inlineCallbacks
    def check_for_replies(self):
        yield self.twitter.replies(self.handle_replies)

    @inlineCallbacks
    def handle_outbound_message(self, message):
        """
        TODO:   Add in_reply_to_status_id parameter if present,
                need access to the Twitter docs to do so at the
                moment.
        """
        log.msg("Twitter transport sending %r" % (message,))
        post_id = yield self.twitter.update(message['content'])
        self.publish_ack(user_message_id=message['message_id'],
                            sent_message_id=post_id)

    @property
    def last_reply_timestamp(self):
        r_key = '%s:%s' % (self.r_prefix, 'last_reply_timestamp')
        return self.r_server.get(r_key)

    @last_reply_timestamp.setter
    def last_reply_timestamp(self, value):
        r_key = '%s:%s' % (self.r_prefix, 'last_reply_timestamp')
        return self.r_server.set(r_key, value)

    def handle_replies(self, message):
        """
        handle_replies is called at a regular interval to check for replies
        that are received on the given account. Attached the SESSION_RESUME
        event type to the messages to keep them distinguishable from messages
        arriving by tracking terms in realtime.
        """
        if self.last_reply_timestamp == None or \
            message.published > self.last_reply_timestamp:
            self.publish_message(
                message_id=message.id,
                content=message.text,
                to_addr=message.title,
                from_addr=message.author.screen_name,
                session_event=TransportUserMessage.SESSION_RESUME,
                transport_type=self.transport_type,
                transport_metadata=message.raw,
            )
            self.last_reply_timestamp = message.published

    def handle_track(self, status):
        """
        Get hits with a status update whenever a tweet matching
        a term being tracked is detected. Attached the SESSION_NONE
        event type as these messages aren't necessarily part of a
        conversation.
        """
        self.publish_message(
            message_id=status.id,
            content=status.text,
            to_addr=status.in_reply_to_screen_name,
            from_addr=status.user.screen_name,
            session_event=TransportUserMessage.SESSION_NONE,
            transport_type=self.transport_type,
            transport_metadata=status.raw,
        )
