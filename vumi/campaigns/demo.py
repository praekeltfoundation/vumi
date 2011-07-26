from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from vumi.campaigns import base

class DemoWorker(base.BaseWorker):
    
    @inlineCallbacks
    def startWorker(self):
        # Connect to the various transports
        # subscribe to the GSM transport to receive PCMs
        self.subscribe_to_transport(transport='gsm', account=self.config.get('msisdn'), 
                                    callback=self.handle_inbound_sms)
        
        # subscribe to the Twitter transport to receive the Tweets
        self.subscribe_to_transport(transport='twitter', account=self.config.get('twitter'),
                                    callback=self.handle_inbound_tweets)
        
        # subscribe to the XMPP transport to receive XMPP message
        self.subscribe_to_transport(transport='xmpp', account='gtalk', 
                                    identifier=self.config.get('gtalk'),
                                    callback=self.handle_inbound_xmpp)
        
        self.sms_publisher = yield self.publish_to_transport(transport='sms', account='opera')
        self.xmpp_publisher = yield self.publish_to_transport(transport='xmpp', account='gtalk', 
                                                                identifier=self.config.get('gtalk'))

    
    def handle_inbound_message(self, transport, uuid, limit):
        if self.is_subscribed(uuid):
            self.say(transport, uuid, "Thanks, africa and #mhealth related tweets "\
                    "have now stopped. Dial *140*%(msisdn)s# or chat to %(gtalk)s "\
                    "to restart." % self.get_subscriber_vars(uuid))
            self.unsubscribe(uuid)
        else:
            self.subscribe(uuid, transport, limit)
            self.say(transport, uuid, "Hi you will shortly be receiving the "\
                    "latest %(limit)s africa and #mhealth related tweets. "\
                    "Dial *140*%(msisdn)s# or chat to %(gtalk)s "\
                    "again to stop" % self.get_subscriber_vars(uuid))
        
    
    def publish_tweet(self, tweet):
        # make a copy as the subscribers dictionary can change while
        # we're iterating
        local_subscribers = self.subscribers.copy()
        for subscriber in local_subscribers:
            transport, counter, limit = self.get_subscriber_info(subscriber)
            
            # send the tweet
            self.say(transport, subscriber, tweet)
            
            # increment the counter for the subscriber
            if self.is_within_quota(subscriber):
                self.increment_message_count_for(subscriber)
            else:
                # notify the user
                self.say(transport, subscriber, "Tweets have stopped as you've "\
                    "reached your maximum of %(limit)s tweets. Dial *140*%(msisdn)s "\
                    "or chat to %(gtalk)s for another %(limit)s messages." % self.get_subscriber_vars(subscriber))
                # unsubscribe the subscriber
                self.unsubscribe(subscriber)

    def handle_inbound_sms(self, message):
        sender = message.payload.get('sender')
        self.handle_inbound_message('sms', sender, 10)
    
    def handle_inbound_xmpp(self, message):
        # xmpp appends user agent info after the / in the address,
        # we don't care about that, assume the email address as the identifier
        sender = message.payload.get('sender').split('/')[0]
        log.msg('received xmpp', message)
        self.handle_inbound_message('xmpp', sender, 50)
    
    def handle_inbound_tweets(self, message):
        if self.subscribers:
            data = message.payload.get('data')
            user = data.get('user', {})
            text = data.get('text', '')
            message = u"@%s: %s" % (user.get('screen_name'), text)
            # for subscriber in self.subscribers.keys():
            self.publish_tweet(message)