from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from getpass import getpass

from vumi.service import Worker, Consumer, Publisher
from vumi.utils import safe_routing_key
from vumi.message import Message
from django.contrib.auth.models import User, Group
from uuid import uuid4

def get_user(sender):
    try:
        user = User.objects.get(email=sender)
    except User.DoesNotExist, e:
        user = User.objects.create(
                            username=str(uuid4())[:30],
                            email=sender)
        group, _ = Group.objects.get_or_create(name='netprophet')
        user.groups.add(group)
    return user

def stop_user(sender):
    user = get_user(sender)
    user.is_active = False
    user.save()

def start_user(sender):
    user = get_user(sender)
    user.is_active = True
    user.save()

class TwitterConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"            # -> route based on pattern matching
    routing_key = 'twitter.inbound'     # -> overriden in publish method
    durable = False                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 1                   # -> do not save to disk
    
    def __init__(self, publisher):
        self.publisher = publisher
    
    def consume_json(self, dictionary):
        user = dictionary.get('user', {})
        text = dictionary.get('text', '')
        log.msg(u"%s from %s said: %s" % (user.get('name'), user.get('location'), text))
        
        # dsl = DSL()
        # response = dsl.send(text)
        # self.publisher.publish_json({
        #     'to': user.get('username'),
        #     'message': response
        # })

class TwitterPublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"            # -> route based on pattern matching
    routing_key = 'twitter.outbound' # -> overriden in publish method
    durable = False                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 1                   # -> do not save to disk
    
    def publish_json(self, dictionary):
        log.msg("Publishing JSON %s" % dictionary)
        super(TwitterPublisher, self).publish_json(dictionary)
    

class TwitterWorker(Worker):
    
    # inlineCallbacks, TwistedMatrix's fancy way of allowing you to write
    # asynchronous code as if it was synchronous by the nifty use of
    # coroutines.
    # See: http://twistedmatrix.com/documents/10.0.0/api/twisted.internet.defer.html#inlineCallbacks
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the TwitterWorker")
        
        # create the publisher
        self.publisher = yield self.start_publisher(TwitterPublisher)
        # when it's done, create the consumer and pass it the publisher
        self.consumer = yield self.start_consumer(TwitterConsumer, self.publisher)
    
    def stopWorker(self):
        log.msg("Stopping the TwitterWorker")
    
class TwitterXMPPBridgeWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg('Starting the TwitterXMPPBridgeWorker')
        
        # create the publisher
        self.publisher = yield self.publish_to('xmpp.outbound.gtalk.%s' %
                                                self.config['username'])
        # when it's done, create the consumer and pass it the publisher
        self.consume("xmpp.inbound.gtalk.%s" % self.config['username'], 
                        self.consume_xmpp_message)
        self.consume('twitter.inbound.%(username)s.%(terms)s' % self.config['twitter'],
                        self.consume_twitter_message)
        self.subscribers = []
    
    def stopWorker(self):
        log.msg("Stopping the TwitterXMPPBridgeWorker")
    
    def consume_xmpp_message(self, message):
        text = message.payload["message"].strip()
        sender = message.payload["sender"].strip()
        if text.lower().strip() == "stop":
            stop_user(sender)
            self.publisher.publish_message(Message(recipient=sender, 
                                            message='Ok, tweets have stopped'))
        else:
            user = get_user(sender)
            if not user.is_active:
                start_user(sender)
                self.publisher.publish_message(Message(recipient=sender, 
                    message="Hi! You you'll start receiving live tweets " \
                                "from netprophet as soon as they become available. "\
                                "Type STOP to stop receiving tweets."))
    
    def consume_twitter_message(self, message):
        data = message.payload.get('data')
        user = data.get('user', {})
        text = data.get('text', '')
        log.msg(message.payload)
        message = u"@%s (%s from %s) said: %s" % (user.get('screen_name'), user.get('name', 'someone'), user.get('location','somewhere'), text)
        subscribers = User.objects.filter(is_active=True,groups__name='netprophet')
        print subscribers
        for user in subscribers:
            self.publisher.publish_message(Message(
                recipient=user.email, 
                message=message))

