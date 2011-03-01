from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from getpass import getpass
from twittytwister import twitter

from vumi.service import Worker, Consumer, Publisher

class TwitterConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"            # -> route based on pattern matching
    routing_key = 'twitter.outbound'    # -> overriden in publish method
    durable = False                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 1                   # -> do not save to disk
    
    def __init__(self, publisher):
        self.publisher = publisher
    
    def consume_json(self, dictionary):
        log.msg("Consumed JSON %s" % dictionary)
        # twitter.send(dictionary.get('to'), dictionary.get('message'))
    

class TwitterPublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"            # -> route based on pattern matching
    routing_key = 'twitter.inbound'     # -> overriden in publish method
    durable = False                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 1                   # -> do not save to disk
    
    def publish_json(self, dictionary):
        log.msg("Publishing JSON %s" % dictionary)
        super(TwitterPublisher, self).publish_json(dictionary)
    

class TwitterTransport(Worker):
    
    # inlineCallbacks, TwistedMatrix's fancy way of allowing you to write
    # asynchronous code as if it was synchronous by the nifty use of
    # coroutines.
    # See: http://twistedmatrix.com/documents/10.0.0/api/twisted.internet.defer.html#inlineCallbacks
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the TwitterTransport config: %s" % self.config)
        
        username = self.config.get('username') or raw_input('Username: ')
        password = self.config.get('password') or getpass('Password: ')
        terms = self.config.get('terms') or raw_input('Track terms: ').split()
        
        # create the publisher
        self.publisher = yield self.start_publisher(TwitterPublisher)
        # when it's done, create the consumer and pass it the publisher
        self.consumer = yield self.start_consumer(TwitterConsumer, self.publisher)
        # publish something into the queue for the consumer to pick up.
        self.stream = yield twitter.TwitterFeed(username, password). \
                                track(self.handle_status, terms). \
                                addErrback(log.err)
        
    def status_part_to_dict(self, part, keys=[]):
        return dict([(key, getattr(part,key)) for key in keys if hasattr(part, key)])
    
    def handle_status(self, status):
        data = self.status_part_to_dict(status, ['geo','text', 'created_at'])
        data['user'] = self.status_part_to_dict(status.user, [
            'id',  
            'followers_count', 
            'statuses_count',
            'friends_count',
            'location',
            'name',
            'screen_name',
            'url',
            'time_zone',
        ])
        self.publisher.publish_json(data)
    
    def stopWorker(self):
        log.msg("Stopping the TwitterTransport")
    

