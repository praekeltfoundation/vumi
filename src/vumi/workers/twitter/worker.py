from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from getpass import getpass

from vumi.service import Worker, Consumer, Publisher

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
    

