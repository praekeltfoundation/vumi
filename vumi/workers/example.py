from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.service import Worker, Consumer, Publisher
from vumi.message  import Message

class ExampleConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = False
    queue_name = "queue"
    routing_key = "routing_key"
    
    def __init__(self, publisher):
        self.publisher = publisher
    
    def consume_message(self, message):
        log.msg("Consumed Message %s" % message)
        reactor.callLater(1, self.publisher.publish_message, message)
    

class ExamplePublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"
    routing_key = "routing_key"
    durable = False
    auto_delete = False
    delivery_mode = 2 # save to disk
    
    def publish_message(self, message):
        log.msg("Publishing Message %s" % message)
        super(ExamplePublisher, self).publish_message(message)
    

class ExampleWorker(Worker):
    
    # inlineCallbacks, TwistedMatrix's fancy way of allowing you to write
    # asynchronous code as if it was synchronous by the nifty use of
    # coroutines.
    # See: http://twistedmatrix.com/documents/10.0.0/api/twisted.internet.defer.html#inlineCallbacks
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the ExampleWorker config: %s" % self.config)
        # create the publisher
        self.publisher = yield self.start_publisher(ExamplePublisher)
        # when it's done, create the consumer and pass it the publisher
        self.consumer = yield self.start_consumer(ExampleConsumer, self.publisher)
        # publish something into the queue for the consumer to pick up.
        reactor.callLater(0, self.publisher.publish_message,
            Message(hello='world'))
    
    def stopWorker(self):
        log.msg("Stopping the ExampleWorker")
    


