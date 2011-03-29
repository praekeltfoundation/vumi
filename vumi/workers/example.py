from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.service import Worker, Consumer, Publisher

class ExampleConsumer(Consumer):
    
    def __init__(self, publisher):
        self.publisher = publisher
    
    def consume_json(self, dictionary):
        log.msg("Consumed JSON %s" % dictionary)
        reactor.callLater(1, self.publisher.publish_json, dictionary)
    

class ExamplePublisher(Publisher):
    
    def publish_json(self, dictionary):
        log.msg("Publishing JSON %s" % dictionary)
        super(ExamplePublisher, self).publish_json(dictionary)
    

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
        reactor.callLater(0, self.publisher.publish_json, {'hello': 'world'})
    
    def stopWorker(self):
        log.msg("Stopping the ExampleWorker")
    


