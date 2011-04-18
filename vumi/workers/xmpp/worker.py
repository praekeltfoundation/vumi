from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.service import Worker, Publisher
from vumi.message import Message

class XMPPPublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    routing_key = "xmpp.outbound.gtalk"
    

class XMPPWorker(Worker):
    
    # inlineCallbacks, TwistedMatrix's fancy way of allowing you to write
    # asynchronous code as if it was synchronous by the nifty use of
    # coroutines.
    # See: http://twistedmatrix.com/documents/10.0.0/api/twisted.internet.defer.html#inlineCallbacks
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the XMPPWorker config: %s" % self.config)
        # create the publisher
        self.publisher = yield self.start_publisher(XMPPPublisher)
        # when it's done, create the consumer and pass it the publisher
        # self.consumer = yield self.start_consumer(XMPPConsumer, self.publisher)
        self.consume("xmpp.inbound.gtalk.%s" % self.config['username'], 
                        self.consume_message)
    
    def consume_message(self, message):
        recipient = message.payload['sender']
        message = "You said: %s " % message.payload['message']
        self.publisher.publish_message(Message(recipient=recipient, message=message), 
            routing_key = "%s.%s" % (self.publisher.routing_key, self.config['username'])
        )
    
    def stopWorker(self):
        log.msg("Stopping the XMPPWorker")
    

