from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.service import Worker, Consumer, Publisher
from vumi.message import Message

class XMPPConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"             # -> route based on pattern matching
    queue_name = 'xmpp.gtalk.inbound'
    routing_key = 'xmpp.gtalk.inbound'
    durable = True                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 2                   # -> do save to disk
    
    def __init__(self, publisher):
        self.publisher = publisher
    
    def consume_message(self, message):
        recipient = message.payload['sender']
        message = "You said: %s " % message.payload['message']
        self.publisher.publish_message(Message(recipient=recipient, message=message))
    

class XMPPPublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    routing_key = "xmpp.gtalk.outbound"
    

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
        self.consumer = yield self.start_consumer(XMPPConsumer, self.publisher)
    
    def stopWorker(self):
        log.msg("Stopping the XMPPWorker")
    

