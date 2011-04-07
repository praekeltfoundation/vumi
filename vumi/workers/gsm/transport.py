from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.service import Worker, Consumer, Publisher
from vumi.message import Message

class GsmConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    auto_delete = False
    queue_name = "gsm.gammu.outbound"
    routing_key = "gsm.gammu.outbound"
    
    def __init__(self, callback):
        self.callback = callback
    
    def consume_json(self, dictionary):
        log.msg("Consumed JSON %s" % dictionary)
        self.callback(dictionary)

    

class GsmPublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"
    routing_key = 'gsm.gammu.inbound'
    durable = True
    auto_delete = False
    delivery_mode = 2
    
    def publish_json(self, dictionary):
        log.msg("Publishing JSON %s" % dictionary)
        super(ExamplePublisher, self).publish_json(dictionary)
    

class GsmTransport(Worker):
    
    # inlineCallbacks, TwistedMatrix's fancy way of allowing you to write
    # asynchronous code as if it was synchronous by the nifty use of
    # coroutines.
    # See: http://twistedmatrix.com/documents/10.0.0/api/twisted.internet.defer.html#inlineCallbacks
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the GsmTransport, config: %s" % self.config)
        # create the publisher
        self.publisher = yield self.start_publisher(GsmPublisher)
        
        """
        INSERT MODEM MAGIC HERE
        
        ...
        
        self.modem = SomeGammuModem()
        self.modem.onMessageCallback = self.onInboundMessage
        
        # when it's done, create the consumer and pass it the publisher
        self.consumer = yield self.start_consumer(GsmConsumer, self.onOutboundMessage)
        """
    
    def onInboundMessage(self, sms):
        self.publisher.publish_json({
            'from': sms.get('from'),
            'message': sms.get('text'),
        })
    
    def onOutboundMessage(self, dictionary):
        self.modem.send({
            'to': dictionary.get('to'),
            'message': dictionary.get('message')
        })
    
    def stopWorker(self):
        log.msg("Stopping the ExampleWorker")
    


