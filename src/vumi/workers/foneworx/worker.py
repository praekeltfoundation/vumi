from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.service import Worker, Consumer, Publisher


class FoneworxConsumer(Consumer):
    exchange_name = "vumi.sms"
    exchange_type = "topic"
    durable = False
    queue_name = "sms.foneworx.test_campaign"
    routing_key = "sms.foneworx.test_campaign"
    
    def __init__(self, publisher):
        self.publisher = publisher
        self.storage = []
    
    def consume_json(self, dictionary):
        log.msg("Consumed JSON %s" % dictionary)
        response = {
            'msisdn': dictionary['msisdn'],
            'message': "You said: %s" % dictionary['message']
        }
        self.publisher.publish_json(response)
    

class FoneworxPublisher(Publisher):
    exchange_name = "vumi.sms"
    exchange_type = "topic"             # -> route based on pattern matching
    routing_key = 'sms.foneworx.test_campaign'
    durable = False                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 2                   # -> do not save to disk
    
    def publish_json(self, dictionary, **kwargs):
        log.msg("Publishing JSON %s with extra args: %s" % (dictionary, kwargs))
        super(FoneworxPublisher, self).publish_json(dictionary, **kwargs)
    

class SMSWorker(Worker):
    
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the SMSWorker")
        self.publisher = yield self.start_publisher(FoneworxPublisher)
        self.consumer = yield self.start_consumer(FoneworxConsumer, self.publisher)
    
    def stopWorker(self):
        log.msg("Stopping the SMSWorker")
    


