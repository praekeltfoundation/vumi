from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.service import Worker, Consumer, Publisher
from vumi.workers.truteq.util import VumiSSMIFactory, SessionType, ussd_code_to_routing_key


class TruTeqConsumer(Consumer):
    """
    This consumer consumes all incoming USSD messages on the *120*663*79#
    shortcode in a campaign specific queue.
    """
    exchange_name = "vumi.ussd"
    exchange_type = "topic"
    durable = False
    queue_name = "ussd.truteq.test_campaign"
    routing_key = "ussd.s120s663s79h"
    
    def __init__(self, publisher):
        self.publisher = publisher
    
    def consume_json(self, dictionary):
        log.msg("Consumed JSON %s" % dictionary)
        ussd_type = dictionary.get('ussd_type')
        msisdn = dictionary.get('msisdn')
        message = dictionary.get('message')
        if(ussd_type == SessionType.NEW):
            response = {
                'ussd_type': SessionType.EXISTING,
                'msisdn': msisdn,
                'message': "Hi! This is an echo service"
            }
        else:
            response = {
                'ussd_type': SessionType.END,
                'msisdn': msisdn,
                'message': 'You said: %s' % message
            }
        self.publisher.publish_json(response)
    

class TruTeqPublisher(Publisher):
    exchange_name = "vumi.ussd"
    exchange_type = "topic"             # -> route based on pattern matching
    routing_key = 'ussd.truteq.s120s663s79h'
    durable = False                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 2                   # -> do not save to disk
    
    def publish_json(self, dictionary, **kwargs):
        log.msg("Publishing JSON %s with extra args: %s" % (dictionary, kwargs))
        super(TruTeqPublisher, self).publish_json(dictionary, **kwargs)
    

class USSDWorker(Worker):
    
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the USSDWorker")
        self.publisher = yield self.start_publisher(TruTeqPublisher)
        self.consumer = yield self.start_consumer(TruTeqConsumer, self.publisher)
    
    def stopWorker(self):
        log.msg("Stopping the USSDWorker")
    


