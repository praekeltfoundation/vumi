from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue
from django.contrib.auth.models import User

from vumi.service import Worker, Consumer, Publisher
from vumi.webapp.api import utils

import json

class SMSKeywordConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    delivery_mode = 2
    queue_name = "sms.clickatell.keywords"
    
    def consume_json(self, dictionary):
        message = dictionary.get('short_message')
        head = message.split(' ')[0]
        
        try:
            user = User.objects.get(username=head)
            profile = user.get_profile()
            urlcallback_set = profile.urlcallback_set.filter(name='sms_received')
            for urlcallback in urlcallback_set:
                try:
                    url = urlcallback.url
                    log.msg('URL: %s' % urlcallback.url)
                    params = [
                            ("route", str(dictionary.get('destination_addr'))),
                            ("msisdn", str(dictionary.get('source_addr'))),
                            ("message", str(dictionary.get('short_message'))),
                            ("json",
                                '{"route":"%s", "msisdn":"%s", "message":"%s"}' % (
                                str(dictionary.get('destination_addr')),
                                str(dictionary.get('source_addr')),
                                str(dictionary.get('short_message'))))
                            ]
                    url, resp = utils.callback(url, params)
                    log.msg('RESP: %s' % resp)
                except Exception, e:
                    log.err(e)
        
        except User.DoesNotExist:
            log.msg("Couldn't find user for message: %s" % message)
        log.msg("DELIVER SM %s" % (json.dumps(dictionary)))
    

class VodacomSMSKeywordConsumer(SMSKeywordConsumer):
    routing_key = "sms.278200702230015"
    

class MTNSMSKeywordConsumer(SMSKeywordConsumer):
    routing_key = 'sms.278326451590115'

class CellCSMSKeywordConsumer(SMSKeywordConsumer):
    routing_key = 'sms.278400317700115'

class FallbackSMSKeywordConsumer(SMSKeywordConsumer):
    routing_key = 'sms.fallback'

class SMSKeywordWorker(Worker):
    """
    A worker that fires off URLCallback's for incoming SMSs
    with keywords
    """
    
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the SMSKeywordWorker, config: %s" % self.config)
        for consumer_class in [
            VodacomSMSKeywordConsumer, 
            MTNSMSKeywordConsumer, 
            CellCSMSKeywordConsumer,
            FallbackSMSKeywordConsumer]:
            yield self.start_consumer(consumer_class)
    
    def stopWorker(self):
        log.msg("Stopping the SMSKeywordWorker")
    