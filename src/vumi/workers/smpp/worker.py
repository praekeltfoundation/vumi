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
    queue_name = "" # overwritten by subclass
    routing_key = "" # overwritten by subclass
    
    
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
                            ("to_msisdn", str(dictionary.get('destination_addr'))),
                            ("from_msisdn", str(dictionary.get('source_addr'))),
                            ("message", str(dictionary.get('short_message')))
                            ]
                    url, resp = utils.callback(url, params)
                    log.msg('RESP: %s' % resp)
                except Exception, e:
                    log.err(e)
        
        except User.DoesNotExist:
            log.msg("Couldn't find user for message: %s" % message)
        log.msg("DELIVER SM %s consumed by %s" % (json.dumps(dictionary),self.__class__.__name__))
    

class FallbackSMSKeywordConsumer(SMSKeywordConsumer):
    routing_key = 'sms.fallback'

def dynamically_create_consumer(name,**kwargs):
    return type("%s_SMSKeywordConsumer" % name, (SMSKeywordConsumer,), kwargs)

class SMSKeywordWorker(Worker):
    """
    A worker that fires off URLCallback's for incoming SMSs
    with keywords
    """
    
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the SMSKeywordWorkers for: %s" % self.config.get('networks'))
        for network,msisdn in self.config.get('networks').items():
            yield self.start_consumer(dynamically_create_consumer(network, 
                routing_key='sms.%s' % msisdn,
                queue_name='sms.keywords.%s' % network.lower()
            ))
        yield self.start_consumer(FallbackSMSKeywordConsumer)
    
    def stopWorker(self):
        log.msg("Stopping the SMSKeywordWorker")


class SMSReceiptConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    delivery_mode = 2
    queue_name = "" # overwritten by subclass
    routing_key = "" # overwritten by subclass
    
    
    def consume_json(self, dictionary):
        log.msg("RECEIPT SM %s consumed by %s" % (json.dumps(dictionary),self.__class__.__name__))
    

class FallbackSMSReceiptConsumer(SMSReceiptConsumer):
    routing_key = 'receipt.fallback'

def dynamically_create_consumer(name,**kwargs):
    return type("%s_SMSReceiptConsumer" % name, (SMSReceiptConsumer,), kwargs)

class SMSReceiptWorker(Worker):
    """
    A worker that fires off URLCallback's for incoming Receipts
    """
    
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the SMSReceiptWorkers for: %s" % self.config.get('OPERATOR_NUMBER'))
        for network,msisdn in self.config.get('OPERATOR_NUMBER').items():
            if len(msisdn):
                yield self.start_consumer(dynamically_create_consumer(network, 
                    routing_key='receipt.%s' % msisdn,
                    queue_name='receipt.%s' % network.lower()
                ))
        yield self.start_consumer(FallbackSMSReceiptConsumer)
    
    def stopWorker(self):
        log.msg("Stopping the SMSReceiptWorker")
    
