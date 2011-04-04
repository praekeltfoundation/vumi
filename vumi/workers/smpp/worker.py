from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.service import Worker, Consumer, Publisher
from vumi.message import Message
from vumi.webapp.api import utils
from vumi.webapp.api.models import Keyword, SentSMS, SMPPResp

import json
import time
from datetime import datetime

from vumi.webapp.api import models


class SMSKeywordConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    delivery_mode = 2
    queue_name = "" # overwritten by subclass
    routing_key = "" # overwritten by subclass


    def consume_message(self, message):
        dictionary = message.payload
        message = dictionary.get('short_message')
        head = message.split(' ')[0]
        try:
            keyword = Keyword.objects.get(keyword=head.lower())
            user = keyword.user
            
            received_sms = models.ReceivedSMS()
            received_sms.user = user
            received_sms.to_msisdn = dictionary.get('destination_addr')
            received_sms.from_msisdn = dictionary.get('source_addr')
            received_sms.message = dictionary.get('short_message')
            
            # FIXME: this is hacky
            received_sms.transport_name = self.queue_name.split('.')[-1]
            # FIXME: EsmeTransceiver doesn't publish these over JSON / AMQP
            # received_sms.transport_msg_id = ...
            # FIXME: this isn't accurate, we might receive it much earlier than
            #        we save it because it could be queued / backlogged.
            received_sms.received_at = datetime.now()
            # FIXME: this is where the fun begins, guessing charsets.
            # received_sms.charset = ...
            received_sms.save()
            
            profile = user.get_profile()
            urlcallback_set = profile.urlcallback_set.filter(name='sms_received')
            for urlcallback in urlcallback_set:
                try:
                    url = urlcallback.url
                    log.msg('URL: %s' % urlcallback.url)
                    params = [
                            ("callback_name", "sms_received"),
                            ("to_msisdn", str(dictionary.get('destination_addr'))),
                            ("from_msisdn", str(dictionary.get('source_addr'))),
                            ("message", str(dictionary.get('short_message')))
                            ]
                    url, resp = utils.callback(url, params)
                    log.msg('RESP: %s' % resp)
                except Exception, e:
                    log.err(e)
            
        except Keyword.DoesNotExist:
            log.msg("Couldn't find keyword for message: %s" % message)
        log.msg("DELIVER SM %s consumed by %s" % (json.dumps(dictionary),self.__class__.__name__))


def dynamically_create_keyword_consumer(name,**kwargs):
    return type("%s_SMSKeywordConsumer" % name, (SMSKeywordConsumer,), kwargs)


class SMSKeywordWorker(Worker):
    """
    A worker that fires off URLCallback's for incoming SMSs
    with keywords
    """

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the SMSKeywordWorkers for: %s" % self.config.get('OPERATOR_NUMBER'))
        transport = self.config.get('TRANSPORT_NAME', 'fallback').lower()
        for network,msisdn in self.config.get('OPERATOR_NUMBER').items():
            if len(msisdn):
                yield self.start_consumer(dynamically_create_keyword_consumer(network,
                    routing_key='sms.inbound.%s.%s' % (transport, msisdn),
                    queue_name='sms.inbound.%s.%s' % (transport, msisdn)
                ))

    def stopWorker(self):
        log.msg("Stopping the SMSKeywordWorker")


#==================================================================================================

class SMSReceiptConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    delivery_mode = 2
    queue_name = "" # overwritten by subclass
    routing_key = "" # overwritten by subclass


    def consume_message(self, message):
        dictionary = message.payload
        log.msg("Consuming message:", message)
        status = dictionary['transport_status']
        transport_name = dictionary['transport_name']
        delivered_at = dictionary['transport_delivered_at']
        message_id = dictionary['transport_msg_id']
        
        try:
            # update sent sms objects
            smpp_link = SMPPResp.objects.filter(message_id=message_id,
                    sent_sms__transport_name=transport_name).latest('created_at')
            sent_sms = smpp_link.sent_sms
            sent_sms.transport_status=status
            sent_sms.transport_msg_id=message_id
            sent_sms.delivered_at=delivered_at
            sent_sms.save() 
            user = sent_sms.user
            
            profile = user.get_profile()
            urlcallback_set = profile.urlcallback_set.filter(name='sms_receipt')
            for urlcallback in urlcallback_set:
                try:
                    url = urlcallback.url
                    log.msg('URL: %s' % urlcallback.url)
                    params = [
                            ("callback_name", "sms_receipt"),
                            ("id", sent_sms.pk),
                            ("transport_status", sent_sms.transport_status),
                            ("transport_status_display",
                                sent_sms.transport_status_display()),
                            ("created_at", sent_sms.created_at),
                            ("updated_at", sent_sms.updated_at),
                            ("delivered_at", sent_sms.delivered_at), 
                            ("from_msisdn", sent_sms.from_msisdn),
                            ("to_msisdn", sent_sms.to_msisdn),
                            ("message", sent_sms.message),
                            ]
                    url, resp = utils.callback(url, params)
                    log.msg('RESP: %s' % resp)
                except Exception, e:
                    log.err(e)
        except SMPPResp.DoesNotExist, e:
            log.err()
        except Exception, e:
            log.err()
        log.msg("RECEIPT SM %s consumed by %s" % (repr(dictionary),self.__class__.__name__))
        return True
 

def dynamically_create_receipt_consumer(name,**kwargs):
    log.msg("Dynamically creating consumer for %s with %s" % (name,
        repr(kwargs)))
    return type("%s_SMSReceiptConsumer" % name, (SMSReceiptConsumer,), kwargs)


class SMSReceiptWorker(Worker):
    """
    A worker that fires off URLCallback's for incoming Receipts
    """

    @inlineCallbacks
    def startWorker(self):
        transport = self.config.get('TRANSPORT_NAME', 'fallback').lower()
        log.msg("Starting the SMSReceiptWorkers for: %s" % transport) 
        yield self.start_consumer(dynamically_create_receipt_consumer(transport,
                    routing_key='sms.receipt.%s' % transport,
                    queue_name='sms.receipt.%s' % transport
                ))
    

    def stopWorker(self):
        log.msg("Stopping the SMSReceiptWorker")


#==================================================================================================

class SMSBatchConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    delivery_mode = 2
    queue_name = "sms.internal.debatcher"
    routing_key = "sms.internal.debatcher"

    def __init__(self, publisher):
        self.publisher = publisher

    def consume_message(self, message):
        dictionary = message.payload
        log.msg("SM BATCH %s consumed by %s" % (json.dumps(dictionary),self.__class__.__name__))
        payload = []
        kwargs = dictionary.get('kwargs')
        if kwargs:
            pk = kwargs.get('pk')
            for o in models.SentSMS.objects.filter(batch=pk):
                mess = {
                        'transport_name':o.transport_name,
                        'batch':o.batch_id,
                        'from_msisdn':o.from_msisdn,
                        'user':o.user_id,
                        'to_msisdn':o.to_msisdn,
                        'message':o.message,
                        'id':o.id
                        }
                print ">>>>", json.dumps(mess)
                self.publisher.publish_message(Message(**mess))
                #reactor.callLater(0, self.publisher.publish_json, mess)
        return True

    def consume(self, message):
        if self.consume_message(Message.from_json(message.content.body)):
            self.ack(message)


#class FallbackSMSBatchConsumer(SMSBatchConsumer):
    #routing_key = 'batch.fallback'


class IndivPublisher(Publisher):
    """
    This publisher publishes all incoming SMPP messages to the
    `vumi.smpp` exchange, its default routing key is `smpp.fallback`
    """
    exchange_name = "vumi"
    exchange_type = "direct"
    routing_key = "sms.outbound.fallback"
    durable = True
    auto_delete = False
    delivery_mode = 2

    def publish_message(self, message, **kwargs):
        dictionary = message.payload
        transport = str(dictionary.get('transport_name', 'fallback')).lower()
        routing_key = 'sms.outbound.' + transport
        kwargs.update({'routing_key':routing_key})
        log.msg("Publishing Message %s with extra args: %s" % (message, kwargs))
        super(IndivPublisher, self).publish_message(message, **kwargs)


class SMSBatchWorker(Worker):
    """
    A worker that breaks up batches of sms's into individual sms's
    """

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the SMSBatchWorker")
        self.publisher = yield self.start_publisher(IndivPublisher)
        yield self.start_consumer(SMSBatchConsumer, self.publisher)
        #yield self.start_consumer(FallbackSMSBatchConsumer)

    def stopWorker(self):
        log.msg("Stopping the SMSBatchWorker")

