from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.service import Worker, Consumer, Publisher
from vumi.message import Message, VUMI_DATE_FORMAT
from vumi.webapp.api import utils
from vumi.webapp.api.models import Keyword, SentSMS, Transport

import json
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
            received_sms.transport_name = self.queue_name.split('.')[-2]
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
                    log.msg('RESP: %s' % repr(resp))
                except Exception, e:
                    log.err(e)
            
        except Keyword.DoesNotExist:
            log.msg("Couldn't find keyword for message: %s" % message)
        log.msg("DELIVER SM %s consumed by %s" % (json.dumps(dictionary),self.__class__.__name__))
        return True


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
        for network,msisdn in sorted(self.config.get('OPERATOR_NUMBER').items()):
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

    
    def find_sent_sms(self, transport_name, message_id):
        return SentSMS.objects.get(
                transport_name__iexact=transport_name,
                transport_msg_id=message_id)


    def consume_message(self, message):
        dictionary = message.payload
        log.msg("Consuming message:", message)
        delivery_status = dictionary['delivery_status']
        transport = dictionary['transport']
        timestamp = dictionary['timestamp']
        transport_message_id = dictionary['transport_message_id']
        
        try:
            sent_sms = self.find_sent_sms(transport, transport_message_id)
            log.msg('Processing receipt for', sent_sms, dictionary)

            if sent_sms.transport_status == delivery_status:
                log.msg("Received duplicate receipt for", sent_sms, dictionary)
            else:
                sent_sms.transport_status=delivery_status
                sent_sms.transport_msg_id=transport_message_id
                sent_sms.delivered_at=timestamp
                sent_sms.save() 
                user = sent_sms.user
                
                profile = user.get_profile()
                urlcallback_set = profile.urlcallback_set.filter(name='sms_receipt')
                for urlcallback in urlcallback_set:
                    try:
                        url = urlcallback.url
                        params = [
                                ("callback_name", "sms_receipt"),
                                ("id", str(sent_sms.pk)),
                                ("transport_status", sent_sms.transport_status),
                                ("created_at",
                                    sent_sms.created_at.strftime(VUMI_DATE_FORMAT)),
                                ("updated_at",
                                    sent_sms.updated_at.strftime(VUMI_DATE_FORMAT)),
                                ("delivered_at",
                                    sent_sms.delivered_at.strftime(VUMI_DATE_FORMAT)), 
                                ("from_msisdn", sent_sms.from_msisdn),
                                ("to_msisdn", sent_sms.to_msisdn),
                                ("message", sent_sms.message),
                                ]
                        log.msg(utils.callback)
                        url, resp = utils.callback(url, params)
                        log.msg('RESP: %s' % resp)
                    except Exception, e:
                        log.err(e)
        except Exception, e:
            log.err()
        log.msg("RECEIPT SM %s consumed by %s" % (repr(dictionary),self.__class__.__name__))
 

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
        for transport in Transport.objects.all():
            log.msg("Starting the SMSReceiptWorkers for: %s" % transport.name) 
            yield self.start_consumer(
                    dynamically_create_receipt_consumer(str(transport.name),
                            routing_key='sms.receipt.%s' % transport.name.lower(),
                            queue_name='sms.receipt.%s' % transport.name.lower()
                        ))
        

    def stopWorker(self):
        log.msg("Stopping the SMSReceiptWorker")


#==================================================================================================

class SMSAckConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    delivery_mode = 2
    queue_name = "" # overwritten by subclass
    routing_key = "" # overwritten by subclass


    def consume_message(self, message):
        dictionary = message.payload
        log.msg("Consuming message:", message)
        id = dictionary['message_id']
        transport_message_id = dictionary['transport_message_id']
        try:
            sent_sms = SentSMS.objects.get(id=id)
            log.msg('Processing ack for', sent_sms, dictionary)
            sent_sms.transport_msg_id=transport_message_id
            sent_sms.save() 
        except Exception:
            log.err()
        log.msg("Message Ack %s consumed by %s" % (repr(dictionary),self.__class__.__name__))


def dynamically_create_ack_consumer(name,**kwargs):
    log.msg("Dynamically creating ack consumer for %s with %s" % (name,
        repr(kwargs)))
    return type("%s_SMSAckConsumer" % name, (SMSAckConsumer,), kwargs)

class SMSAckWorker(Worker):
    """
    A worker that writes all ack's to the database
    """

    @inlineCallbacks
    def startWorker(self):
        for transport in Transport.objects.all():
            log.msg("Starting the SMSAckWorkers for: %s" % transport.name) 
            yield self.start_consumer(
                    dynamically_create_ack_consumer(str(transport.name),
                            routing_key='sms.ack.%s' % transport.name.lower(),
                            queue_name='sms.ack.%s' % transport.name.lower()
                        ))
        

    def stopWorker(self):
        log.msg("Stopping the SMSAckWorker")


#==================================================================================================

class SMSInboundConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    delivery_mode = 2
    queue_name = "" # overwritten by subclass
    routing_key = "" # overwritten by subclass


    def consume_message(self, message):
        dictionary = message.payload
        log.msg("Consuming message:", message)
        log.msg("Inbound Message %s consumed by %s" % (repr(dictionary),self.__class__.__name__))


def dynamically_create_inbound_consumer(name,**kwargs):
    log.msg("Dynamically creating inbound consumer for %s with %s" % (name,
        repr(kwargs)))
    return type("%s_SMSInboundConsumer" % name, (SMSInboundConsumer,), kwargs)

class SMSInboundWorker(Worker):
    """
    A worker that writes all inbound SMS's to the database
    """

    @inlineCallbacks
    def startWorker(self):
        for transport in Transport.objects.all():
            log.msg("Starting the SMSInboundWorkers for: %s" % transport.name) 
            yield self.start_consumer(
                    dynamically_create_inbound_consumer(str(transport.name),
                            routing_key='sms.inbound.%s' % transport.name.lower(),
                            queue_name='sms.inbound.%s' % transport.name.lower()
                        ))
        

    def stopWorker(self):
        log.msg("Stopping the SMSInboundWorker")


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
                self.publisher.publish_message(Message(**mess),
                        routing_key='sms.outbound.%s' % o.transport_name.lower(),
                        require_bind='ANY')

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

