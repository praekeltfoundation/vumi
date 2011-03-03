from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue
from django.contrib.auth.models import User

from vumi.service import Worker, Consumer, Publisher
from vumi.webapp.api import utils

import json

from vumi.webapp.api import models


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
                            ("callback_name", "sms_received"),
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
        for network,msisdn in self.config.get('OPERATOR_NUMBER').items():
            if len(msisdn):
                yield self.start_consumer(dynamically_create_keyword_consumer(network,
                    routing_key='sms.%s' % msisdn,
                    queue_name='sms.keywords.%s' % network.lower()
                ))
        yield self.start_consumer(FallbackSMSKeywordConsumer)

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


    def consume_json(self, dictionary):
        _id = kwargs['delivery_report']['id']
        if len(_id):
            resp = models.SMPPResp.objects.get(message_id=_id)
            sent = resp.sent_sms
            log.msg("""
                    id: %s
                    transport_status: %s
                    transport_status_display: %s
                    created_at: %s
                    updated_at: %s
                    delivered_at: %s
                    from_msisdn: %s
                    to_msisdn: %s
                    message: %s
                    """ % (
                        sent.id,
                        kwargs['delivery_report']['stat'],
                        kwargs['delivery_report']['stat'],
                        sent.created_at,
                        sent.updated_at,
                        time.strftime(
                            "%Y-%m-%d %H:%M:%S",
                            time.strptime(
                                "20"+kwargs['delivery_report']['done_date'],
                                "%Y%m%d%H%M%S"
                                )
                            ),
                        kwargs['destination_addr'],
                        sent.to_msisdn,
                        sent.message
                        ))
        log.msg("RECEIPT SM %s consumed by %s" % (json.dumps(dictionary),self.__class__.__name__))


class FallbackSMSReceiptConsumer(SMSReceiptConsumer):
    routing_key = 'receipt.fallback'


def dynamically_create_reciept_consumer(name,**kwargs):
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
                yield self.start_consumer(dynamically_create_receipt_consumer(network,
                    routing_key='receipt.%s' % msisdn,
                    queue_name='receipt.%s' % network.lower()
                ))
        yield self.start_consumer(FallbackSMSReceiptConsumer)

    def stopWorker(self):
        log.msg("Stopping the SMSReceiptWorker")


#==================================================================================================

class SMSBatchConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    delivery_mode = 2
    queue_name = "batch"
    routing_key = "batch"

    def __init__(self, publisher):
        self.publisher = publisher

    def consume_json(self, dictionary):
        log.msg("SM BATCH %s consumed by %s" % (json.dumps(dictionary),self.__class__.__name__))
        payload = []
        kwargs = dictionary.get('kwargs')
        if kwargs:
            pk = kwargs.get('pk')
            for o in models.SentSMS.objects.filter(send_group=pk):
                mess = {
                        'transport_name':o.transport_name,
                        'send_group':o.send_group_id,
                        'from_msisdn':o.from_msisdn,
                        'user':o.user_id,
                        'to_msisdn':o.to_msisdn,
                        'message':o.message,
                        'id':o.id
                        }
                print ">>>>", json.dumps(mess)
                self.publisher.publish_json(mess)
                #reactor.callLater(0, self.publisher.publish_json, mess)
        return True

    def consume(self, message):
        if self.consume_json(json.loads(message.content.body)):
            self.ack(message)


class FallbackSMSBatchConsumer(SMSBatchConsumer):
    routing_key = 'batch.fallback'


class IndivPublisher(Publisher):
    """
    This publisher publishes all incoming SMPP messages to the
    `vumi.smpp` exchange, its default routing key is `smpp.fallback`
    """
    exchange_name = "vumi"
    exchange_type = "direct"
    routing_key = "sms.indiv"
    durable = True
    auto_delete = False
    delivery_mode = 2

    def publish_json(self, dictionary, **kwargs):
        log.msg("Publishing JSON %s with extra args: %s" % (dictionary, kwargs))
        super(IndivPublisher, self).publish_json(dictionary, **kwargs)


class SMSBatchWorker(Worker):
    """
    A worker that breaks up batches of sms's into individual sms's
    """

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the SMSBatchWorker")
        self.publisher = yield self.start_publisher(IndivPublisher)
        yield self.start_consumer(SMSBatchConsumer, self.publisher)
        yield self.start_consumer(FallbackSMSBatchConsumer)

    def stopWorker(self):
        log.msg("Stopping the SMSBatchWorker")

