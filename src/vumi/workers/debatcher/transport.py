from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.service import Worker, Consumer, Publisher

import json

#import os
#os.environ['DJANGO_SETTINGS_MODULE'] = 'vumi.webapp.settings'
from vumi.webapp.api import models
#from vumi.webapp.api import forms


class BatchConsumer(Consumer):
    """
    This consumer creates the generic outbound SMPP transport.
    Anything published to the `vumi.smpp` exchange with
    routing key smpp.* (* == single word match, # == zero or more words)
    """
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    auto_delete = False
    queue_name = "sms_send"
    routing_key = "vumi.*"

    def __init__(self, publisher):
        self.publisher = publisher

    def consume_json(self, dictionary):
        log.msg("Consumed JSON %s" % dictionary)
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


class IndivPublisher(Publisher):
    """
    This publisher publishes all incoming SMPP messages to the
    `vumi.smpp` exchange, its default routing key is `smpp.fallback`
    """
    exchange_name = "vumi"
    exchange_type = "direct"
    routing_key = "vumi.webapp.sms.receipt"
    durable = True
    auto_delete = False
    delivery_mode = 2

    def publish_json(self, dictionary, **kwargs):
        log.msg("Publishing JSON %s with extra args: %s" % (dictionary, kwargs))
        super(IndivPublisher, self).publish_json(dictionary, **kwargs)


class DebatchTransport(Worker):
    """
    The DebatchTransport
    """

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the DebatchTransport config: %s" % self.config)
        self.publisher = yield self.start_publisher(IndivPublisher)
        self.consumer = yield self.start_consumer(BatchConsumer, self.publisher)

    def errback(self, *args, **kwargs):
        print "Got Error: ", args, kwargs

    def stopWorker(self):
        log.msg("Stopping the DebatchTransport")

