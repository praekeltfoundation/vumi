from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import LoopingCall
from twisted.internet import reactor

from vumi.service import Worker, Consumer, Publisher
from vumi.message import Message
from vumi.workers.smpp.client import EsmeTransceiverFactory, EsmeTransceiver

import json
import re

#import os
#os.environ['DJANGO_SETTINGS_MODULE'] = 'vumi.webapp.settings'
from vumi.webapp.api import models
from vumi.webapp.api import forms
from vumi.webapp.api import utils
from vumi.utils import *

import urllib
import urllib2

from datetime import datetime, timedelta
import time


class SmppConsumer(Consumer):
    """
    This consumer creates the generic outbound SMPP transport.
    Anything published to the `vumi.smpp` exchange with
    routing key smpp.* (* == single word match, # == zero or more words)
    """
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    auto_delete = False
    queue_name = "sms.outbound.fallback"
    routing_key = "sms.outbound.fallback"

    def __init__(self, send_callback):
        self.send = send_callback
        log.msg("Consuming on %s -> %s" % (self.routing_key, self.queue_name))

    def consume_message(self, message):
        log.msg("Consumed JSON", message)
        sequence_number = self.send(**message.payload)
        formdict = {
                "sent_sms":message.payload.get("id"),
                "sequence_number": sequence_number,
                }
        log.msg("SMPPLinkForm", repr(formdict))
        form = forms.SMPPLinkForm(formdict)
        form.save()
        return True

    def consume(self, message):
        if self.consume_message(Message.from_json(message.content.body)):
            self.ack(message)


def dynamically_create_smpp_consumer(name, **kwargs):
    return type("%s_SmppConsumer" % name, (SmppConsumer,), kwargs)


class SmppPublisher(Publisher):
    """
    This publisher publishes all incoming SMPP messages to the
    `vumi.smpp` exchange, its default routing key is `smpp.fallback`
    """
    exchange_name = "vumi"
    exchange_type = "topic"             # -> route based on pattern matching
    routing_key = 'smpp.fallback'       # -> overriden in publish method
    durable = False                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 2                   # -> save to disk

    def publish_message(self, message, **kwargs):
        log.msg("Publishing Message %s with extra args: %s" % (message, kwargs))
        super(SmppPublisher, self).publish_message(message, **kwargs)


class SmppTransport(Worker):
    """
    The SmppTransport
    """

    def startWorker(self):
        log.msg("Starting the SmppTransport")
        # start the Smpp transport
        factory = EsmeTransceiverFactory(
                int(self.config['smpp_increment']),
                int(self.config['smpp_offset']))
        factory.loadDefaults(self.config)
        factory.setLatestSequenceNumber(self.getLatestSequenceNumber())
        factory.setConnectCallback(self.esme_connected)
        factory.setDisconnectCallback(self.esme_disconnected)
        factory.setSubmitSMRespCallback(self.submit_sm_resp)
        factory.setDeliveryReportCallback(self.delivery_report)
        factory.setDeliverSMCallback(self.deliver_sm)
        factory.setLoopingQuerySMCallback(self.query_sm_group)
        log.msg(factory.defaults)
        reactor.connectTCP(
                factory.defaults['host'],
                factory.defaults['port'],
                factory)


    def getLatestSequenceNumber(self):
        sequence_number = 0
        try:
            sequence_number = models.SMPPLink.objects.latest().sequence_number
        except Exception, e:
            log.msg("No SMPPLink entries yet")
        return sequence_number


    @inlineCallbacks
    def esme_connected(self, client):
        log.msg("ESME Connected, adding handlers")
        self.esme_client = client
        self.esme_client.set_handler(self)

        # Start the publisher
        self.publisher = yield self.start_publisher(SmppPublisher)
        # Start the consumer, pass along the send_smpp callback for sending
        # back consumed AMQP messages over SMPP.
        #self.consumer = yield self.start_consumer(SmppConsumer, self.send_smpp)
        transport = self.config.get('TRANSPORT_NAME', 'fallback').lower()
        yield self.start_consumer(dynamically_create_smpp_consumer(transport,
                    routing_key='sms.outbound.%s' % transport,
                    queue_name='sms.outbound.%s' % transport
                ), self.send_smpp)


    @inlineCallbacks
    def esme_disconnected(self):
        log.msg("ESME Disconnected, stopping consumer")
        stop = yield self.consumer.stop()


    @inlineCallbacks
    def submit_sm_resp(self, *args, **kwargs):
        smpplink = models.SMPPLink.objects \
                .filter(sequence_number=kwargs['sequence_number']) \
                .latest('created_at')
        kwargs.update({'sent_sms':smpplink.sent_sms_id})
        log.msg("SMPPRespForm <- %s" % repr(kwargs))
        form = forms.SMPPRespForm(kwargs)
        form.save()
        yield log.msg("SUBMIT SM RESP %s" % repr(kwargs))


    @inlineCallbacks
    def query_sm_group(self, *args, **kwargs):
        try:
            self.second_counter += 1
            if self.second_counter >= 60:
                self.second_counter = 0
        except:
            self.second_counter = 0
        fromdate = datetime.now() - timedelta(days=1)
        smppRespList = models.SMPPResp.objects \
                .filter(created_at__gte=fromdate) \
                .extra(where=['ROUND(EXTRACT(SECOND FROM created_at)) = %d' % (self.second_counter)]) \
                .order_by('-created_at')
        for r in smppRespList:
            route = get_operator_number(
                    r.sent_sms.to_msisdn,
                    self.config['COUNTRY_CODE'],
                    self.config.get('OPERATOR_PREFIX',{}),
                    self.config.get('OPERATOR_NUMBER',{}))
            sequence_number = self.esme_client.query_sm(
                    message_id = r.message_id,
                    source_addr = route
                    )
        yield log.msg("LOOPING QUERY SM" % repr(kwargs))


    @inlineCallbacks
    def delivery_report(self, *args, **kwargs):
        transport_name = self.config.get('TRANSPORT_NAME', 'fallback').lower()
        log.msg("DELIVERY REPORT", kwargs)
        dictionary = {
            'transport_name': transport_name,
            'transport_msg_id': kwargs['delivery_report']['id'],
            'transport_status': kwargs['delivery_report']['stat'],
            'transport_delivered_at': datetime.strptime(
                kwargs['delivery_report']['done_date'],
                "%y%m%d%H%M%S")
        }
        yield self.publisher.publish_message(Message(**dictionary),
            routing_key='sms.receipt.%s' % (transport_name,))


    @inlineCallbacks
    def deliver_sm(self, *args, **kwargs):
        yield self.publisher.publish_message(Message(**kwargs), 
            routing_key='sms.inbound.%s.%s' % (
                self.config.get('TRANSPORT_NAME', 'fallback').lower(),
                kwargs.get('destination_addr')))


    def send_smpp(self, id, to_msisdn, message, *args, **kwargs):
        log.msg("Sending SMPP, to: %s, message: %s" % (to_msisdn, repr(message)))
        route = get_operator_number(to_msisdn,
                self.config['COUNTRY_CODE'],
                self.config.get('OPERATOR_PREFIX',{}),
                self.config.get('OPERATOR_NUMBER',{}))
        sequence_number = self.esme_client.submit_sm(
                short_message = message.encode('utf-8'),
                destination_addr = str(to_msisdn),
                source_addr = route,
                )
        #self.deliver_sm(
                #short_message=str(message),
                #destination_addr=str(to_msisdn),
                #source_addr=route)
        return sequence_number


    def stopWorker(self):
        log.msg("Stopping the SMPPTransport")

