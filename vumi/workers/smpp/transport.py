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
import redis


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

    def __init__(self, r_server, r_prefix, smpp_offset, send_callback):
        self.r_server = r_server
        self.r_prefix = r_prefix
        self.smpp_offset = smpp_offset
        self.send = send_callback
        log.msg("Consuming on %s -> %s" % (self.routing_key, self.queue_name))

    def consume_message(self, message):
        log.msg("Consumed JSON", message)
        sequence_number = self.send(**message.payload)
        self.r_server.set("%s_%s#last_sequence_number" % (self.r_prefix, self.smpp_offset),
                sequence_number)
        self.r_server.set("%s#%s" % (self.r_prefix, sequence_number),
                message.payload.get("id"))
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
        # Connect to Redis
        self.r_server = redis.Redis("localhost", db=get_deploy_int(self._amqp_client.vhost))
        log.msg("Connected to Redis")
        self.r_prefix = "%s@%s:%s" % (self.config['system_id'], self.config['host'], self.config['port'])
        log.msg("r_prefix = %s" % self.r_prefix)

        log.msg("Starting the SmppTransport")
        # start the Smpp transport
        factory = EsmeTransceiverFactory(self.config, self._amqp_client.vumi_options)
        factory.loadDefaults(self.config)

        self.sequence_key = "%s_%s#last_sequence_number" % (self.r_prefix, self.config['smpp_offset'])
        log.msg("sequence_key = %s" % (self.sequence_key))
        last_sequence_number = int(self.r_server.get(self.sequence_key) or 0)

        factory.setLatestSequenceNumber(last_sequence_number)
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
                ), self.r_server, self.r_prefix, self.config['smpp_offset'], self.send_smpp)


    @inlineCallbacks
    def esme_disconnected(self):
        log.msg("ESME Disconnected, stopping consumer")
        stop = yield self.consumer.stop()


    @inlineCallbacks
    def submit_sm_resp(self, *args, **kwargs):
        redis_key = "%s#%s" % (self.r_prefix, kwargs['sequence_number'])
        log.msg("redis_key = %s" % redis_key)
        sent_sms_id = self.r_server.get(redis_key)
        self.r_server.delete(redis_key)
        kwargs.update({'sent_sms':sent_sms_id})
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
        # first do a lookup in our YAML to see if we've got a source_addr
        # defined for the given MT number, if not, trust the from_msisdn 
        # in the message
        route = get_operator_number(to_msisdn,
                self.config.get('COUNTRY_CODE',''),
                self.config.get('OPERATOR_PREFIX',{}),
                self.config.get('OPERATOR_NUMBER',{})) or kwargs.get('from_msisdn', '')
        sequence_number = self.esme_client.submit_sm(
                short_message = message.encode('utf-8'),
                destination_addr = str(to_msisdn),
                source_addr = route,
                )
        return sequence_number


    def stopWorker(self):
        log.msg("Stopping the SMPPTransport")

