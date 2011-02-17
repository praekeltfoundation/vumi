from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.service import Worker, Consumer, Publisher
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
    queue_name = "sms_receipt"
    routing_key = "vumi.*"

    def __init__(self, send_callback):
        self.send = send_callback

    def consume_json(self, dictionary):
        log.msg("Consumed JSON %s" % dictionary)
        sequence_number = self.send(**dictionary)
        formdict = {
                "sent_sms":dictionary.get("id"),
                "sequence_number": sequence_number,
                }
        log.msg("SMPPLinkForm <- %s" % formdict)
        form = forms.SMPPLinkForm(formdict)
        form.save()
        return True

    def consume(self, message):
        if self.consume_json(json.loads(message.content.body)):
            self.ack(message)


class SmppPublisher(Publisher):
    """
    This publisher publishes all incoming SMPP messages to the
    `vumi.smpp` exchange, its default routing key is `smpp.fallback`
    """
    exchange_name = "vumi.smpp"
    exchange_type = "topic"             # -> route based on pattern matching
    routing_key = 'smpp.fallback'       # -> overriden in publish method
    durable = False                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 2                   # -> do not save to disk

    def publish_json(self, dictionary, **kwargs):
        log.msg("Publishing JSON %s with extra args: %s" % (dictionary, kwargs))
        super(SmppPublisher, self).publish_json(dictionary, **kwargs)


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
        factory.setDeliverSMCallback(self.deliver_sm)
        print factory.defaults
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
        self.consumer = yield self.start_consumer(SmppConsumer, self.send_smpp)


    @inlineCallbacks
    def esme_disconnected(self):
        log.msg("ESME Disconnected, stopping consumer")
        stop = yield self.consumer.stop()


    @inlineCallbacks
    def submit_sm_resp(self, *args, **kwargs):
        smpplink = models.SMPPLink.objects \
                .filter(sequence_number=kwargs['sequence_number']) \
                .order_by('-created_at')[:1].get()
        kwargs.update({'sent_sms':smpplink.sent_sms_id})
        log.msg("SMPPRespForm <- %s" % kwargs)
        form = forms.SMPPRespForm(kwargs)
        form.save()
        yield log.msg("SUBMIT SM RESP %s" % (kwargs))


    @inlineCallbacks
    def deliver_sm(self, *args, **kwargs):
        message = kwargs.get('short_message')
        head = message.split(' ')[0]
        try:
            user = models.User.objects.get(username=head)
        except:
            user= None
        if user:
            profile = user.get_profile()
            urlcallback_set = profile.urlcallback_set.filter(name='sms_received')
            for urlcallback in urlcallback_set:
                try:
                    #url = "http://localhost:8080/"
                    url = urlcallback.url
                    log.msg('URL: %s' % urlcallback.url)
                    params = [("json",
                        '{"route":"%s", "msisdn":"%s", "message":"%s"}' % (
                        kwargs.get('destination_addr'),
                        kwargs.get('source_addr'),
                        kwargs.get('short_message'))
                        )]
                    url, resp = utils.callback(url, params)
                    log.msg('RESP: %s' % resp) 
                    #params = {'json' : '{"route":"%s", "msisdn":"%s", "message":"%s"}' % (
                        #kwargs.get('destination_addr'),
                        #kwargs.get('source_addr'),
                        #kwargs.get('short_message')
                        #)}
                    #data = urllib.urlencode(params)
                    #req = urllib2.Request(url, data)
                    #resp = urllib2.urlopen(req)
                except Exception, e:
                    log.err(e)
        else:
            log.msg("Couldn't find user for message: %s" % message)
        yield log.msg("DELIVER SM %s" % (json.dumps(kwargs)))

    @inlineCallbacks
    def deliver_sm__(self, *args, **kwargs):
        groupdict = {'title':'reply', 'user':1}
        groupform = forms.SendGroupForm(groupdict)
        if not groupform.is_valid():
            raise FormValidationError(groupform)
        send_group = groupform.save()
        sentdict = {
                'transport_name': 'smpp',
                'from_msisdn': u'27123456789',
                'send_group': send_group.id,
                'user': 1,
                'to_msisdn': kwargs['source_addr'],
                'message': 'You said: "'+kwargs['short_message']+'"'
                }
        sentform = forms.SentSMSForm(sentdict)
        if not sentform.is_valid():
            raise FormValidationError(sentform)
        sent_sms = sentform.save()
        sequence_number = self.send_smpp(
                sent_sms.id,
                sent_sms.to_msisdn,
                sent_sms.message
                )
        linkdict = {
                "sent_sms":sent_sms.id,
                "sequence_number": sequence_number,
                }
        log.msg("SMPPLinkForm <- %s" % linkdict)
        linkform = forms.SMPPLinkForm(linkdict)
        linkform.save()
        yield log.msg("DELIVER SM %s" % (sentdict))


    def send_smpp(self, id, to_msisdn, message, *args, **kwargs):
        print "Sending SMPP, to: %s, message: %s" % (to_msisdn, message)
        route = get_operator_number(to_msisdn,
                self.config['COUNTRY_CODE'],
                self.config.get('OPERATOR_PREFIX',{}),
                self.config.get('OPERATOR_NUMBER',{}))
        sequence_number = self.esme_client.submit_sm(
                short_message = str(message),
                destination_addr = str(to_msisdn),
                source_addr = route,
                )
        #self.deliver_sm(
                #short_message=str(message),
                #destination_addr=str(to_msisdn),
                #source_addr=route)
        return sequence_number


    def sms_callback(self, *args, **kwargs):
        print "Got SMS:", args, kwargs

    def errback(self, *args, **kwargs):
        print "Got Error: ", args, kwargs

    def stopWorker(self):
        log.msg("Stopping the SMPPTransport")

