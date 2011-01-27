from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.service import Worker, Consumer, Publisher
from vumi.workers.smpp.client import EsmeTransceiverFactory, EsmeTransceiver

import json

import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'vumi.webapp.settings'
from vumi.webapp.api import models
from vumi.webapp.api import forms


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
    queue_name = "sms_send"
    routing_key = "vumi.*"

    def __init__(self, send_callback):
        self.send = send_callback

    def consume_json(self, dictionary):
        log.msg("Consumed JSON %s" % dictionary)
        payload = []
        kwargs = dictionary.get('kwargs')
        if kwargs:
            payload = kwargs.get('payload', [])
        for mess in payload:
            sequence_number = self.send(**mess)
            formdict = {
                    "sent_sms":mess.get("id"),
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
        factory = EsmeTransceiverFactory()
        factory.loadDefaults(self.config)
        factory.setSequenceNumber(self.getLatestSequenceNumber())
        factory.setConnectCallback(self.esme_connected)
        factory.setDisconnectCallback(self.esme_disconnected)
        factory.setSubmitSMRespCallback(self.submit_sm_resp)
        factory.setDeliverSMCallback(self.deliver_sm)
        reactor.connectTCP(
                factory.defaults['host'],
                factory.defaults['port'],
                factory)


    def getLatestSequenceNumber(self):
        sequence_number = 1
        try:
            sequence_number = 1 + models.SMPPLink.objects.latest().sequence_number
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
        form = form = forms.SendGroupForm({'title':'reply', 'user':1})
        if not form.is_valid():
            raise FormValidationError(form)
        send_group = form.save()
        formdict = {
                'transport_name': 'smpp',
                'from_msisdn': u'27123456789',
                'send_group': send_group.id,
                'user': 1,
                'to_msisdn': kwargs['source_addr'],
                'message': 'You said: "'+kwargs['short_message']+'"'
                }
        form = forms.SentSMSForm(formdict)
        if not form.is_valid():
            raise FormValidationError(form)
        sent_sms = form.save()
        sequence_number = self.send_smpp(
                sent_sms.id,
                sent_sms.to_msisdn,
                sent_sms.message
                )
        formdict = {
                "sent_sms":sent_sms.id,
                "sequence_number": sequence_number,
                }
        log.msg("SMPPLinkForm <- %s" % formdict)
        form = forms.SMPPLinkForm(formdict)
        form.save()


    def send_smpp(self, id, to_msisdn, message, *args, **kwargs):
        print "Sending SMPP, to: %s, message: %s" % (to_msisdn, message)
        sequence_number = self.esme_client.submit_sm(
                short_message = str(message),
                destination_addr = str(to_msisdn),
                )
        return sequence_number


    def sms_callback(self, *args, **kwargs):
        print "Got SMS:", args, kwargs

    def errback(self, *args, **kwargs):
        print "Got Error: ", args, kwargs

    def stopWorker(self):
        log.msg("Stopping the SMPPTransport")

