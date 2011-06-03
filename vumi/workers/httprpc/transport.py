# -*- coding: utf-8 -*-
from twisted.python import log
from twisted.web import xmlrpc, http
from twisted.web.resource import Resource
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from datetime import datetime, timedelta
from vumi.webapp.api.models import SentSMS, ReceivedSMS, Keyword
from vumi.webapp.api.gateways.opera import utils
from vumi.utils import safe_routing_key
from vumi.webapp.api import forms
from vumi.message import Message, JSONMessageEncoder
from vumi.service import Worker, Consumer, Publisher
import cgi, json, iso8601, base64, xmlrpclib
from time import sleep
from txamqp.queue import TimeoutDeferredQueue, Closed
from twisted.web.server import NOT_DONE_YET
from twisted.internet.task import deferLater
import uuid
from vumi.message import Message


class HttpRpcResource(Resource):

    def __init__(self, transport):
        self.transport = transport
        self.publisher = transport.publisher
        Resource.__init__(self)


    def render_POST(self, request):
        content = request.content.read()
        log.msg(self.transport, ">>>>", repr(content))
        uu = uuid.uuid4()
        self.transport.requests[uu] = request
        mess = 
        self.transport.publisher.publish_message(
        d = deferLater(reactor, 2, lambda: uu)
        d.addCallback(self.transport.finishRequest)
        return NOT_DONE_YET


#class HttpRpcConsumer(Consumer):
    #exchange_name = "vumi"
    #exchange_type = "direct"
    #durable = True
    #queue_name = routing_key = "ussd.outbound.cellulant" #TODO fix in config

    #def __init__(self, transport):
        #self.transport = transport
        #self.publisher = transport.publisher


#class HttpRpcPublisher(Publisher):
    #exchange_name = "vumi"
    #exchange_type = "direct"
    #routing_key = "ussd.inbound.cellulant.fallback" #TODO fix in config
    #durable = True
    #auto_delete = False
    #delivery_mode = 2 # save to disk


class HttpRpcTransport(Worker):

    @inlineCallbacks
    def startWorker(self):
        self.uuid = uuid.uuid4()
        log.msg("Starting HttpRpcTransport %s config: %s" % (self.uuid, self.config))
        self.publish_key = 'ussd.inbound.cellulant.http'
        self.consume_key = 'ussd.outbound.cellulant.http'

        self.requests = {}

        self.publisher = yield self.publish_to(self.publish_key)
        self.consume(self.consume_key, self.consume_message)

        ## create the publisher
        #self.publisher = yield self.start_publisher(HttpRpcPublisher)
        ## when it's done, create the consumer and pass it the transport
        #self.consumer = yield self.start_consumer(HttpRpcConsumer, self)

        # start receipt web resource
        self.receipt_resource = yield self.start_web_resources(
            [
                (HttpRpcResource(self), self.config['web_path']),
            ],
            self.config['web_port']
        )

    def consume_message(self, message):
        pass
        #_message = packCellulantUSSDMessage(message)
        #try:
            #self.publisher.publish_message(_message)
        #except:
            #pass
        #return _message

    def finishRequest(self, uuid, data='No data\n'):
        request = self.requests.get(uuid)
        if request:
            request.write(data)
            request.finish()
            self.requests[uuid] = None

    def stopWorker(self):
        log.msg("Stopping the HttpRpcTransport")
