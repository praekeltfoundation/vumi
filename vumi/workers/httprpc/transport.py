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
        uu = str(uuid.uuid4())
        self.transport.requests[uu] = request
        mess = Message(message=content,
                uuid=uu,
                transport_key=self.transport.consume_key)
        self.transport.publisher.publish_message(mess, routing_key=self.transport.consume_key)
        return NOT_DONE_YET



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

        # start receipt web resource
        self.receipt_resource = yield self.start_web_resources(
            [
                (HttpRpcResource(self), self.config['web_path']),
            ],
            self.config['web_port']
        )

    def consume_message(self, message):
        log.msg("HttpRpcTransport consuming on %s: %s" % (
            self.consume_key,
            repr(message.payload)))
        if message.payload.get('uuid') and message.payload.get('message'):
            self.finishRequest(
                    message.payload['uuid'],
                    message.payload['message'])


    def finishRequest(self, uuid, data):
        log.msg("finishRequest data:", repr(data))
        #log.msg(repr(self.requests))
        request = self.requests.get(uuid)
        if request:
            request.write(str(data))
            request.finish()
            del self.requests[uuid]

    def stopWorker(self):
        log.msg("Stopping the HttpRpcTransport")
