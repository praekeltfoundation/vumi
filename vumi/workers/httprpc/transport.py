import uuid

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from vumi.message import Message
from vumi.service import Worker


class HttpRpcHealthResource(Resource):
    isLeaf = True
    def render_GET(self, request):
        request.setResponseCode(http.OK)
        return "OK"


class HttpRpcResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render_GET(self, request):
        log.msg("HttpRpcResource.render_GET args:", repr(request.args))
        return "OK\n"

    def render_POST(self, request):
        content = request.content.read()
        uu = str(uuid.uuid4())
        self.transport.requests[uu] = request
        mess = Message(message = content,
                uuid = uu,
                return_path = [self.transport.consume_key])
        self.transport.publisher.publish_message(mess)
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
                (HttpRpcHealthResource(), 'health'),
            ],
            self.config['web_port'])

    def consume_message(self, message):
        log.msg("HttpRpcTransport consuming on %s: %s" % (
            self.consume_key,
            repr(message.payload)))
        if message.payload.get('uuid') and message.payload.get('message'):
            self.finishRequest(
                    message.payload['uuid'],
                    message.payload['message'])

    def finishRequest(self, uuid, data=''):
        log.msg("HttpRpcTransport.finishRequest with data:", repr(data))
        log.msg(repr(self.requests))
        request = self.requests.get(uuid)
        if request:
            request.write(str(data))
            request.finish()
            del self.requests[uuid]

    def stopWorker(self):
        log.msg("Stopping the HttpRpcTransport")
