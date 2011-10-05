import uuid

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from vumi.message import Message
from vumi.transports.base import Transport
from vumi.service import Worker


class HttpRpcHealthResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render_GET(self, request):
        request.setResponseCode(http.OK)
        return "pReq:%s" % len(self.transport.requests)


class HttpRpcResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render_(self, request, http_action=None):
        request.setHeader("content-type", "text/plain")
        uu = str(uuid.uuid4().get_hex())
        md = {}
        md['args'] = request.args
        md['content'] = request.content.read()
        md['path'] = request.path
        log.msg("HttpRpcResource HTTP Action: %s  Message.message: %s" % (
            http_action, repr(md)))
        self.transport.publish_message(
                message_id=uu,
                content=md,
                to_addr='',
                from_addr='',
                transport_name=self.transport.config.get('transport_name'),
                transport_type=self.transport.config.get('transport_type')
                )
        self.transport.requests[uu] = request
        return NOT_DONE_YET

    def render_GET(self, request):
        return self.render_(request, "render_GET")

    def render_POST(self, request):
        return self.render_(request, "render_POST")


class HttpRpcTransport(Transport):

    @inlineCallbacks
    def setup_transport(self):
        self.uuid = uuid.uuid4()
        log.msg("Starting HttpRpcTransport %s config: %s" % (self.uuid,
                                                             self.config))
        self.requests = {}

        # start receipt web resource
        self.receipt_resource = yield self.start_web_resources(
            [
                (HttpRpcResource(self), self.config['web_path']),
                (HttpRpcHealthResource(self), 'health'),
            ],
            self.config['web_port'])
        print self.receipt_resource

    def handle_outbound_message(self, message):
        log.msg("HttpRpcTransport consuming %s" % (message))
        if message.payload.get('in_reply_to') and 'content' in message.payload:
            self.finishRequest(
                    message.payload['in_reply_to'],
                    message.payload['content'])

    def finishRequest(self, uuid, message=''):
        data = str(message)
        log.msg("HttpRpcTransport.finishRequest with data:", repr(data))
        log.msg(repr(self.requests))
        request = self.requests.get(uuid)
        if request:
            request.write(data)
            request.finish()
            del self.requests[uuid]

    def stopWorker(self):
        log.msg("Stopping the HttpRpcTransport")


class DummyRpcWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting DummyRpcWorker with config: %s" % (self.config))
        self.publish_key = self.config['consume_key']  # Swap consume and
        self.consume_key = self.config['publish_key']  # publish keys

        self.publisher = yield self.publish_to(self.publish_key)
        self.consume(self.consume_key, self.consume_message)

    def consume_message(self, message):
        log.msg("DummyRpcWorker consuming on %s: %s" % (
            self.consume_key,
            repr(message.payload)))
        self.publisher.publish_message(Message(
                uuid=message.payload['uuid'],
                message="OK"),
            routing_key=message.payload['return_path'].pop())

    def stopWorker(self):
        log.msg("Stopping the MenuWorker")
