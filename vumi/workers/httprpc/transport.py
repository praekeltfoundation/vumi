import uuid

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from vumi.message import Message
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

    def render_(self, request, logmsg=None):
        request.setHeader("content-type", "text/plain")
        uu = str(uuid.uuid4())
        md = {}
        md['args'] = request.args
        md['content'] = request.content.read()
        md['path'] = request.path
        #if logmsg:
        log.msg("HttpRpcResource", logmsg, "Message.message:", repr(md))
        message = Message(message=md, uuid=uu,
                          return_path=[self.transport.consume_key])
        self.transport.publisher.publish_message(message)
        self.transport.requests[uu] = request
        return NOT_DONE_YET

    def render_GET(self, request):
        return self.render_(request, "render_GET")

    def render_POST(self, request):
        return self.render_(request, "render_POST")


class HttpRpcTransport(Worker):

    @inlineCallbacks
    def startWorker(self):
        self.uuid = uuid.uuid4()
        log.msg("Starting HttpRpcTransport %s config: %s" % (self.uuid,
                                                             self.config))
        self.publish_key = self.config['publish_key']
        self.consume_key = self.config['consume_key']

        self.requests = {}

        self.publisher = yield self.publish_to(self.publish_key)
        self.consume(self.consume_key, self.consume_message)

        # start receipt web resource
        self.receipt_resource = yield self.start_web_resources(
            [
                (HttpRpcResource(self), self.config['web_path']),
                (HttpRpcHealthResource(self), 'health'),
            ],
            self.config['web_port'])
        print self.receipt_resource

    def consume_message(self, message):
        log.msg("HttpRpcTransport consuming on %s: %s" % (
            self.consume_key,
            repr(message.payload)))
        if message.payload.get('uuid') and 'message' in message.payload:
            self.finishRequest(
                    message.payload['uuid'],
                    message.payload['message'])

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
        hi = '''
<request>
    <headertext>Welcome to Vodacom Ikhwezi!</headertext>
    <options>
        <option
            command="1"
            order="1"
            callback="http://vumi.praekeltfoundation.org/api/v1/ussd/ikhwezi/"
            display="True">finish session.</option>
        <option
            command="2"
            order="2"
            callback="http://vumi.praekeltfoundation.org/api/v1/ussd/ikhwezi/"
            display="True">return to this screen.</option>
        <option
            command="3"
            order="3"
            callback="http://vumi.praekeltfoundation.org/api/v1/ussd/ikhwezi/"
            display="True">continue to next screen.</option>
    </options>
</request>'''
        cont = '''
<request>
    <headertext>Nothing to see yet</headertext>
    <options>
        <option
            command="1"
            order="1"
            callback="http://vumi.praekeltfoundation.org/api/v1/ussd/ikhwezi/"
            display="True">finish session.</option>
    </options>
</request>'''
        bye = '''
<request>
    <headertext>Goodbye!</headertext>
</request>'''

        log.msg("DummyRpcWorker consuming on %s: %s" % (
            self.consume_key,
            repr(message.payload)))
        reply = hi
        if message.payload['message']['args'].get('request', [''])[0] == "1":
            reply = bye
        if message.payload['message']['args'].get('request', [''])[0] == "3":
            reply = cont
        self.publisher.publish_message(Message(
                uuid=message.payload['uuid'],
                message=reply),
            routing_key=message.payload['return_path'].pop())

    def stopWorker(self):
        log.msg("Stopping the MenuWorker")
