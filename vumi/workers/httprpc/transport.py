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
        if logmsg:
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


class VodacomMessagingResponse(object):
    def __init__(self, config):
        self.config = config
        self.context = ''
        self.freetext_option = None
        self.template_freetext_option_string = '<option' \
            + ' command="1"' \
            + ' order="1"' \
            + ' callback="http://%(web_host)s%(web_path)s?context=%(context)s"' \
            + ' display="False"' \
            + ' ></option>'
        self.option_list = []
        self.template_numbered_option_string = '<option' \
            + ' command="%(order)s"' \
            + ' order="%(order)s"' \
            + ' callback="http://%(web_host)s%(web_path)s?context=%(context)s"' \
            + ' display="True"' \
            + ' >%(text)s</option>'

    def set_headertext(self, headertext):
        self.headertext = headertext

    def set_context(self, context):
        """
        context is a unique identifier for the state that generated
        the message the user is responding to
        """
        self.context = context
        if self.freetext_option:
            self.accept_freetext()
        count = 0
        while count < len(self.option_list):
            self.option_list[count].update({'context': self.context})
            count += 1

    def add_option(self, text):
        self.freetext_option = None
        dict = {'text': str(text)}
        dict['order'] = len(self.option_list) + 1
        dict.update({
            'web_path': self.config['web_path'],
            'web_host': self.config['web_host'],
            'context': self.context})
        self.option_list.append(dict)

    def accept_freetext(self):
        self.option_list = []
        self.freetext_option = self.template_freetext_option_string % {
            'web_path': self.config['web_path'],
            'web_host': self.config['web_host'],
            'context': self.context}

    def __str__(self):
        headertext = '\t<headertext>%s</headertext>\n' % self.headertext
        options = ''
        if self.freetext_option or len(self.option_list) > 0:
            options = '\t<options>\n'
            for o in self.option_list:
                options += '\t\t' + self.template_numbered_option_string % o + '\n'
            if self.freetext_option:
                options += '\t\t' + self.freetext_option + '\n'
            options += '\t</options>\n'
        response = '<request>\n' + headertext + options + '</request>'
        return response


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
