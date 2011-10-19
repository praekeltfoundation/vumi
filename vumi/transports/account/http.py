import uuid

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.web import http
from twisted.web.resource import Resource
from vumi.transports.base import Transport


class HttpHealthResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render_GET(self, request):
        request.setResponseCode(http.OK)
        return "OK"


class HttpResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render_(self, request, http_action=None):
        log.msg("HttpRpcResource HTTP Action: %s" % (request,))
        request.setHeader("content-type", "text/plain")
        uu = str(uuid.uuid4().get_hex())
        self.transport.handle_raw_inbound_message(uu, request)
        return "Message id: %s" % (uu)

    def render_GET(self, request):
        return self.render_(request, "render_GET")

    def render_POST(self, request):
        return self.render_(request, "render_POST")


class HttpTransport(Transport):
    """
    Strictly for internal testing only
    this has NO SECURITY!
    """

    @inlineCallbacks
    def setup_transport(self):

        # start receipt web resource
        self.web_resource = yield self.start_web_resources(
            [
                (HttpResource(self), self.config['web_path']),
                (HttpHealthResource(self), 'health'),
            ],
            self.config['web_port'])

    @inlineCallbacks
    def teardown_transport(self):
        yield self.web_resource.loseConnection()

    def handle_outbound_message(self, message):
        log.msg("HttpRpcTransport consuming %s" % (message))

    def handle_raw_inbound_message(self, msgid, request):
        content = str(request.args.get('content', [None])[0])
        to_addr = str(request.args.get('to_addr', [None])[0])
        from_addr = str(request.args.get('from_addr', [None])[0])

        transport_metadata = {}
        self.publish_message(
                message_id=msgid,
                content=content,
                to_addr=to_addr,
                from_addr=from_addr,
                provider='internet',
                transport_name=self.transport_name,
                transport_type=self.config.get('transport_type'),
                )
