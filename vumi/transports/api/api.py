# -*- test-case-name: vumi.transports.api.tests.test_api -*-
import uuid
import json

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.web import http
from twisted.web.resource import Resource
from vumi.transports.base import Transport


class HttpHealthResource(Resource):
    isLeaf = True

    def __init__(self):
        Resource.__init__(self)

    def render_GET(self, request):
        request.setResponseCode(http.OK)
        return json.dumps({})


class HttpResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render(self, request, http_action=None):
        log.msg("HttpResource HTTP Action: %s" % (request,))
        request.setHeader("content-type", "text/plain")
        uu = uuid.uuid4().get_hex()
        self.transport.handle_raw_inbound_message(uu, request)
        return json.dumps({
            'message_id': uu,
        })


class HttpApiTransport(Transport):
    """
    Strictly for internal testing only
    this has NO SECURITY!

    Configuration Values
    --------------------
    web_path : str
        The path relative to the host where this listens
    web_port : int
        The port this listens on
    transport_name : str
        The name this transport instance will use to create it's queues
    """

    @inlineCallbacks
    def setup_transport(self):

        # start receipt web resource
        self.web_resource = yield self.start_web_resources(
            [
                (HttpResource(self), self.config['web_path']),
                (HttpHealthResource(), 'health'),
            ],
            int(self.config['web_port']))

    @inlineCallbacks
    def teardown_transport(self):
        yield self.web_resource.loseConnection()

    def handle_outbound_message(self, message):
        log.msg("HttpApiTransport consuming %s" % (message))

    def handle_raw_inbound_message(self, message_id, request):
        content = str(request.args.get('content', [None])[0])
        to_addr = str(request.args.get('to_addr', [None])[0])
        from_addr = str(request.args.get('from_addr', [None])[0])
        log.msg("HttpApiTransport sending from %s to %s message \"%s\"" % (
            from_addr, to_addr, content))
        self.publish_message(
            message_id=message_id,
            content=content,
            to_addr=to_addr,
            from_addr=from_addr,
            provider='vumi',
            transport_type='http_api',
        )
