# -*- test-case-name: vumi.transports.httprpc.tests.test_httprpc -*-

import json

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport


class HttpRpcHealthResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render_GET(self, request):
        request.setResponseCode(http.OK)
        return self.transport.get_health_response()


class HttpRpcResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render_(self, request, http_action=None):
        log.msg("HttpRpcResource HTTP Action: %s" % (request,))
        request.setHeader("content-type", "text/plain")
        msgid = Transport.generate_message_id()
        self.transport.requests[msgid] = request
        self.transport.handle_raw_inbound_message(msgid, request)
        return NOT_DONE_YET

    def render_PUT(self, request):
        return self.render_(request, "render_PUT")

    def render_GET(self, request):
        return self.render_(request, "render_GET")

    def render_POST(self, request):
        return self.render_(request, "render_POST")


class HttpRpcTransport(Transport):
    """Base class for synchronous HTTP transports.

    Because a reply from an application worker is needed before the HTTP
    response can be completed, a reply needs to be returned to the same
    transport worker that generated the inbound message. This means that
    currently there many only be one transport worker for each instance
    of this transport of a given name.
    """

    def get_transport_url(self, suffix=''):
        """
        Get the URL for the HTTP resource. Requires the worker to be started.

        This is mostly useful in tests, and probably shouldn't be used
        in non-test code, because the API might live behind a load
        balancer or proxy.
        """
        addr = self.web_resource.getHost()
        return "http://%s:%s/%s" % (addr.host, addr.port, suffix.lstrip('/'))

    @inlineCallbacks
    def setup_transport(self):
        self.requests = {}

        # start receipt web resource
        self.web_resource = yield self.start_web_resources(
            [
                (HttpRpcResource(self), self.config['web_path']),
                (HttpRpcHealthResource(self), 'health'),
            ],
            self.config['web_port'])

    @inlineCallbacks
    def teardown_transport(self):
        yield self.web_resource.loseConnection()

    def get_health_response(self):
        return json.dumps({
            'pending_requests': len(self.requests)
        })

    def handle_outbound_message(self, message):
        log.msg("HttpRpcTransport consuming %s" % (message))
        if message.payload.get('in_reply_to') and 'content' in message.payload:
            self.finish_request(
                    message.payload['in_reply_to'],
                    message.payload['content'].encode('utf-8'))

    def handle_raw_inbound_message(self, msgid, request):
        raise NotImplementedError("Sub-classes should implement"
                                  " handle_raw_inbound_message.")

    def finish_request(self, msgid, data, code=200):
        log.msg("HttpRpcTransport.finish_request with data:", repr(data))
        log.msg(repr(self.requests))
        request = self.requests.get(msgid)
        if request:
            request.setResponseCode(code)
            request.write(data)
            request.finish()
            del self.requests[msgid]
            response_id = "%s:%s:%s" % (request.client.host,
                                        request.client.port,
                                        Transport.generate_message_id())
            return response_id
