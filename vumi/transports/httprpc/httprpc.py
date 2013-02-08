# -*- test-case-name: vumi.transports.httprpc.tests.test_httprpc -*-

import json

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi.config import ConfigText, ConfigInt, ConfigBool
from vumi import log


class HttpRpcTransportConfig(Transport.CONFIG_CLASS):
    """Base config definition for transports.

    You should subclass this and add transport-specific fields.
    """

    web_path = ConfigText("The path to listen for requests on.")
    web_port = ConfigInt(
        "The port to listen for requests on, defaults to `0`.", default=0)
    health_path = ConfigText(
        "The path to listen for downstream health checks on"
        " (useful with HAProxy)", default='health')
    request_cleanup_interval = ConfigInt(
        "How often should we actively look for old connections that should"
        " manually be timed out. Anything less than `1` disables the request"
        " cleanup meaning that all request objects will be kept in memory"
        " until the server is restarted, regardless if the remote side has"
        " dropped the connection or not. Defaults to 5 seconds.",
        default=5)
    request_timeout = ConfigInt(
        "How long should we wait for the remote side generating the response"
        " for this synchronous operation to come back. Any connection that has"
        " waited longer than `request_timeout` seconds will manually be"
        " closed. Defaults to 4 minutes.", default=(4 * 60))
    request_timeout_status_code = ConfigInt(
        "What HTTP status code should be generated when a timeout occurs."
        " Defaults to `504 Gateway Timeout`.", default=504)
    request_timeout_body = ConfigText(
        "What HTTP body should be returned when a timeout occurs."
        " Defaults to ''.", default='')
    noisy = ConfigBool(
        "Defaults to `False` set to `True` to make this transport log"
        " verbosely.", default=False)


class HttpRpcHealthResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render_GET(self, request):
        request.setResponseCode(http.OK)
        request.do_not_log = True
        return self.transport.get_health_response()


class HttpRpcResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render_(self, request, request_id=None):
        request_id = request_id or Transport.generate_message_id()
        request.setHeader("content-type", self.transport.content_type)
        self.transport.set_request(request_id, request)
        self.transport.handle_raw_inbound_message(request_id, request)
        return NOT_DONE_YET

    def render_PUT(self, request):
        return self.render_(request)

    def render_GET(self, request):
        return self.render_(request)

    def render_POST(self, request):
        return self.render_(request)


class HttpRpcTransport(Transport):
    """Base class for synchronous HTTP transports.

    Because a reply from an application worker is needed before the HTTP
    response can be completed, a reply needs to be returned to the same
    transport worker that generated the inbound message. This means that
    currently there many only be one transport worker for each instance
    of this transport of a given name.
    """
    content_type = 'text/plain'

    CONFIG_CLASS = HttpRpcTransportConfig

    def validate_config(self):
        config = self.get_static_config()
        self.web_path = config.web_path
        self.web_port = config.web_port
        self.health_path = config.health_path.lstrip('/')
        self.request_timeout = config.request_timeout
        self.request_timeout_status_code = config.request_timeout_status_code
        self.noisy = config.noisy
        self.request_timeout_body = config.request_timeout_body
        self.gc_requests_interval = config.request_cleanup_interval

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
        self._requests = {}
        self.request_gc = LoopingCall(self.manually_close_requests)
        self.clock = self.get_clock()
        self.request_gc.clock = self.clock
        self.request_gc.start(self.gc_requests_interval)

        # start receipt web resource
        self.web_resource = yield self.start_web_resources(
            [
                (HttpRpcResource(self), self.web_path),
                (HttpRpcHealthResource(self), self.health_path),
            ],
            self.web_port)

    @inlineCallbacks
    def teardown_transport(self):
        yield self.web_resource.loseConnection()
        if self.request_gc.running:
            self.request_gc.stop()

    def get_clock(self):
        """
        For easier stubbing in tests
        """
        return reactor

    def manually_close_requests(self):
        for request_id, (timestamp, request) in self._requests.items():
            if timestamp < self.clock.seconds() - self.request_timeout:
                self.close_request(request_id)

    def close_request(self, request_id):
        log.warn('Timing out %s' % (request_id,))
        self.finish_request(request_id, self.request_timeout_body,
            self.request_timeout_status_code)

    def get_health_response(self):
        return json.dumps({
            'pending_requests': len(self._requests)
        })

    def set_request(self, request_id, request_object, timestamp=None):
        if timestamp is None:
            timestamp = self.clock.seconds()
        self._requests[request_id] = (timestamp, request_object)

    def get_request(self, request_id):
        if request_id in self._requests:
            _, request = self._requests[request_id]
            return request

    def remove_request(self, request_id):
        del self._requests[request_id]

    def emit(self, msg):
        if self.noisy:
            log.debug(msg)

    @inlineCallbacks
    def handle_outbound_message(self, message):
        self.emit("HttpRpcTransport consuming %s" % (message))
        if message.payload.get('in_reply_to') and 'content' in message.payload:
            self.finish_request(
                    message.payload['in_reply_to'],
                    message.payload['content'].encode('utf-8'))
            yield self.publish_ack(user_message_id=message['message_id'],
                sent_message_id=message['message_id'])

    def handle_raw_inbound_message(self, msgid, request):
        raise NotImplementedError("Sub-classes should implement"
                                  " handle_raw_inbound_message.")

    def finish_request(self, request_id, data, code=200, headers={}):
        self.emit("HttpRpcTransport.finish_request with data: %s" % (
            repr(data),))
        request = self.get_request(request_id)
        if request:
            for h_name, h_values in headers.iteritems():
                request.responseHeaders.setRawHeaders(h_name, h_values)
            request.setResponseCode(code)
            request.write(data)
            request.finish()
            self.remove_request(request_id)
            response_id = "%s:%s:%s" % (request.client.host,
                                        request.client.port,
                                        Transport.generate_message_id())
            return response_id
