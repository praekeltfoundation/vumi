# -*- test-case-name: vumi.transports.httprpc.tests.test_httprpc -*-

import json

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi import log


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

    Takes the following configuration parameters:

    :param str web_path:
        The path to listen for requests on.
    :param int web_port:
        The port to listen for requests on, defaults to `0`.
    :param str health_path:
        The path to listen for downstream health checks on
        (useful with HAProxy)
    :param int request_cleanup_interval:
        How often should we actively look for old connections that should
        manually be timed out. Anything less than `1` disables the request
        cleanup meaning that all request objects will be kept in memory until
        the server is restarted, regardless if the remote side has dropped
        the connection or not.
        Defaults to 5 seconds.
    :param int request_timeout:
        How long should we wait for the remote side generating the response
        for this synchronous operation to come back. Any connection that has
        waited longer than `request_timeout` seconds will manually be closed.
        Defaults to 4 minutes.
    :param int request_timeout_status_code:
        What HTTP status code should be generated when a timeout occurs.
        Defaults to `504 Gateway Timeout`.
    :param str request_timeout_body:
        What HTTP body should be returned when a timeout occurs.
        Defaults to ''.
    :param bool noisy:
        Defaults to `False` set to `True` to make this transport log
        verbosely.
    """
    content_type = 'text/plain'

    def validate_config(self):
        self.web_path = self.config['web_path']
        self.web_port = int(self.config['web_port'])
        self.health_path = self.config.get('health_path', 'health').lstrip('/')
        self.request_timeout = int(self.config.get('request_timeout', 60 * 4))
        self.request_timeout_status_code = int(
            self.config.get('request_timeout_status_code', 504))
        self.noisy = bool(self.config.get('noisy', False))
        self.request_timeout_body = self.config.get('request_timeout_body', '')

        self.gc_requests_interval = int(
            self.config.get('request_cleanup_interval', 5))

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
        log.error('Timing out %s' % (request_id,))
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
            log.message(msg)

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
