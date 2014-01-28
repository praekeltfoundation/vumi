# -*- test-case-name: vumi.transports.httprpc.tests.test_httprpc -*-

import json

from twisted.cred.portal import Portal
from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.web import http
from twisted.web.guard import BasicCredentialFactory, HTTPAuthSessionWrapper
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi import log
from vumi.config import ConfigText, ConfigInt, ConfigBool, ConfigError
from vumi.transports.base import Transport
from vumi.transports.httprpc.auth import HttpRpcRealm, StaticAuthChecker


class HttpRpcTransportConfig(Transport.CONFIG_CLASS):
    """Base config definition for transports.

    You should subclass this and add transport-specific fields.
    """

    web_path = ConfigText("The path to listen for requests on.", static=True)
    web_port = ConfigInt(
        "The port to listen for requests on, defaults to `0`.",
        default=0, static=True)
    web_username = ConfigText(
        "The username to require callers to authenticate with. If ``None``"
        " then no authentication is required. Currently only HTTP Basic"
        " authentication is supported.",
        default=None, static=True)
    web_password = ConfigText(
        "The password to go with ``web_username``. Must be ``None`` if and"
        " only if ``web_username`` is ``None``.",
        default=None, static=True)
    web_auth_domain = ConfigText(
        "The name of authentication domain.",
        default="Vumi HTTP RPC transport", static=True)
    health_path = ConfigText(
        "The path to listen for downstream health checks on"
        " (useful with HAProxy)", default='health', static=True)
    request_cleanup_interval = ConfigInt(
        "How often should we actively look for old connections that should"
        " manually be timed out. Anything less than `1` disables the request"
        " cleanup meaning that all request objects will be kept in memory"
        " until the server is restarted, regardless if the remote side has"
        " dropped the connection or not. Defaults to 5 seconds.",
        default=5, static=True)
    request_timeout = ConfigInt(
        "How long should we wait for the remote side generating the response"
        " for this synchronous operation to come back. Any connection that has"
        " waited longer than `request_timeout` seconds will manually be"
        " closed. Defaults to 4 minutes.", default=(4 * 60), static=True)
    request_timeout_status_code = ConfigInt(
        "What HTTP status code should be generated when a timeout occurs."
        " Defaults to `504 Gateway Timeout`.", default=504, static=True)
    request_timeout_body = ConfigText(
        "What HTTP body should be returned when a timeout occurs."
        " Defaults to ''.", default='', static=True)
    noisy = ConfigBool(
        "Defaults to `False` set to `True` to make this transport log"
        " verbosely.", default=False, static=True)
    validation_mode = ConfigText(
        "The mode to operate in. Can be 'strict' or 'permissive'. If 'strict'"
        " then any parameter received that is not listed in EXPECTED_FIELDS"
        " nor in IGNORED_FIELDS will raise an error. If 'permissive' then no"
        " error is raised as long as all the EXPECTED_FIELDS are present.",
        default='strict', static=True)

    def post_validate(self):
        auth_supplied = (self.web_username is None, self.web_password is None)
        if any(auth_supplied) and not all(auth_supplied):
            raise ConfigError("If either web_username or web_password is"
                              " specified, both must be specified")


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
    ENCODING = 'UTF-8'
    STRICT_MODE = 'strict'
    PERMISSIVE_MODE = 'permissive'
    DEFAULT_VALIDATION_MODE = STRICT_MODE
    KNOWN_VALIDATION_MODES = [STRICT_MODE, PERMISSIVE_MODE]

    def validate_config(self):
        config = self.get_static_config()
        self.web_path = config.web_path
        self.web_port = config.web_port
        self.web_username = config.web_username
        self.web_password = config.web_password
        self.web_auth_domain = config.web_auth_domain
        self.health_path = config.health_path.lstrip('/')
        self.request_timeout = config.request_timeout
        self.request_timeout_status_code = config.request_timeout_status_code
        self.noisy = config.noisy
        self.request_timeout_body = config.request_timeout_body
        self.gc_requests_interval = config.request_cleanup_interval
        self._validation_mode = config.validation_mode
        if self._validation_mode not in self.KNOWN_VALIDATION_MODES:
            raise ConfigError('Invalid validation mode: %s' % (
                self._validation_mode,))

    def get_transport_url(self, suffix=''):
        """
        Get the URL for the HTTP resource. Requires the worker to be started.

        This is mostly useful in tests, and probably shouldn't be used
        in non-test code, because the API might live behind a load
        balancer or proxy.
        """
        addr = self.web_resource.getHost()
        return "http://%s:%s/%s" % (addr.host, addr.port, suffix.lstrip('/'))

    def get_authenticated_resource(self, resource):
        if not self.web_username:
            return resource
        realm = HttpRpcRealm(resource)
        checkers = [
            StaticAuthChecker(self.web_username, self.web_password),
        ]
        portal = Portal(realm, checkers)
        cred_factories = [
            BasicCredentialFactory(self.web_auth_domain),
        ]
        return HTTPAuthSessionWrapper(portal, cred_factories)

    @inlineCallbacks
    def setup_transport(self):
        self._requests = {}
        self.request_gc = LoopingCall(self.manually_close_requests)
        self.clock = self.get_clock()
        self.request_gc.clock = self.clock
        self.request_gc.start(self.gc_requests_interval)

        rpc_resource = HttpRpcResource(self)
        rpc_resource = self.get_authenticated_resource(rpc_resource)

        # start receipt web resource
        self.web_resource = yield self.start_web_resources(
            [
                (rpc_resource, self.web_path),
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

    def get_field_values(self, request, expected_fields,
                            ignored_fields=frozenset()):
        values = {}
        errors = {}
        for field in request.args:
            if field not in (expected_fields | ignored_fields):
                if self._validation_mode == self.STRICT_MODE:
                    errors.setdefault('unexpected_parameter', []).append(field)
            else:
                values[field] = (
                    request.args.get(field)[0].decode(self.ENCODING))
        for field in expected_fields:
            if field not in values:
                errors.setdefault('missing_parameter', []).append(field)
        return values, errors

    def ensure_message_values(self, message, expected_fields):
        missing_fields = []
        for field in expected_fields:
            if not message[field]:
                missing_fields.append(field)
        return missing_fields

    def manually_close_requests(self):
        for request_id, request_data in self._requests.items():
            timestamp = request_data['timestamp']
            if timestamp < self.clock.seconds() - self.request_timeout:
                self.close_request(request_id)

    def close_request(self, request_id):
        log.warning('Timing out %s' % (self.get_request_to_addr(request_id),))
        self.finish_request(request_id, self.request_timeout_body,
                            self.request_timeout_status_code)

    def get_health_response(self):
        return json.dumps({
            'pending_requests': len(self._requests)
        })

    def set_request(self, request_id, request_object, timestamp=None):
        if timestamp is None:
            timestamp = self.clock.seconds()
        self._requests[request_id] = {
            'timestamp': timestamp,
            'request': request_object,
        }

    def get_request(self, request_id):
        if request_id in self._requests:
            return self._requests[request_id]['request']

    def remove_request(self, request_id):
        del self._requests[request_id]

    def emit(self, msg):
        if self.noisy:
            log.debug(msg)

    def handle_outbound_message(self, message):
        self.emit("HttpRpcTransport consuming %s" % (message))
        missing_fields = self.ensure_message_values(message,
                            ['in_reply_to', 'content'])
        if missing_fields:
            return self.reject_message(message, missing_fields)
        else:
            self.finish_request(
                    message.payload['in_reply_to'],
                    message.payload['content'].encode('utf-8'))
            return self.publish_ack(user_message_id=message['message_id'],
                sent_message_id=message['message_id'])

    def reject_message(self, message, missing_fields):
        return self.publish_nack(user_message_id=message['message_id'],
            sent_message_id=message['message_id'],
            reason='Missing fields: %s' % ', '.join(missing_fields))

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

    # NOTE: This hackery is required so that we know what to_addr a message
    #       was received on. This is useful so we can log more useful debug
    #       information when something goes wrong, like a timeout for example.
    #
    #       Since all the different transports that subclass this
    #       base class have different implementations for retreiving the
    #       to_addr it's impossible to grab this information higher up
    #       in a consistent manner.
    def publish_message(self, **kwargs):
        self.set_request_to_addr(kwargs['message_id'], kwargs['to_addr'])
        return super(HttpRpcTransport, self).publish_message(**kwargs)

    def get_request_to_addr(self, request_id):
        return self._requests[request_id].get('to_addr', 'Unknown')

    def set_request_to_addr(self, request_id, to_addr):
        if request_id in self._requests:
            self._requests[request_id]['to_addr'] = to_addr
