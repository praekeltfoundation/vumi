# -*- test-case-name: vumi.transports.netcore.tests.test_netcore -*-

from vumi.config import (
    ConfigServerEndpoint, ConfigText, ConfigBool, ConfigInt,
    ServerEndpointFallback)
from vumi.transports import Transport
from vumi.transports.httprpc.httprpc import HttpRpcHealthResource
from vumi.utils import build_web_site

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET


class NetcoreTransportConfig(Transport.CONFIG_CLASS):

    twisted_endpoint = ConfigServerEndpoint(
        'The endpoint to listen on.',
        required=True, static=True,
        fallbacks=[ServerEndpointFallback()])
    web_path = ConfigText(
        "The path to serve this resource on.",
        default='/api/v1/netcore/', static=True)
    health_path = ConfigText(
        "The path to serve the health resource on.",
        default='/health/', static=True)
    reject_none = ConfigBool(
        "Reject messages where the content parameter equals 'None'",
        required=False, default=True, static=True)

    # TODO: Deprecate these fields when confmodel#5 is done.
    host = ConfigText(
        "*DEPRECATED* 'host' and 'port' fields may be used in place of the"
        " 'twisted_endpoint' field.", static=True)
    port = ConfigInt(
        "*DEPRECATED* 'host' and 'port' fields may be used in place of the"
        " 'twisted_endpoint' field.", static=True)


class NetcoreResource(Resource):

    isLeaf = True

    def __init__(self, transport):
        Resource.__init__(self)
        self.transport = transport
        self.config = transport.get_static_config()

    def render_POST(self, request):
        expected_keys = [
            'to_addr',
            'from_addr',
            'content',
            'circle',
            'source',
        ]

        received = set(request.args.keys())
        expected = set(expected_keys)
        if received != expected:
            request.setResponseCode(http.BAD_REQUEST)
            return ('Not all expected parameters received. '
                    'Only allowing: %r, received: %r' % (
                        expected_keys, request.args.keys()))
        param_values = [param[0] for param in request.args.values()]
        if not all(param_values):
            request.setResponseCode(http.BAD_REQUEST)
            return ('Not all parameters have values. '
                    'Received: %r' % (sorted(param_values),))

        content = request.args['content'][0]
        if self.config.reject_none and content == "None":
            request.setResponseCode(http.BAD_REQUEST)
            return ('"None" string literal not allowed for content parameter.')

        self.handle_request(request)
        return NOT_DONE_YET

    def handle_request(self, request):
        to_addr = request.args['to_addr'][0]
        from_addr = request.args['from_addr'][0]
        content = request.args['content'][0]
        circle = request.args['circle'][0]
        source = request.args['source'][0]

        # NOTE: If we have a leading 0 then the normalization middleware
        #       will deal with it.
        if not from_addr.startswith('0'):
            from_addr = '0%s' % (from_addr,)

        d = self.transport.handle_raw_inbound_message(
            to_addr, from_addr, content, circle, source)
        d.addCallback(lambda msg: request.write(msg['message_id']))
        d.addCallback(lambda _: request.finish())
        return d


class NetcoreTransport(Transport):

    CONFIG_CLASS = NetcoreTransportConfig

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        self.endpoint = config.twisted_endpoint
        self.resource = NetcoreResource(self)

        self.factory = build_web_site({
            config.health_path: HttpRpcHealthResource(self),
            config.web_path: self.resource,
        })
        self.server = yield self.endpoint.listen(self.factory)

    def teardown_transport(self):
        return self.server.stopListening()

    def handle_raw_inbound_message(self, to_addr, from_addr, content,
                                   circle, source):
        return self.publish_message(
            content=content,
            from_addr=from_addr,
            to_addr=to_addr,
            transport_type='sms',
            transport_metadata={
                'netcore': {
                    'circle': circle,
                    'source': source,
                }
            })

    def get_health_response(self):
        return 'OK'
