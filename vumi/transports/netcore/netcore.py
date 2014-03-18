# -*- test-case-name: vumi.transports.netcore.tests.test_netcore -*-

from vumi.config import ConfigServerEndpoint, ConfigText
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
        required=True, static=True)
    web_path = ConfigText(
        "The path to serve this resource on.",
        default='/api/v1/netcore/', static=True)
    health_path = ConfigText(
        "The path to serve the health resource on.",
        default='/health/', static=True)


def has_all_params(request):
    params = ['to_addr', 'from_addr', 'content', 'circle', 'source']
    return all([(key in request.args) for key in params])


class NetcoreResource(Resource):

    isLeaf = True

    def __init__(self, transport):
        Resource.__init__(self)
        self.transport = transport

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

        self.handle_request(request)
        return NOT_DONE_YET

    def handle_request(self, request):
        to_addr = request.args['to_addr'][0]
        from_addr = request.args['from_addr'][0]
        content = request.args['content'][0]
        circle = request.args['circle'][0]
        source = request.args['source'][0]

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
