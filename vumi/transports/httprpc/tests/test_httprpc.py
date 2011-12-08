import json

from twisted.trial.unittest import TestCase
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web.server import Site
from twisted.internet import reactor
from twisted.internet.base import DelayedCall

from vumi.utils import http_request
from vumi.transports.httprpc import HttpRpcTransport
from vumi.message import TransportUserMessage
from vumi.tests.utils import get_stubbed_worker


class OkTransport(HttpRpcTransport):

    def handle_raw_inbound_message(self, msgid, request):
        self.publish_message(
                message_id=msgid,
                content='',
                to_addr='',
                from_addr='',
                provider='',
                session_event=TransportUserMessage.SESSION_NEW,
                transport_name=self.transport_name,
                transport_type=self.config.get('transport_type'),
                transport_metadata={},
                )


class MockResource(Resource):
    isLeaf = True

    def __init__(self, handler):
        Resource.__init__(self)
        self.handler = handler

    def render_GET(self, request):
        return self.handler(request)

    def render_POST(self, request):
        return self.handler(request)


class MockHttpServer(object):

    def __init__(self, handler):
        self._handler = handler
        self._webserver = None
        self.addr = None
        self.url = None

    @inlineCallbacks
    def start(self):
        root = MockResource(self._handler)
        site_factory = Site(root)
        self._webserver = yield reactor.listenTCP(0, site_factory)
        self.addr = self._webserver.getHost()
        self.url = "http://%s:%s/" % (self.addr.host, self.addr.port)

    @inlineCallbacks
    def stop(self):
        yield self._webserver.loseConnection()


class TestTransport(TestCase):

    @inlineCallbacks
    def setUp(self):
        DelayedCall.debug = True
        self.ok_transport_calls = DeferredQueue()
        self.mock_service = MockHttpServer(self.handle_request)
        yield self.mock_service.start()
        config = {
            'transport_name': 'test_ok_transport',
            'transport_type': 'ok',
            'ussd_string_prefix': '',
            'web_path': "foo",
            'web_port': 0,
            'url': self.mock_service.url,
            'username': 'testuser',
            'password': 'testpass',
            }
        self.worker = get_stubbed_worker(OkTransport, config)
        self.broker = self.worker._amqp_client.broker
        yield self.worker.startWorker()
        addr = self.worker.web_resource.getHost()
        self.worker_url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()
        yield self.mock_service.stop()

    def handle_request(self, request):
        self.ok_transport_calls.put(request)
        return ''

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(self.worker_url + "health", "",
                                    method='GET')
        self.assertEqual(json.loads(result), {
            'pending_requests': 0
        })

    @inlineCallbacks
    def test_inbound(self):
        d = http_request(self.worker_url + "foo", '', method='GET')
        msg, = yield self.broker.wait_messages("vumi",
            "test_ok_transport.inbound", 1)
        payload = msg.payload
        tum = TransportUserMessage(**payload)
        rep = tum.reply("OK")
        self.broker.publish_message("vumi", "test_ok_transport.outbound",
                rep)
        response = yield d
        self.assertEqual(response, 'OK')
