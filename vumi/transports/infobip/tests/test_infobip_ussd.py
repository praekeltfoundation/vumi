"""Test for vumi.transport.infobip.infobip_ussd."""

import json

from twisted.trial.unittest import TestCase
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web.server import Site
from twisted.internet import reactor
from twisted.internet.base import DelayedCall

from vumi.utils import http_request
from vumi.transports.infobip.infobip_ussd import InfobipUssdTransport
from vumi.message import TransportUserMessage
from vumi.tests.utils import get_stubbed_worker


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


class TestInfobipUssdTransport(TestCase):

    @inlineCallbacks
    def setUp(self):
        DelayedCall.debug = True
        self.vodacom_messaging_calls = DeferredQueue()
        self.mock_infobip = MockHttpServer(self.handle_request)
        yield self.mock_infobip.start()
        config = {
            'transport_name': 'test_infobip',
            'transport_type': 'ussd',
            'ussd_string_prefix': '*120*666',
            'web_path': "/session/",
            'web_host': "localhost",
            'web_port': 0,
            'url': self.mock_infobip.url,
            'username': 'testuser',
            'password': 'testpass',
            }
        self.worker = get_stubbed_worker(InfobipUssdTransport, config)
        self.broker = self.worker._amqp_client.broker
        yield self.worker.startWorker()
        addr = self.worker.web_resource.getHost()
        self.worker_url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()
        yield self.mock_infobip.stop()

    def handle_request(self, request):
        self.vodacom_messaging_calls.put(request)
        return ''

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(self.worker_url + "health", "",
                                    method='GET')
        self.assertEqual(json.loads(result), {
            'pending_requests': 0
        })

    @inlineCallbacks
    def test_start(self):
        json_content = json.dumps({
                'msisdn': '55567890',
                'text':"hello there",
                'shortCode': "*120*666#"
                })
        d = http_request(self.worker_url + "session/1/start",
                json_content, method='POST')
        msg, = yield self.broker.wait_messages("vumi",
            "test_infobip.inbound", 1)
        payload = msg.payload
        self.assertEqual(payload['content'], 'hello there')
        tum = TransportUserMessage(**payload)
        rep = tum.reply("hello yourself")
        self.broker.publish_message("vumi", "test_infobip.outbound",
                rep)
        response = yield d
        correct_response = {
                "shouldClose": False,
                "responseExitCode": 200,
                "ussdMenu": "hello yourself",
                "responseMessage": "",
                }
        self.assertEqual(json.loads(response), correct_response)

