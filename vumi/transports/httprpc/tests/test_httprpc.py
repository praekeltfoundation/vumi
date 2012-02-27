import json

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.internet.base import DelayedCall

from vumi.utils import http_request
from vumi.transports.httprpc import HttpRpcTransport
from vumi.message import TransportUserMessage
from vumi.tests.utils import get_stubbed_worker, MockHttpServer


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
        self.worker_url = self.worker.get_transport_url()

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
