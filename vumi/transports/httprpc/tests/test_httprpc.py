import json

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.internet.base import DelayedCall

from vumi.utils import http_request
from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.httprpc import HttpRpcTransport
from vumi.message import TransportUserMessage
from vumi.tests.utils import MockHttpServer


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


class TestTransport(TransportTestCase):

    transport_class = OkTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TestTransport, self).setUp()
        config = {
            'web_path': "foo",
            'web_port': 0,
            'username': 'testuser',
            'password': 'testpass',
            }
        self.transport = yield self.get_transport(config)
        self.transport_url = self.transport.get_transport_url()

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(self.transport_url + "health", "",
                                    method='GET')
        self.assertEqual(json.loads(result), {
            'pending_requests': 0
        })

    @inlineCallbacks
    def test_inbound(self):
        d = http_request(self.transport_url + "foo", '', method='GET')
        msg, = yield self.wait_for_dispatched_messages(1)
        payload = msg.payload
        tum = TransportUserMessage(**payload)
        rep = tum.reply("OK")
        yield self.dispatch(rep)
        response = yield d
        self.assertEqual(response, 'OK')


class JSONTransport(HttpRpcTransport):

    def handle_raw_inbound_message(self, msgid, request):
        request_content = json.loads(request.content.read())
        self.publish_message(
                message_id=msgid,
                content=request_content['content'],
                to_addr=request_content['to_addr'],
                from_addr=request_content['from_addr'],
                provider='',
                session_event=TransportUserMessage.SESSION_NEW,
                transport_name=self.transport_name,
                transport_type=self.config.get('transport_type'),
                transport_metadata={},
                )


class TestJSONTransport(TransportTestCase):

    transport_class = JSONTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TestJSONTransport, self).setUp()
        config = {
            'web_path': "foo",
            'web_port': 0,
            'username': 'testuser',
            'password': 'testpass',
            }
        self.transport = yield self.get_transport(config)
        self.transport_url = self.transport.get_transport_url()

    @inlineCallbacks
    def test_inbound(self):
        d = http_request(self.transport_url + "foo",
                '{"content": "hello",'
                ' "to_addr": "the_app",'
                ' "from_addr": "some_msisdn"'
                '}',
                method='POST')
        msg, = yield self.wait_for_dispatched_messages(1)
        payload = msg.payload
        self.assertEqual(payload['content'], 'hello')
        self.assertEqual(payload['to_addr'], 'the_app')
        self.assertEqual(payload['from_addr'], 'some_msisdn')
        tum = TransportUserMessage(**payload)
        rep = tum.reply('{"content": "bye"}')
        yield self.dispatch(rep)
        response = yield d
        self.assertEqual(response, '{"content": "bye"}')
