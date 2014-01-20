import json

from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import Clock

from vumi.utils import http_request, http_request_full, basic_auth_string
from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import LogCatcher
from vumi.transports.httprpc import HttpRpcTransport
from vumi.message import TransportUserMessage
from vumi.transports.tests.helpers import TransportHelper


class OkTransport(HttpRpcTransport):

    def handle_raw_inbound_message(self, msgid, request):
        self.publish_message(
                message_id=msgid,
                content='',
                to_addr='to_addr',
                from_addr='',
                provider='',
                session_event=TransportUserMessage.SESSION_NEW,
                transport_name=self.transport_name,
                transport_type=self.config.get('transport_type'),
                transport_metadata={},
                )


class TestTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.clock = Clock()
        self.patch(OkTransport, 'get_clock', lambda _: self.clock)
        config = {
            'web_path': "foo",
            'web_port': 0,
            'request_timeout': 10,
            'request_timeout_status_code': 418,
            'request_timeout_body': 'I am a teapot',
            }
        self.tx_helper = self.add_helper(TransportHelper(OkTransport))
        self.transport = yield self.tx_helper.get_transport(config)
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
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        rep = yield self.tx_helper.make_dispatch_reply(msg, "OK")
        response = yield d
        self.assertEqual(response, 'OK')
        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(ack['user_message_id'], rep['message_id'])
        self.assertEqual(ack['sent_message_id'], rep['message_id'])

    @inlineCallbacks
    def test_nack(self):
        msg = yield self.tx_helper.make_dispatch_outbound("outbound")
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'], 'Missing fields: in_reply_to')

    @inlineCallbacks
    def test_timeout(self):
        d = http_request_full(self.transport_url + "foo", '', method='GET')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        with LogCatcher(message='Timing') as lc:
            self.clock.advance(10.1)  # .1 second after timeout
            response = yield d
            [warning] = lc.messages()
            self.assertEqual(warning, 'Timing out to_addr')
        self.assertEqual(response.delivered_body, 'I am a teapot')
        self.assertEqual(response.code, 418)


class TestTransportWithAuthentication(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.clock = Clock()
        self.patch(OkTransport, 'get_clock', lambda _: self.clock)
        config = {
            'web_path': "foo",
            'web_port': 0,
            'web_username': 'user-1',
            'web_password': 'pass-secret',
            'web_auth_domain': 'Mordor',
            'request_timeout': 10,
            'request_timeout_status_code': 418,
            'request_timeout_body': 'I am a teapot',
            }
        self.tx_helper = self.add_helper(TransportHelper(OkTransport))
        self.transport = yield self.tx_helper.get_transport(config)
        self.transport_url = self.transport.get_transport_url()

    @inlineCallbacks
    def test_health_doesnt_require_auth(self):
        result = yield http_request(self.transport_url + "health", "",
                                    method='GET')
        self.assertEqual(json.loads(result), {
            'pending_requests': 0
        })

    @inlineCallbacks
    def test_inbound_with_successful_auth(self):
        headers = {
            'Authorization': basic_auth_string("user-1", "pass-secret")
        }
        d = http_request(self.transport_url + "foo", '',
                         headers=headers, method='GET')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        rep = yield self.tx_helper.make_dispatch_reply(msg, "OK")
        response = yield d
        self.assertEqual(response, 'OK')
        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(ack['user_message_id'], rep['message_id'])
        self.assertEqual(ack['sent_message_id'], rep['message_id'])

    @inlineCallbacks
    def test_inbound_with_failed_auth(self):
        headers = {
            'Authorization': basic_auth_string("user-1", "bad-pass")
        }
        d = http_request(self.transport_url + "foo", '',
                         headers=headers, method='GET')
        response = yield d
        self.assertEqual(response, 'Unauthorized')

    @inlineCallbacks
    def test_inbound_without_auth(self):
        d = http_request(self.transport_url + "foo", '', method='GET')
        response = yield d
        self.assertEqual(response, 'Unauthorized')


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


class TestJSONTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        config = {
            'web_path': "foo",
            'web_port': 0,
            }
        self.tx_helper = self.add_helper(TransportHelper(JSONTransport))
        self.transport = yield self.tx_helper.get_transport(config)
        self.transport_url = self.transport.get_transport_url()

    @inlineCallbacks
    def test_inbound(self):
        d = http_request(self.transport_url + "foo",
                '{"content": "hello",'
                ' "to_addr": "the_app",'
                ' "from_addr": "some_msisdn"'
                '}',
                method='POST')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], 'hello')
        self.assertEqual(msg['to_addr'], 'the_app')
        self.assertEqual(msg['from_addr'], 'some_msisdn')
        yield self.tx_helper.make_dispatch_reply(msg, '{"content": "bye"}')
        response = yield d
        self.assertEqual(response, '{"content": "bye"}')


class CustomOutboundTransport(OkTransport):
    RESPONSE_HEADERS = {
        'Darth-Vader': ["Anakin Skywalker"],
        'Admiral-Ackbar': ["It's a trap!", "Shark"]
    }

    def handle_outbound_message(self, message):
        self.finish_request(
                message.payload['in_reply_to'],
                message.payload['content'].encode('utf-8'),
                headers=self.RESPONSE_HEADERS)


class TestCustomOutboundTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        config = {
            'web_path': "foo",
            'web_port': 0,
            'username': 'testuser',
            'password': 'testpass',
            }
        self.tx_helper = self.add_helper(
            TransportHelper(CustomOutboundTransport))
        self.transport = yield self.tx_helper.get_transport(config)
        self.transport_url = self.transport.get_transport_url()

    @inlineCallbacks
    def test_optional_headers(self):
        d = http_request_full(self.transport_url + "foo", '', method='GET')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        yield self.tx_helper.make_dispatch_reply(msg, "OK")

        response = yield d
        self.assertEqual(
            response.headers.getRawHeaders('Darth-Vader'),
            ["Anakin Skywalker"])
        self.assertEqual(
            response.headers.getRawHeaders('Admiral-Ackbar'),
            ["It's a trap!", "Shark"])
