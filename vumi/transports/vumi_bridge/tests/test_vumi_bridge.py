import base64
import json

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.error import ConnectionLost
from twisted.internet.task import Clock
from twisted.web.server import NOT_DONE_YET
from twisted.python.failure import Failure

from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import MockHttpServer
from vumi.transports.tests.helpers import TransportHelper
from vumi.transports.vumi_bridge import (
    GoConversationClientTransport, GoConversationServerTransport)
from vumi.config import ConfigError
from vumi.utils import http_request_full


class TestGoConversationTransportBase(VumiTestCase):

    transport_class = None

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.mock_server = MockHttpServer(self.handle_inbound_request)
        self.add_cleanup(self.mock_server.stop)
        self.clock = Clock()

        yield self.mock_server.start()

        self._pending_reqs = []
        self.add_cleanup(self.finish_requests)

    @inlineCallbacks
    def get_transport(self, **config):
        defaults = {
            'base_url': self.mock_server.url,
            'account_key': 'account-key',
            'conversation_key': 'conversation-key',
            'access_token': 'access-token',
        }
        defaults.update(config)
        transport = yield self.tx_helper.get_transport(defaults)
        yield self.setup_transport(transport)
        returnValue(transport)

    def setup_transport(self, transport):
        pass

    @inlineCallbacks
    def finish_requests(self):
        for req in self._pending_reqs:
            if not req.finished:
                yield req.finish()

    def handle_inbound_request(self, request):
        self.mock_server.queue.put(request)
        return NOT_DONE_YET

    @inlineCallbacks
    def get_next_request(self):
        req = yield self.mock_server.queue.get()
        self._pending_reqs.append(req)
        returnValue(req)


class TestGoConversationTransport(TestGoConversationTransportBase):

    transport_class = GoConversationClientTransport

    @inlineCallbacks
    def setup_transport(self, transport):
        transport.clock = self.clock
        # when the transport fires up it starts two new connections,
        # wait for them & name them accordingly
        reqs = []
        reqs.append((yield self.get_next_request()))
        reqs.append((yield self.get_next_request()))
        if reqs[0].path.endswith('messages.json'):
            self.message_req = reqs[0]
            self.event_req = reqs[1]
        else:
            self.message_req = reqs[1]
            self.event_req = reqs[0]
        # put some data on the wire to have connectionMade called
        self.message_req.write('')
        self.event_req.write('')

    @inlineCallbacks
    def test_auth_headers(self):
        yield self.get_transport()
        [msg_auth_header] = self.message_req.requestHeaders.getRawHeaders(
            'Authorization')
        self.assertEqual(msg_auth_header, 'Basic %s' % (
            base64.b64encode('account-key:access-token')))
        [event_auth_header] = self.event_req.requestHeaders.getRawHeaders(
            'Authorization')
        self.assertEqual(event_auth_header, 'Basic %s' % (
            base64.b64encode('account-key:access-token')))

    @inlineCallbacks
    def test_req_path(self):
        yield self.get_transport()
        self.assertEqual(
            self.message_req.path,
            '/conversation-key/messages.json')
        self.assertEqual(
            self.event_req.path,
            '/conversation-key/events.json')

    @inlineCallbacks
    def test_receiving_messages(self):
        yield self.get_transport()
        msg = self.tx_helper.make_inbound("inbound")
        self.message_req.write(msg.to_json().encode('utf-8') + '\n')
        [received_msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(received_msg['message_id'], msg['message_id'])

    @inlineCallbacks
    def test_receiving_events(self):
        transport = yield self.get_transport()
        # prime the mapping
        yield transport.map_message_id('remote', 'local')
        ack = self.tx_helper.make_ack(event_id='event-id')
        ack['user_message_id'] = 'remote'
        self.event_req.write(ack.to_json().encode('utf-8') + '\n')
        [received_ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(received_ack['event_id'], ack['event_id'])
        self.assertEqual(received_ack['user_message_id'], 'local')
        self.assertEqual(received_ack['sent_message_id'], 'remote')

    @inlineCallbacks
    def test_sending_messages(self):
        yield self.get_transport()
        msg = self.tx_helper.make_outbound(
            "outbound", session_event=TransportUserMessage.SESSION_CLOSE)
        d = self.tx_helper.dispatch_outbound(msg)
        req = yield self.get_next_request()
        received_msg = json.loads(req.content.read())
        self.assertEqual(received_msg, {
            'content': msg['content'],
            'in_reply_to': None,
            'to_addr': msg['to_addr'],
            'message_id': msg['message_id'],
            'session_event': TransportUserMessage.SESSION_CLOSE,
            'helper_metadata': {},
        })

        remote_id = TransportUserMessage.generate_id()
        reply = msg.copy()
        reply['message_id'] = remote_id
        req.write(reply.to_json().encode('utf-8'))
        req.finish()
        yield d

        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(ack['user_message_id'], msg['message_id'])
        self.assertEqual(ack['sent_message_id'], remote_id)

    @inlineCallbacks
    def test_reconnecting(self):
        transport = yield self.get_transport()
        message_client = transport.message_client
        message_client.connectionLost(Failure(ConnectionLost('foo')))

        config = transport.get_static_config()

        self.assertTrue(transport.delay > config.initial_delay)
        self.assertEqual(transport.retries, 1)
        self.assertTrue(transport.reconnect_call)
        self.clock.advance(transport.delay + 0.1)

        # write something to ensure connectionMade() is called on
        # the protocol
        message_req = yield self.get_next_request()
        message_req.write('')

        event_req = yield self.get_next_request()
        event_req.write('')

        self.assertEqual(transport.delay, config.initial_delay)
        self.assertEqual(transport.retries, 0)
        self.assertFalse(transport.reconnect_call)


class TestGoConversationServerTransport(TestGoConversationTransportBase):

    transport_class = GoConversationServerTransport

    def test_server_settings_without_configs(self):
        return self.assertFailure(self.get_transport(), ConfigError)

    def get_configured_transport(self):
        return self.get_transport(
            message_path='messages.json', event_path='events.json',
            web_port='0')

    def post_msg(self, url, msg_json):
        data = msg_json.encode('utf-8')
        return http_request_full(
            url.encode('utf-8'), data=data, headers={
                'Content-Type': 'application/json; charset=utf-8',
            })

    @inlineCallbacks
    def test_receiving_messages(self):
        transport = yield self.get_configured_transport()
        url = transport.get_transport_url('messages.json')
        msg = self.tx_helper.make_inbound("inbound")
        resp = yield self.post_msg(url, msg.to_json())
        self.assertEqual(resp.code, 200)
        [received_msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(received_msg['message_id'], msg['message_id'])

    @inlineCallbacks
    def test_receive_bad_message(self):
        transport = yield self.get_configured_transport()
        url = transport.get_transport_url('messages.json')
        resp = yield self.post_msg(url, 'This is not JSON.')
        self.assertEqual(resp.code, 400)
        [failure] = self.flushLoggedErrors()
        self.assertTrue('No JSON object' in str(failure))

    @inlineCallbacks
    def test_receiving_events(self):
        transport = yield self.get_configured_transport()
        url = transport.get_transport_url('events.json')
        # prime the mapping
        yield transport.map_message_id('remote', 'local')
        ack = self.tx_helper.make_ack(event_id='event-id')
        ack['user_message_id'] = 'remote'
        resp = yield self.post_msg(url, ack.to_json())
        self.assertEqual(resp.code, 200)
        [received_ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(received_ack['event_id'], ack['event_id'])
        self.assertEqual(received_ack['user_message_id'], 'local')
        self.assertEqual(received_ack['sent_message_id'], 'remote')

    @inlineCallbacks
    def test_receive_bad_event(self):
        transport = yield self.get_configured_transport()
        url = transport.get_transport_url('events.json')
        resp = yield self.post_msg(url, 'This is not JSON.')
        self.assertEqual(resp.code, 400)
        [failure] = self.flushLoggedErrors()
        self.assertTrue('No JSON object' in str(failure))

    @inlineCallbacks
    def test_sending_messages(self):
        yield self.get_configured_transport()
        msg = self.tx_helper.make_outbound(
            "outbound", session_event=TransportUserMessage.SESSION_CLOSE)
        d = self.tx_helper.dispatch_outbound(msg)
        req = yield self.get_next_request()
        received_msg = json.loads(req.content.read())
        self.assertEqual(received_msg, {
            'content': msg['content'],
            'in_reply_to': None,
            'to_addr': msg['to_addr'],
            'message_id': msg['message_id'],
            'session_event': TransportUserMessage.SESSION_CLOSE,
            'helper_metadata': {},
        })

        remote_id = TransportUserMessage.generate_id()
        reply = msg.copy()
        reply['message_id'] = remote_id
        req.write(reply.to_json().encode('utf-8'))
        req.finish()
        yield d

        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(ack['user_message_id'], msg['message_id'])
        self.assertEqual(ack['sent_message_id'], remote_id)
