import json
import os

from twisted.internet.defer import inlineCallbacks, returnValue, DeferredQueue
from twisted.internet.task import Clock
from twisted.web.client import Agent
from twisted.web.server import NOT_DONE_YET

import certifi

from vumi.message import TransportUserMessage
from vumi.tests.fake_connection import FakeHttpServer
from vumi.tests.helpers import VumiTestCase
from vumi.transports.tests.helpers import TransportHelper
from vumi.transports.vumi_bridge import GoConversationTransport
from vumi.config import ConfigError
from vumi.utils import http_request_full


class TestGoConversationTransportBase(VumiTestCase):

    transport_class = None

    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.fake_http = FakeHttpServer(self.handle_inbound_request)
        self.clock = Clock()
        self._request_queue = DeferredQueue()
        self._pending_reqs = []
        self.add_cleanup(self.finish_requests)

    @inlineCallbacks
    def get_transport(self, start=True, **config):
        defaults = {
            'account_key': 'account-key',
            'conversation_key': 'conversation-key',
            'access_token': 'access-token',
            'publish_status': True,
        }
        defaults.update(config)
        transport = yield self.tx_helper.get_transport(defaults, start=False)
        transport.agent_factory = self.fake_http.get_agent
        if start:
            yield transport.startWorker()
        returnValue(transport)

    @inlineCallbacks
    def finish_requests(self):
        for req in self._pending_reqs:
            if not req.finished:
                yield req.finish()

    def handle_inbound_request(self, request):
        self._request_queue.put(request)
        return NOT_DONE_YET

    @inlineCallbacks
    def get_next_request(self):
        req = yield self._request_queue.get()
        self._pending_reqs.append(req)
        returnValue(req)


class TestGoConversationTransport(TestGoConversationTransportBase):

    transport_class = GoConversationTransport

    def test_server_settings_without_configs(self):
        return self.assertFailure(self.get_transport(), ConfigError)

    def get_configured_transport(self, start=True):
        return self.get_transport(start=start, web_path='test', web_port='0')

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

        [status] = yield self.tx_helper.wait_for_dispatched_statuses(1)
        self.assertEquals(status['status'], 'ok')
        self.assertEquals(status['component'], 'received-from-vumi-go')
        self.assertEquals(status['type'], 'good_request')
        self.assertEquals(status['message'], 'Good request received')

    @inlineCallbacks
    def test_receive_bad_message(self):
        transport = yield self.get_configured_transport()
        url = transport.get_transport_url('messages.json')
        resp = yield self.post_msg(url, 'This is not JSON.')
        self.assertEqual(resp.code, 400)
        [failure] = self.flushLoggedErrors()
        self.assertTrue('No JSON object' in str(failure))

        [status] = yield self.tx_helper.wait_for_dispatched_statuses(1)
        self.assertEquals(status['status'], 'down')
        self.assertEquals(status['component'], 'received-from-vumi-go')
        self.assertEquals(status['type'], 'bad_request')
        self.assertEquals(status['message'], 'Bad request received')

    @inlineCallbacks
    def test_receiving_ack_events(self):
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

        statuses = yield self.tx_helper.wait_for_dispatched_statuses(1)
        self.assertEqual(len(statuses), 2)
        self.assertEquals(statuses[0]['status'], 'ok')
        self.assertEquals(statuses[0]['component'], 'sent-by-vumi-go')
        self.assertEquals(statuses[0]['type'], 'vumi_go_sent')
        self.assertEquals(statuses[0]['message'], 'Sent by Vumi Go')
        self.assertEquals(statuses[1]['status'], 'ok')
        self.assertEquals(statuses[1]['component'], 'vumi-go-event')
        self.assertEquals(statuses[1]['type'], 'good_request')
        self.assertEquals(statuses[1]['message'],
                          'Good event received from Vumi Go')

    @inlineCallbacks
    def test_receiving_nack_events(self):
        transport = yield self.get_configured_transport()
        url = transport.get_transport_url('events.json')
        # prime the mapping
        yield transport.map_message_id('remote', 'local')
        nack = self.tx_helper.make_nack(event_id='event-id')
        nack['user_message_id'] = 'remote'
        resp = yield self.post_msg(url, nack.to_json())
        self.assertEqual(resp.code, 200)
        [received_nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(received_nack['event_id'], nack['event_id'])
        self.assertEqual(received_nack['user_message_id'], 'local')
        self.assertEqual(received_nack['sent_message_id'], 'remote')

        statuses = yield self.tx_helper.wait_for_dispatched_statuses(1)
        self.assertEqual(len(statuses), 2)
        self.assertEquals(statuses[0]['status'], 'down')
        self.assertEquals(statuses[0]['component'], 'sent-by-vumi-go')
        self.assertEquals(statuses[0]['type'], 'vumi_go_failed')
        self.assertEquals(statuses[0]['message'], 'Vumi Go failed to send')
        self.assertEquals(statuses[1]['status'], 'ok')
        self.assertEquals(statuses[1]['component'], 'vumi-go-event')
        self.assertEquals(statuses[1]['type'], 'good_request')
        self.assertEquals(statuses[1]['message'],
                          'Good event received from Vumi Go')

    @inlineCallbacks
    def test_receive_bad_event(self):
        transport = yield self.get_configured_transport()
        url = transport.get_transport_url('events.json')
        resp = yield self.post_msg(url, 'This is not JSON.')
        self.assertEqual(resp.code, 400)
        [failure] = self.flushLoggedErrors()
        self.assertTrue('No JSON object' in str(failure))

        [status] = yield self.tx_helper.wait_for_dispatched_statuses(1)
        self.assertEquals(status['status'], 'down')
        self.assertEquals(status['component'], 'vumi-go-event')
        self.assertEquals(status['type'], 'bad_request')
        self.assertEquals(status['message'], 'Bad event received from Vumi Go')

    @inlineCallbacks
    def test_weak_cacerts_installed(self):
        yield self.get_configured_transport()
        self.assertEqual(os.environ["SSL_CERT_FILE"], certifi.old_where())

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

        [status] = yield self.tx_helper.wait_for_dispatched_statuses(1)
        self.assertEquals(status['status'], 'ok')
        self.assertEquals(status['component'], 'submitted-to-vumi-go')
        self.assertEquals(status['type'], 'good_request')
        self.assertEquals(status['message'], 'Message accepted by Vumi Go')

    @inlineCallbacks
    def test_sending_bad_messages(self):
        yield self.get_configured_transport()
        msg = self.tx_helper.make_outbound(
            "outbound", session_event=TransportUserMessage.SESSION_CLOSE)
        self.tx_helper.dispatch_outbound(msg)
        req = yield self.get_next_request()
        req.setResponseCode(400, "Bad Request")

        req.finish()

        [status] = yield self.tx_helper.wait_for_dispatched_statuses(1)
        self.assertEquals(status['status'], 'down')
        self.assertEquals(status['component'], 'submitted-to-vumi-go')
        self.assertEquals(status['type'], 'bad_request')
        self.assertEquals(status['message'],
                          'Message submission rejected by Vumi Go')

    @inlineCallbacks
    def test_teardown_before_start(self):
        transport = yield self.get_configured_transport(start=False)
        yield transport.teardown_transport()

    def test_agent_factory_default(self):
        self.assertTrue(isinstance(
            GoConversationTransport.agent_factory(), Agent))
