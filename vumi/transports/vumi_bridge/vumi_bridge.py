# -*- test-case-name: vumi.transports.vumi_bridge.tests.test_vumi_bridge -*-

import base64
import json

from twisted.internet.defer import inlineCallbacks
from twisted.web.http_headers import Headers
from twisted.web import http

from vumi.transports import Transport
from vumi.transports.vumi_bridge.client import StreamingClient
from vumi.config import ConfigText, ConfigDict, ConfigInt
from vumi.persist.txredis_manager import TxRedisManager
from vumi.message import TransportUserMessage, TransportEvent
from vumi.utils import http_request_full
from vumi import log


class VumiBridgeTransportConfig(Transport.CONFIG_CLASS):
    account_key = ConfigText(
        'The account key to connect with.', static=True, required=True)
    conversation_key = ConfigText(
        'The conversation key to use.', static=True, required=True)
    access_token = ConfigText(
        'The access token for the conversation key.', static=True,
        required=True)
    base_url = ConfigText(
        'The base URL for the API', static=True,
        default='https://go.vumi.org/api/v1/go/http_api/')
    message_life_time = ConfigInt(
        'How long to keep message_ids around for.', static=True,
        default=48 * 60 * 60)  # default is 48 hours.
    redis_manager = ConfigDict(
        "Redis client configuration.", default={}, static=True)


class GoConversationTransport(Transport):
    """
    This transport essentially connects as a client to Vumi Go's streaming
    HTTP API [1]_.

    It allows one to bridge Vumi and Vumi Go installations.

    NOTE:   Since we're basically bridging two separate installations we're
            leaving some of the attributes that we would normally change the
            same. Specifically `transport_type`.

    .. [1] https://github.com/praekelt/vumi-go/blob/develop/docs/http_api.rst

    """

    CONFIG_CLASS = VumiBridgeTransportConfig

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        self.redis = yield TxRedisManager.from_config(
            config.redis_manager)
        self.client = StreamingClient()
        self.connect_api_clients()

    def connect_api_clients(self):
        self.message_client = self.client.stream(
            TransportUserMessage, self.handle_inbound_message,
            log.error, self.get_url('messages.json'),
            headers=Headers(self.get_auth_headers()),
            on_disconnect=self.reconnect_api_clients)
        self.event_client = self.client.stream(
            TransportEvent, self.handle_inbound_event,
            log.error, self.get_url('events.json'),
            headers=Headers(self.get_auth_headers()),
            on_disconnect=self.reconnect_api_clients)

    def reconnect_api_clients(self, reason):
        self.disconnect_api_clients()
        self.connect_api_clients()

    def disconnect_api_clients(self):
        self.message_client.disconnect()
        self.event_client.disconnect()

    def get_auth_headers(self):
        config = self.get_static_config()
        return {
            'Authorization': ['Basic ' + base64.b64encode('%s:%s' % (
                config.account_key, config.access_token))],
        }

    def get_url(self, path):
        config = self.get_static_config()
        url = '/'.join([
            config.base_url.rstrip('/'), config.conversation_key, path])
        return url

    def teardown_transport(self):
        self.disconnect_api_clients()

    @inlineCallbacks
    def map_message_id(self, remote_message_id, local_message_id):
        config = self.get_static_config()
        yield self.redis.set(remote_message_id, local_message_id)
        yield self.redis.expire(remote_message_id, config.message_life_time)

    def get_message_id(self, remote_message_id):
        return self.redis.get(remote_message_id)

    @inlineCallbacks
    def handle_outbound_message(self, message):
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
        }
        headers.update(self.get_auth_headers())

        params = {
            'to_addr': message['to_addr'],
            'content': message['content'],
            'message_id': message['message_id'],
            'in_reply_to': message['in_reply_to'],
            'session_event': message['session_event']
        }

        resp = yield http_request_full(
            self.get_url('messages.json'),
            data=json.dumps(params).encode('utf-8'),
            headers=headers,
            method='PUT')

        if resp.code != http.OK:
            log.warning('Unexpected status code: %s, body: %s' % (
                resp.code, resp.delivered_body))
            yield self.publish_nack(message['message_id'],
                                    reason='Unexpected status code: %s' % (
                                        resp.code,))
            print 'nack sent!'
            return

        remote_message = json.loads(resp.delivered_body)
        yield self.map_message_id(
            remote_message['message_id'], message['message_id'])
        yield self.publish_ack(user_message_id=message['message_id'],
                               sent_message_id=remote_message['message_id'])

    def handle_inbound_message(self, message):
        return self.publish_message(**message.payload)

    @inlineCallbacks
    def handle_inbound_event(self, event):
        remote_message_id = event['user_message_id']
        local_message_id = yield self.get_message_id(remote_message_id)
        event['user_message_id'] = local_message_id
        event['sent_message_id'] = remote_message_id
        yield self.publish_event(**event.payload)
