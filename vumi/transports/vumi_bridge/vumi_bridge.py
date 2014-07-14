# -*- test-case-name: vumi.transports.vumi_bridge.tests.test_vumi_bridge -*-

import base64
import json
import random

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.web.http_headers import Headers
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports import Transport
from vumi.transports.vumi_bridge.client import StreamingClient
from vumi.config import ConfigText, ConfigDict, ConfigInt, ConfigFloat
from vumi.persist.txredis_manager import TxRedisManager
from vumi.message import TransportUserMessage, TransportEvent
from vumi.utils import to_kwargs, http_request_full
from vumi import log


class VumiBridgeClientTransportConfig(Transport.CONFIG_CLASS):
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
    max_reconnect_delay = ConfigInt(
        'Maximum number of seconds between connection attempts', default=3600,
        static=True)
    max_retries = ConfigInt(
        'Maximum number of consecutive unsuccessful connection attempts '
        'after which no further connection attempts will be made. If this is '
        'not explicitly set, no maximum is applied',
        static=True)
    initial_delay = ConfigFloat(
        'Initial delay for first reconnection attempt', default=0.1,
        static=True)
    factor = ConfigFloat(
        'A multiplicitive factor by which the delay grows',
        # (math.e)
        default=2.7182818284590451,
        static=True)
    jitter = ConfigFloat(
        'Percentage of randomness to introduce into the delay length'
        'to prevent stampeding.',
        # molar Planck constant times c, joule meter/mole
        default=0.11962656472,
        static=True)


class VumiBridgeServerTransportConfig(VumiBridgeClientTransportConfig):
    # Most of this copied wholesale from vumi.transports.httprpc.

    web_port = ConfigInt(
        "The port to listen for requests on, defaults to `0`.",
        default=0, static=True)
    message_path = ConfigText(
        "The path to listen for message requests on.", required=True,
        static=True)
    event_path = ConfigText(
        "The path to listen for event requests on.", required=True,
        static=True)
    health_path = ConfigText(
        "The path to listen for downstream health checks on"
        " (useful with HAProxy)", default='health', static=True)


class GoConversationTransportBase(Transport):

    def get_url(self, path):
        config = self.get_static_config()
        url = '/'.join([
            config.base_url.rstrip('/'), config.conversation_key, path])
        return url

    @inlineCallbacks
    def map_message_id(self, remote_message_id, local_message_id):
        config = self.get_static_config()
        yield self.redis.set(remote_message_id, local_message_id)
        yield self.redis.expire(remote_message_id, config.message_life_time)

    def get_message_id(self, remote_message_id):
        return self.redis.get(remote_message_id)

    def handle_inbound_message(self, message):
        return self.publish_message(**message.payload)

    @inlineCallbacks
    def handle_inbound_event(self, event):
        remote_message_id = event['user_message_id']
        local_message_id = yield self.get_message_id(remote_message_id)
        event['user_message_id'] = local_message_id
        event['sent_message_id'] = remote_message_id
        yield self.publish_event(**event.payload)

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
        if 'helper_metadata' in message:
            params['helper_metadata'] = message['helper_metadata']

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
            return

        remote_message = json.loads(resp.delivered_body)
        yield self.map_message_id(
            remote_message['message_id'], message['message_id'])
        yield self.publish_ack(user_message_id=message['message_id'],
                               sent_message_id=remote_message['message_id'])

    def get_auth_headers(self):
        config = self.get_static_config()
        return {
            'Authorization': ['Basic ' + base64.b64encode('%s:%s' % (
                config.account_key, config.access_token))],
        }


class GoConversationClientTransport(GoConversationTransportBase):
    """
    This transport essentially connects as a client to Vumi Go's streaming
    HTTP API [1]_.

    It allows one to bridge Vumi and Vumi Go installations.

    NOTE:   Since we're basically bridging two separate installations we're
            leaving some of the attributes that we would normally change the
            same. Specifically `transport_type`.

    .. [1] https://github.com/praekelt/vumi-go/blob/develop/docs/http_api.rst

    """

    CONFIG_CLASS = VumiBridgeClientTransportConfig
    continue_trying = True
    clock = reactor

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        self.redis = yield TxRedisManager.from_config(
            config.redis_manager)
        self.retries = 0
        self.delay = config.initial_delay
        self.reconnect_call = None
        self.client = StreamingClient()
        self.connect_api_clients()

    def teardown_transport(self):
        if self.reconnect_call:
            self.reconnect_call.cancel()
            self.reconnect_call = None
        self.continue_trying = False
        self.disconnect_api_clients()

    def connect_api_clients(self):
        self.message_client = self.client.stream(
            TransportUserMessage, self.handle_inbound_message,
            log.error, self.get_url('messages.json'),
            headers=Headers(self.get_auth_headers()),
            on_connect=self.reset_reconnect_delay,
            on_disconnect=self.reconnect_api_clients)
        self.event_client = self.client.stream(
            TransportEvent, self.handle_inbound_event,
            log.error, self.get_url('events.json'),
            headers=Headers(self.get_auth_headers()),
            on_connect=self.reset_reconnect_delay,
            on_disconnect=self.reconnect_api_clients)

    def reconnect_api_clients(self, reason):
        self.disconnect_api_clients()
        if not self.continue_trying:
            log.msg('Not retrying because of explicit request')
            return

        config = self.get_static_config()
        self.retries += 1
        if (config.max_retries is not None
                and (self.retries > config.max_retries)):
            log.warning('Abandoning reconnecting after %s attempts.' % (
                self.retries))
            return

        self.delay = min(self.delay * config.factor,
                         config.max_reconnect_delay)
        if config.jitter:
            self.delay = random.normalvariate(self.delay,
                                              self.delay * config.jitter)
        log.msg('Will retry in %s seconds' % (self.delay,))
        self.reconnect_call = self.clock.callLater(self.delay,
                                                   self.connect_api_clients)

    def reset_reconnect_delay(self):
        config = self.get_static_config()
        self.delay = config.initial_delay
        self.retries = 0
        self.reconnect_call = None
        self.continue_trying = True

    def disconnect_api_clients(self):
        self.message_client.disconnect()
        self.event_client.disconnect()


class GoConversationTransport(GoConversationClientTransport):

    def setup_transport(self, *args, **kwargs):
        log.warning(
            'GoConversationTransport is deprecated, please use '
            '`GoConversationClientTransport` instead.')
        return super(GoConversationTransport, self).setup_transport(
            *args, **kwargs)


class GoConversationHealthResource(Resource):
    # Most of this copied wholesale from vumi.transports.httprpc.
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render_GET(self, request):
        request.setResponseCode(http.OK)
        request.do_not_log = True
        return self.transport.get_health_response()


class GoConversationResource(Resource):
    # Most of this copied wholesale from vumi.transports.httprpc.
    isLeaf = True

    def __init__(self, callback):
        self.callback = callback
        Resource.__init__(self)

    def render_(self, request, request_id=None):
        request.setHeader("content-type", 'application/json; charset=utf-8')
        self.callback(request)
        return NOT_DONE_YET

    def render_PUT(self, request):
        return self.render_(request)

    def render_POST(self, request):
        return self.render_(request)


class GoConversationServerTransport(GoConversationTransportBase):
    # Most of this copied wholesale from vumi.transports.httprpc.

    CONFIG_CLASS = VumiBridgeServerTransportConfig

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        self.redis = yield TxRedisManager.from_config(
            config.redis_manager)

        self.web_resource = yield self.start_web_resources([
            (GoConversationResource(self.handle_raw_inbound_message),
             config.message_path),
            (GoConversationResource(self.handle_raw_inbound_event),
             config.event_path),
            (GoConversationHealthResource(self), config.health_path),
        ], config.web_port)

    def teardown_transport(self):
        return self.web_resource.loseConnection()

    def get_transport_url(self, suffix=''):
        """
        Get the URL for the HTTP resource. Requires the worker to be started.

        This is mostly useful in tests, and probably shouldn't be used
        in non-test code, because the API might live behind a load
        balancer or proxy.
        """
        addr = self.web_resource.getHost()
        return "http://%s:%s/%s" % (addr.host, addr.port, suffix.lstrip('/'))

    @inlineCallbacks
    def handle_raw_inbound_event(self, request):
        try:
            data = json.loads(request.content.read())
            msg = TransportEvent(_process_fields=True, **to_kwargs(data))
            yield self.handle_inbound_event(msg)
            request.finish()
        except Exception as e:
            log.err(e)
            request.setResponseCode(400)
            request.finish()

    @inlineCallbacks
    def handle_raw_inbound_message(self, request):
        try:
            data = json.loads(request.content.read())
            msg = TransportUserMessage(
                _process_fields=True, **to_kwargs(data))
            yield self.handle_inbound_message(msg)
            request.finish()
        except Exception as e:
            log.err(e)
            request.setResponseCode(400)
            request.finish()
