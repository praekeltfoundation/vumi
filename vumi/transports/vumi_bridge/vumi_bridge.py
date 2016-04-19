# -*- test-case-name: vumi.transports.vumi_bridge.tests.test_vumi_bridge -*-

import base64
import json
import os

import certifi

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.web.client import Agent

from treq.client import HTTPClient

from vumi.transports import Transport
from vumi.config import ConfigText, ConfigDict, ConfigInt, ConfigFloat
from vumi.persist.txredis_manager import TxRedisManager
from vumi.message import TransportUserMessage, TransportEvent
from vumi.utils import to_kwargs, StatusEdgeDetector
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
        default='https://go.vumi.org/api/v1/go/http_api_nostream/')
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
    web_port = ConfigInt(
        "The port to listen for requests on, defaults to `0`.",
        default=0, static=True)
    web_path = ConfigText(
        "The path to listen for inbound requests on.", required=True,
        static=True)
    health_path = ConfigText(
        "The path to listen for downstream health checks on"
        " (useful with HAProxy)", default='health', static=True)


class GoConversationTransportBase(Transport):

    @classmethod
    def agent_factory(cls):
        """For swapping out the Agent we use in tests."""
        return Agent(reactor)

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

        http_client = HTTPClient(self.agent_factory())
        resp = yield http_client.put(
            self.get_url('messages.json'),
            data=json.dumps(params).encode('utf-8'),
            headers=headers)
        resp_body = yield resp.content()

        if resp.code != http.OK:
            log.warning('Unexpected status code: %s, body: %s' % (
                resp.code, resp_body))
            self.update_status(
                status='down', component='submitted-to-vumi-go',
                type='bad_request',
                message='Message submission rejected by Vumi Go')
            yield self.publish_nack(message['message_id'],
                                    reason='Unexpected status code: %s' % (
                                        resp.code,))
            return

        remote_message = json.loads(resp_body)
        yield self.map_message_id(
            remote_message['message_id'], message['message_id'])
        self.update_status(
            status='ok', component='submitted-to-vumi-go',
            type='good_request', message='Message accepted by Vumi Go')
        yield self.publish_ack(user_message_id=message['message_id'],
                               sent_message_id=remote_message['message_id'])

    def get_auth_headers(self):
        config = self.get_static_config()
        return {
            'Authorization': ['Basic ' + base64.b64encode('%s:%s' % (
                config.account_key, config.access_token))],
        }

    @inlineCallbacks
    def update_status(self, **kw):
        '''Publishes a status if it is not a repeat of the previously
        published status.'''
        if self.status_detect.check_status(**kw):
            yield self.publish_status(**kw)


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


class GoConversationTransport(GoConversationTransportBase):
    # Most of this copied wholesale from vumi.transports.httprpc.

    CONFIG_CLASS = VumiBridgeTransportConfig

    redis = None
    web_resource = None

    @inlineCallbacks
    def setup_transport(self):
        self.setup_cacerts()
        config = self.get_static_config()
        self.redis = yield TxRedisManager.from_config(
            config.redis_manager)

        self.web_resource = yield self.start_web_resources([
            (GoConversationResource(self.handle_raw_inbound_message),
             "%s/messages.json" % (config.web_path)),
            (GoConversationResource(self.handle_raw_inbound_event),
             "%s/events.json" % (config.web_path)),
            (GoConversationHealthResource(self), config.health_path),
        ], config.web_port)
        self.status_detect = StatusEdgeDetector()

    @inlineCallbacks
    def teardown_transport(self):
        if self.web_resource is not None:
            yield self.web_resource.loseConnection()
        if self.redis is not None:
            self.redis.close_manager()

    def setup_cacerts(self):
        # TODO: This installs an older CA certificate chain that allows
        #       some weak CA certificates. We should switch to .where() when
        #       Vumi Go's certificate doesn't rely on older intermediate
        #       certificates.
        os.environ["SSL_CERT_FILE"] = certifi.old_where()

    def get_transport_url(self, suffix=''):
        """
        Get the URL for the HTTP resource. Requires the worker to be started.

        This is mostly useful in tests, and probably shouldn't be used
        in non-test code, because the API might live behind a load
        balancer or proxy.
        """
        addr = self.web_resource.getHost()
        return "http://%s:%s/%s/%s" % (
            addr.host, addr.port, self.config["web_path"], suffix.lstrip('/'))

    @inlineCallbacks
    def handle_raw_inbound_event(self, request):
        try:
            data = json.loads(request.content.read())
            msg = TransportEvent(_process_fields=True, **to_kwargs(data))
            yield self.handle_inbound_event(msg)
            request.finish()
            if msg.payload["event_type"] == "ack":
                self.update_status(
                    status='ok', component='sent-by-vumi-go',
                    type='vumi_go_sent', message='Sent by Vumi Go')
            elif msg.payload["event_type"] == "nack":
                self.update_status(
                    status='down', component='sent-by-vumi-go',
                    type='vumi_go_failed', message='Vumi Go failed to send')
            self.update_status(
                status='ok', component='vumi-go-event',
                type='good_request',
                message='Good event received from Vumi Go')
        except Exception as e:
            log.err(e)
            request.setResponseCode(400)
            request.finish()
            self.update_status(
                status='down', component='vumi-go-event',
                type='bad_request', message='Bad event received from Vumi Go')

    @inlineCallbacks
    def handle_raw_inbound_message(self, request):
        try:
            data = json.loads(request.content.read())
            msg = TransportUserMessage(
                _process_fields=True, **to_kwargs(data))
            yield self.handle_inbound_message(msg)
            request.finish()
            self.update_status(
                status='ok', component='received-from-vumi-go',
                type='good_request', message='Good request received')
        except Exception as e:
            log.err(e)
            request.setResponseCode(400)
            request.finish()
            self.update_status(
                status='down', component='received-from-vumi-go',
                type='bad_request', message='Bad request received')
