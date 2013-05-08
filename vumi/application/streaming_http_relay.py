# -*- test-case-name: vumi.application.tests.test_streaming_http_relay -*-

import json
import copy
import random
from functools import partial
from collections import defaultdict

from zope.interface import implements
from twisted.cred import portal, checkers, credentials, error
from twisted.web import resource, http, util
from twisted.web.server import NOT_DONE_YET
from twisted.web.guard import HTTPAuthSessionWrapper
from twisted.web.guard import BasicCredentialFactory
from twisted.internet.error import ConnectionDone
from twisted.internet.defer import (
    Deferred, inlineCallbacks, returnValue, maybeDeferred)

from vumi import log
from vumi.utils import http_request_full, to_kwargs
from vumi.config import (
    ConfigUrl, ConfigText, ConfigInt, ConfigDict, ConfigList, ConfigBool)
from vumi.transports.httprpc import httprpc
from vumi.application.base import ApplicationWorker
from vumi.persist.txredis_manager import TxRedisManager
from vumi.message import TransportUserMessage, TransportEvent


class StreamingHTTPRelayConfig(ApplicationWorker.CONFIG_CLASS):
    """Streaming HTTP message relay.
    """
    # Static setup.
    web_path = ConfigText(
        "URL path to listen on for outbound messages.", static=True,
        required=True)
    web_port = ConfigInt(
        "TCP port to listen on for outbound messages.", static=True,
        required=True)
    health_path = ConfigText(
        "URL path to listen on for health checks.", static=True,
        default='/health/')
    redis_manager = ConfigDict(
        "Parameters to connect to Redis with.", static=True, default={})
    reply_requires_inbound = ConfigBool(
        "If True, reply messages require stored inbound messages to be found.",
        static=True, default=False)

    # For non-streaming mode.
    push_message_url = ConfigUrl("Optional URL to POST incoming message to.")
    push_event_url = ConfigUrl("Optional URL to POST incoming events to.")

    # Dynamic configuration.
    api_username = ConfigText("Username for API calls.", default='http_api')
    api_auth_tokens = ConfigList("List of auth tokens for API calls.")
    api_msg_options = ConfigDict(
        "(deprecated) Message options for outbound messages.")


class StreamingHTTPRelayWorker(ApplicationWorker):
    CONFIG_CLASS = StreamingHTTPRelayConfig
    ALLOWED_ENDPOINTS = frozenset(['default'])

    reply_header = 'X-Vumi-HTTPRelay-Reply'

    @inlineCallbacks
    def setup_redis(self):
        self.redis = yield TxRedisManager.from_config(
            self.get_static_config().redis_manager)

    @inlineCallbacks
    def setup_application(self):
        config = self.get_static_config()
        self.web_path = config.web_path
        self.web_port = config.web_port
        self.health_path = config.health_path

        # Set these to empty dictionaries because we're not interested
        # in using any of the helper functions at this point.
        self._event_handlers = {}
        self._session_handlers = {}
        yield self.setup_redis()

        self.client_manager = StreamingClientManager(
            self.redis.sub_manager('http_api:message_cache'))

        self.webserver = self.start_web_resources([
            (StreamingResource(self), self.web_path),
            (httprpc.HttpRpcHealthResource(self), self.health_path),
        ], self.web_port)

    def stream(self, stream_class, api_username, message):
        # Publish the message by manually specifying the routing key
        rk = stream_class.routing_key % {
            'transport_name': self.transport_name,
            'api_username': api_username,
        }
        return self.client_manager.publish(rk, message)

    def register_client(self, key, message_class, callback):
        self.client_manager.start(key, message_class, callback)
        return self.client_manager.flush_backlog(key, message_class, callback)

    def unregister_client(self, api_username, callback):
        self.client_manager.stop(api_username, callback)

    @inlineCallbacks
    def consume_user_message(self, message):
        config = yield self.get_config(message)
        push_message_url = config.push_message_url

        if push_message_url:
            resp = yield self.push(push_message_url.geturl(), message)
            if resp.code != http.OK:
                log.warning('Got unexpected response code %s from %s' % (
                    resp.code, push_message_url))
        else:
            yield self.stream(MessageStream, config.api_username, message)

    @inlineCallbacks
    def consume_unknown_event(self, event):
        """
        FIXME:  We're forced to do too much hoopla when trying to link events
                back to the original message.
        """
        config = yield self.get_config(event)
        push_event_url = config.push_event_url

        if push_event_url:
            resp = yield self.push(push_event_url.geturl(), event)
            if resp.code != http.OK:
                log.warning('Got unexpected response code %s from %s' % (
                    resp.code, push_event_url))
        else:
            yield self.stream(EventStream, config.api_username, event)

    def push(self, url, vumi_message):
        data = vumi_message.to_json().encode('utf-8')
        return http_request_full(url.encode('utf-8'), data=data, headers={
            'Content-Type': 'application/json; charset=utf-8',
        })

    def get_health_response(self):
        return str(sum([len(callbacks) for callbacks in
                    self.client_manager.clients.values()]))

    @inlineCallbacks
    def teardown_application(self):
        yield super(StreamingHTTPRelayWorker, self).teardown_application()
        yield self.webserver.loseConnection()

    def get_api_user_config(self, api_user):
        # This should be overridden in subclasses that support multiple api
        # users.
        return self.get_config(None)

    def get_inbound_message(self, message_id):
        # TODO: Support inbound message stuff.
        return None


class StreamingClientManager(object):

    MAX_BACKLOG_SIZE = 100
    CLIENT_PREFIX = 'clients'

    def __init__(self, redis):
        self.redis = redis
        self.clients = defaultdict(list)

    def client_key(self, *args):
        return u':'.join([self.CLIENT_PREFIX] + map(unicode, args))

    def backlog_key(self, key):
        return self.client_key('backlog', key)

    @inlineCallbacks
    def flush_backlog(self, key, message_class, callback):
        backlog_key = self.backlog_key(key)
        while True:
            obj = yield self.redis.rpop(backlog_key)
            if obj is None:
                break
            yield maybeDeferred(callback, message_class.from_json(obj))

    def start(self, key, message_class, callback):
        self.clients[key].append(callback)

    def stop(self, key, callback):
        self.clients[key].remove(callback)

    def publish(self, key, msg):
        callbacks = self.clients[key]
        if callbacks:
            callback = random.choice(callbacks)
            return maybeDeferred(callback, msg)
        else:
            return self.queue_in_backlog(key, msg)

    @inlineCallbacks
    def queue_in_backlog(self, key, msg):
        backlog_key = self.backlog_key(key)
        yield self.redis.lpush(backlog_key, msg.to_json())
        yield self.redis.ltrim(backlog_key, 0, self.MAX_BACKLOG_SIZE - 1)


class StreamResource(resource.Resource):

    message_class = None
    proxy_buffering = False

    def __init__(self, worker, api_username):
        resource.Resource.__init__(self)
        self.worker = worker
        self.api_username = api_username
        self.user_apis = {}

        self.stream_ready = Deferred()
        self.stream_ready.addCallback(self.setup_stream)
        self._callback = None
        self._rk = self.routing_key % {
            'transport_name': self.worker.transport_name,
            'api_username': self.api_username,
        }

    def render_GET(self, request):
        # Turn off proxy buffering, nginx will otherwise buffer our streaming
        # output which makes clients sad.
        # See #proxy_buffering at
        # http://nginx.org/en/docs/http/ngx_http_proxy_module.html
        request.responseHeaders.addRawHeader('X-Accel-Buffering',
            'yes' if self.proxy_buffering else 'no')
        # Twisted's Agent has trouble closing a connection when the server has
        # sent the HTTP headers but not the body, but sometimes we need to
        # close a connection when only the headers have been received.
        # Sending an empty string as a workaround gets the body consumer
        # stuff started anyway and then we have the ability to close the
        # connection.
        request.write('')
        done = request.notifyFinish()
        done.addBoth(self.teardown_stream)
        self._callback = partial(self.publish, request)
        self.stream_ready.callback(request)
        return NOT_DONE_YET

    def setup_stream(self, request):
        return self.worker.register_client(self._rk, self.message_class,
            self._callback)

    def teardown_stream(self, err):
        if not (err is None or err.trap(ConnectionDone)):
            log.error(err)
        return self.worker.unregister_client(self._rk, self._callback)

    def publish(self, request, message):
        line = u'%s\n' % (message.to_json(),)
        request.write(line.encode('utf-8'))


class EventStream(StreamResource):

    message_class = TransportEvent
    routing_key = '%(transport_name)s.stream.event.%(api_username)s'


class MessageStream(StreamResource):

    message_class = TransportUserMessage
    routing_key = '%(transport_name)s.stream.message.%(api_username)s'

    def render_PUT(self, request):
        d = Deferred()
        d.addCallback(self.handle_PUT)
        d.callback(request)
        return NOT_DONE_YET

    def get_msg_options(self, payload, white_list=[]):
        raw_payload = copy.deepcopy(payload.copy())
        msg_options = dict((key, value)
                           for key, value in raw_payload.items()
                           if key in white_list)
        return msg_options

    @inlineCallbacks
    def handle_PUT(self, request):
        try:
            payload = json.loads(request.content.read())
        except ValueError:
            request.setResponseCode(http.BAD_REQUEST, 'Invalid Message')
            request.finish()
            return

        config = yield self.worker.get_api_user_config(request.getUser())
        in_reply_to = payload.get('in_reply_to')
        if in_reply_to:
            yield self.handle_PUT_reply(config, request, payload, in_reply_to)
        else:
            yield self.handle_PUT_send_to(config, request, payload)

    @inlineCallbacks
    def handle_PUT_reply(self, config, request, payload, in_reply_to):
        msg_options = self.get_msg_options(payload, ['content', 'to_addr'])
        if config.api_msg_options:
            msg_options.update(config.api_msg_options)
        content = msg_options.pop('content')

        # Pull these out of msg_options, but keep them in case we need to build
        # a fake message.
        to_addr = msg_options.pop('to_addr', None),
        from_addr = msg_options.pop('from_addr', None),
        transport_name = msg_options.pop('transport_type', None),
        transport_type = msg_options.pop('transport_name', None),

        inbound_msg = yield self.worker.get_inbound_message(in_reply_to)
        if inbound_msg is None:
            # No inbound message, so build a fake one and just trust whatever
            # we get from the client.

            if config.reply_requires_inbound:
                # We aren't allowed to invent a fake message, so die.
                request.setResponseCode(http.BAD_REQUEST)
                request.write('Invalid in_reply_to value')
                request.finish()
                return

            if ('to_addr' is None) or ('transport_metadata' not in payload):
                request.setResponseCode(http.BAD_REQUEST, 'Invalid Message')
                request.finish()
                return

            # Create a fake inbound message to reply to.
            inbound_msg = TransportUserMessage(
                message_id=in_reply_to, content=None,
                from_addr=to_addr, to_addr=from_addr,
                transport_name=transport_type, transport_type=transport_name,
                transport_metadata=payload['transport_metadata'])

        msg = yield self.worker.reply_to(
            inbound_msg, content, endpoint='default', **to_kwargs(msg_options))

        request.setResponseCode(http.OK)
        request.write(msg.to_json())
        request.finish()

    @inlineCallbacks
    def handle_PUT_send_to(self, config, request, payload):
        config = yield self.worker.get_api_user_config(request.getUser())

        msg_options = self.get_msg_options(payload, ['content', 'to_addr'])
        if config.api_msg_options:
            msg_options.update(config.api_msg_options)
        to_addr = msg_options.pop('to_addr')
        content = msg_options.pop('content')

        msg = yield self.worker.send_to(
            to_addr, content, endpoint='default', **msg_options)

        request.setResponseCode(http.OK)
        request.write(msg.to_json())
        request.finish()


class APIUserResource(resource.Resource):

    CONCURRENCY_LIMIT = 10

    def __init__(self, worker, api_username):
        resource.Resource.__init__(self)
        self.worker = worker
        self.redis = worker.redis
        self.api_username = api_username

    def key(self, *args):
        return ':'.join(['concurrency'] + map(unicode, args))

    @inlineCallbacks
    def is_allowed(self, user_id):
        count = int((yield self.redis.get(self.key(user_id))) or 0)
        returnValue(count < self.CONCURRENCY_LIMIT)

    def track_request(self, user_id):
        return self.redis.incr(self.key(user_id))

    def release_request(self, err, user_id):
        return self.redis.decr(self.key(user_id))

    def render(self, request):
        return resource.NoResource().render(request)

    def getChild(self, path, request):
        return util.DeferredResource(self.getDeferredChild(path, request))

    @inlineCallbacks
    def getDeferredChild(self, path, request):
        class_map = {
            'events.json': EventStream,
            'messages.json': MessageStream,
        }
        stream_class = class_map.get(path)

        if stream_class is None:
            returnValue(resource.NoResource())

        user_id = request.getUser()
        if (yield self.is_allowed(user_id)):

            # remove track when request is closed
            finished = request.notifyFinish()
            finished.addBoth(self.release_request, user_id)

            yield self.track_request(user_id)
            returnValue(stream_class(self.worker, self.api_username))
        returnValue(resource.ErrorPage(http.FORBIDDEN, 'Forbidden',
                                        'Too many concurrent connections'))


class StreamingResource(resource.Resource):

    def __init__(self, worker):
        resource.Resource.__init__(self)
        self.worker = worker

    def render(self, request):
        return resource.NoResource().render(request)

    def getChild(self, api_username, request):
        if not api_username:
            return resource.NoResource()

        res = APIUserResource(self.worker, api_username)
        checker = APIUserAccessChecker(self.worker, api_username)
        realm = APIUserRealm(res)
        p = portal.Portal(realm, [checker])

        factory = BasicCredentialFactory("Vumi Message Stream")
        protected_resource = HTTPAuthSessionWrapper(p, [factory])

        return protected_resource


class APIUserRealm(object):
    implements(portal.IRealm)

    def __init__(self, resource):
        self.resource = resource

    def requestAvatar(self, user, mind, *interfaces):
        if resource.IResource in interfaces:
            return (resource.IResource, self.resource, lambda: None)
        raise NotImplementedError()


class APIUserAccessChecker(object):
    implements(checkers.ICredentialsChecker)
    credentialInterfaces = (credentials.IUsernamePassword,)

    def __init__(self, worker, api_username):
        self.worker = worker
        self.api_username = api_username

    @inlineCallbacks
    def requestAvatarId(self, credentials):
        config = yield self.worker.get_api_user_config(self.api_username)
        username = credentials.username
        token = credentials.password
        if username == self.api_username and token in config.api_auth_tokens:
            returnValue(username)
        raise error.UnauthorizedLogin()
