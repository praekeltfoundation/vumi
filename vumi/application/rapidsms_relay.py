# -*- test-case-name: vumi.application.tests.test_rapidsms_relay -*-
import json
from base64 import b64encode

from zope.interface import implements
from twisted.internet.defer import (
    inlineCallbacks, returnValue, DeferredList)
from twisted.web import http
from twisted.web.resource import Resource, IResource
from twisted.web.server import NOT_DONE_YET
from twisted.cred import portal, checkers, credentials, error
from twisted.web.guard import HTTPAuthSessionWrapper, BasicCredentialFactory

from vumi.application.base import ApplicationWorker
from vumi.persist.txredis_manager import TxRedisManager
from vumi.config import (
    ConfigUrl, ConfigText, ConfigInt, ConfigDict, ConfigBool, ConfigContext,
    ConfigList)
from vumi.message import to_json, TransportUserMessage
from vumi.utils import http_request_full
from vumi.errors import ConfigError, InvalidEndpoint
from vumi import log


class HealthResource(Resource):
    isLeaf = True

    def render_GET(self, request):
        request.setResponseCode(http.OK)
        request.do_not_log = True
        return 'OK'


class SendResource(Resource):
    isLeaf = True

    def __init__(self, application):
        self.application = application
        Resource.__init__(self)

    def finish_request(self, request, msgs):
        request.setResponseCode(http.OK)
        request.write(to_json([msg.payload for msg in msgs]))
        request.finish()

    def fail_request(self, request, f):
        if f.check(BadRequestError):
            code = http.BAD_REQUEST
        else:
            code = http.INTERNAL_SERVER_ERROR
        log.err(f)
        request.setResponseCode(code)
        request.write(f.getErrorMessage())
        request.finish()

    def render_(self, request):
        log.msg("Send request: %s" % (request,))
        request.setHeader("content-type", "application/json; charset=utf-8")
        d = self.application.handle_raw_outbound_message(request)
        d.addCallback(lambda msgs: self.finish_request(request, msgs))
        d.addErrback(lambda f: self.fail_request(request, f))
        return NOT_DONE_YET

    def render_PUT(self, request):
        return self.render_(request)

    def render_GET(self, request):
        return self.render_(request)

    def render_POST(self, request):
        return self.render_(request)


class BadRequestError(Exception):
    """Raised when an invalid request was received from RapidSMS."""


class RapidSMSRelayRealm(object):
    implements(portal.IRealm)

    def __init__(self, resource):
        self.resource = resource

    def requestAvatar(self, user, mind, *interfaces):
        if IResource in interfaces:
            return (IResource, self.resource, lambda: None)
        raise NotImplementedError()


class RapidSMSRelayAccessChecker(object):
    implements(checkers.ICredentialsChecker)
    credentialInterfaces = (credentials.IUsernamePassword,
                            credentials.IAnonymous)

    def __init__(self, get_avatar_id):
        self._get_avatar_id = get_avatar_id

    def requestAvatarId(self, credentials):
        return self._get_avatar_id(credentials)


class RapidSMSRelayConfig(ApplicationWorker.CONFIG_CLASS):
    """RapidSMS relay configuration."""

    web_path = ConfigText(
        "Path to listen for outbound messages from RapidSMS on.",
        static=True)
    web_port = ConfigInt(
        "Port to listen for outbound messages from RapidSMS on.",
        static=True)
    redis_manager = ConfigDict(
        "Redis manager configuration (only required if"
        " `allow_replies` is true)",
        default={}, static=True)
    allow_replies = ConfigBool(
        "Whether to support replies via the `in_reply_to` argument"
        " from RapidSMS.", default=True, static=True)

    vumi_username = ConfigText(
        "Username required when calling `web_path` (default: no"
        " authentication)",
        default=None)
    vumi_password = ConfigText(
        "Password required when calling `web_path`", default=None)
    vumi_auth_method = ConfigText(
        "Authentication method required when calling `web_path`."
        "The 'basic' method is currently the only available method",
        default='basic')
    vumi_reply_timeout = ConfigInt(
        "Number of seconds to keep original messages in redis so that"
        " replies may be sent via `in_reply_to`.", default=10 * 60)
    allowed_endpoints = ConfigList(
        'List of allowed endpoints to send from.',
        required=True, default=("default",))

    rapidsms_url = ConfigUrl("URL of the rapidsms http backend.")
    rapidsms_username = ConfigText(
        "Username to use for the `rapidsms_url` (default: no authentication)",
        default=None)
    rapidsms_password = ConfigText(
        "Password to use for the `rapidsms_url`", default=None)
    rapidsms_auth_method = ConfigText(
        "Authentication method to use with `rapidsms_url`."
        " The 'basic' method is currently the only available method.",
        default='basic')
    rapidsms_http_method = ConfigText(
        "HTTP request method to use for the `rapidsms_url`",
        default='POST')


class RapidSMSRelay(ApplicationWorker):
    """Application that relays messages to RapidSMS."""

    CONFIG_CLASS = RapidSMSRelayConfig

    ALLOWED_ENDPOINTS = None

    def validate_config(self):
        self.supported_auth_methods = {
            'basic': self.generate_basic_auth_headers,
        }

    def generate_basic_auth_headers(self, username, password):
        credentials = ':'.join([username, password])
        auth_string = b64encode(credentials.encode('utf-8'))
        return {
            'Authorization': ['Basic %s' % (auth_string,)]
        }

    def get_auth_headers(self, config):
        auth_method, username, password = (config.rapidsms_auth_method,
                                           config.rapidsms_username,
                                           config.rapidsms_password)
        if auth_method not in self.supported_auth_methods:
            raise ConfigError('HTTP Authentication method %s'
                              ' not supported' % (repr(auth_method,)))
        if username is not None:
            handler = self.supported_auth_methods.get(auth_method)
            return handler(username, password)
        return {}

    def get_protected_resource(self, resource):
        checker = RapidSMSRelayAccessChecker(self.get_avatar_id)
        realm = RapidSMSRelayRealm(resource)
        p = portal.Portal(realm, [checker])
        factory = BasicCredentialFactory("RapidSMS Relay")
        protected_resource = HTTPAuthSessionWrapper(p, [factory])
        return protected_resource

    @inlineCallbacks
    def get_avatar_id(self, creds):
        # The ConfigContext(username=...) passed into .get_config() is to
        # allow sub-classes to change how config.vumi_username and
        # config.vumi_password are looked up by overriding .get_config().
        if credentials.IAnonymous.providedBy(creds):
            config = yield self.get_config(None, ConfigContext(username=None))
            # allow anonymous authentication if no username is configured
            if config.vumi_username is None:
                returnValue(None)
        elif credentials.IUsernamePassword.providedBy(creds):
            username, password = creds.username, creds.password
            config = yield self.get_config(None,
                                           ConfigContext(username=username))
            if (username == config.vumi_username and
                    password == config.vumi_password):
                returnValue(username)
        raise error.UnauthorizedLogin()

    @inlineCallbacks
    def setup_application(self):
        config = self.get_static_config()
        self.redis = None
        if config.allow_replies:
            self.redis = yield TxRedisManager.from_config(config.redis_manager)
        send_resource = self.get_protected_resource(SendResource(self))
        self.web_resource = yield self.start_web_resources(
            [
                (send_resource, config.web_path),
                (HealthResource(), 'health'),
            ],
            config.web_port)

    @inlineCallbacks
    def teardown_application(self):
        yield self.web_resource.loseConnection()
        if self.redis is not None:
            yield self.redis.close_manager()

    def _msg_key(self, message_id):
        return ":".join(["messages", message_id])

    def _load_message(self, message_id):
        d = self.redis.get(self._msg_key(message_id))
        d.addCallback(lambda r: r and TransportUserMessage.from_json(r))
        return d

    def _store_message(self, message, timeout):
        msg_key = self._msg_key(message['message_id'])
        d = self.redis.set(msg_key, message.to_json())
        d.addCallback(lambda r: self.redis.expire(msg_key, timeout))
        return d

    @inlineCallbacks
    def _handle_reply_to(self, config, content, to_addrs, in_reply_to):
        if not config.allow_replies:
            raise BadRequestError("Support for `in_reply_to` not configured.")
        orig_msg = yield self._load_message(in_reply_to)
        if not orig_msg:
            raise BadRequestError("Original message %r not found." %
                                  (in_reply_to,))
        if to_addrs:
            if len(to_addrs) > 1 or to_addrs[0] != orig_msg['from_addr']:
                raise BadRequestError(
                    "Supplied `to_addrs` don't match `from_addr` of original"
                    " message %r" % (in_reply_to,))
        reply = yield self.reply_to(orig_msg, content)
        returnValue([reply])

    def send_rapidsms_nonreply(self, to_addr, content, config, endpoint):
        """Call .send_to() for a message from RapidSMS that is not a reply.

        This is for overriding by sub-classes that need to add additional
        message options.
        """
        return self.send_to(to_addr, content, endpoint=endpoint)

    def _handle_send_to(self, config, content, to_addrs, endpoint):
        sends = []
        try:
            self.check_endpoint(config.allowed_endpoints, endpoint)
            for to_addr in to_addrs:
                sends.append(self.send_rapidsms_nonreply(
                    to_addr, content, config, endpoint))
        except InvalidEndpoint, e:
            raise BadRequestError(e)
        d = DeferredList(sends, consumeErrors=True)
        d.addCallback(lambda msgs: [msg[1] for msg in msgs if msg[0]])
        return d

    @inlineCallbacks
    def handle_raw_outbound_message(self, request):
        config = yield self.get_config(
            None, ConfigContext(username=request.getUser()))
        data = json.loads(request.content.read())
        content = data['content']
        to_addrs = data['to_addr']
        if not isinstance(to_addrs, list):
            raise BadRequestError(
                "Supplied `to_addr` (%r) was not a list." % (to_addrs,))
        in_reply_to = data.get('in_reply_to')
        endpoint = data.get('endpoint')
        if in_reply_to is not None:
            msgs = yield self._handle_reply_to(config, content, to_addrs,
                                               in_reply_to)
        else:
            msgs = yield self._handle_send_to(config, content, to_addrs,
                                              endpoint)
        returnValue(msgs)

    @inlineCallbacks
    def _call_rapidsms(self, message):
        config = yield self.get_config(message)
        http_method = config.rapidsms_http_method.encode("utf-8")
        headers = self.get_auth_headers(config)
        yield self._store_message(message, config.vumi_reply_timeout)
        response = http_request_full(config.rapidsms_url.geturl(),
                                     message.to_json(),
                                     headers, http_method)
        response.addCallback(lambda response: log.info(response.code))
        response.addErrback(lambda failure: log.err(failure))
        yield response

    def consume_user_message(self, message):
        return self._call_rapidsms(message)

    def close_session(self, message):
        return self._call_rapidsms(message)

    def consume_ack(self, event):
        log.info("Acknowledgement received for message %r"
                 % (event['user_message_id']))

    def consume_delivery_report(self, event):
        log.info("Delivery report received for message %r, status %r"
                 % (event['user_message_id'], event['delivery_status']))
