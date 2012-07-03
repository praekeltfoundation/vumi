# -*- test-case-name: vumi.application.tests.test_rapidsms_relay -*-
from urllib import urlencode
from base64 import b64encode

from twisted.web import http
from twisted.web.resource import Resource

from vumi.application.base import ApplicationWorker
from vumi.utils import http_request_full
from vumi.errors import ConfigError
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

    def render_(self, request):
        log.msg("Send request: %s" % (request,))
        request.setHeader("content-type", "application/json")
        msg = self.application.handle_raw_outbound_message(request)
        return msg.to_json()

    def render_PUT(self, request):
        return self.render_(request, "render_PUT")

    def render_GET(self, request):
        return self.render_(request, "render_GET")

    def render_POST(self, request):
        return self.render_(request, "render_POST")


class RapidSMSRelay(ApplicationWorker):
    """Application that relays messages to RapidSMS.

    RapidSMS relay configuration options:

    :param str rapidsms_url:
        URL of the rapidsms http backend.
    """

    def validate_config(self):
        self.rapidsms_url = self.config['rapidsms_url']
        self.web_path = self.config['web_path']
        self.web_port = int(self.config['web_port'])

        self.supported_auth_methods = {
            'basic': self.generate_basic_auth_headers,
        }
        self.username = self.config.get('username')
        self.password = self.config.get('password')
        self.http_method = self.config.get('http_method', 'POST')
        self.auth_method = self.config.get('auth_method', 'basic')
        if self.auth_method not in self.supported_auth_methods:
            raise ConfigError('HTTP Authentication method %s'
                              ' not supported' % (repr(self.auth_method,)))

    def generate_basic_auth_headers(self, username, password):
        credentials = ':'.join([username, password])
        auth_string = b64encode(credentials.encode('utf-8'))
        return {
            'Authorization': ['Basic %s' % (auth_string,)]
        }

    def get_auth_headers(self):
        if self.username is not None:
            handler = self.supported_auth_methods.get(self.auth_method)
            return handler(self.username, self.password)
        return {}

    def setup_application(self):
        # start receipt web resource
        self.web_resource = yield self.start_web_resources(
            [
                (SendResource(self), self.web_path),
                (HealthResource(self), 'health'),
            ],
            self.web_port)

    def teardown_application(self):
        yield self.web_resource.loseConnection()

    def handle_raw_outbound_message(self, request):
        pass

    def _call_rapidsms(self, message):
        headers = self.get_auth_headers()
        params = {
            'sms': message['content'] or '',
            'sender': message['from_addr'],
        }
        url = "%s?%s" % (self.rapidsms_url, urlencode(params))
        return http_request_full(url, headers=headers)

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
