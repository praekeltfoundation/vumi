# -*- test-case-name: vumi.application.tests.test_http_relay -*-
from urllib2 import urlparse
from base64 import b64encode

from twisted.python import log
from twisted.web import http
from twisted.internet.defer import inlineCallbacks

from vumi.application.base import ApplicationWorker
from vumi.utils import http_request_full
from vumi.errors import VumiError


class HTTPRelayError(VumiError):
    pass


class HTTPRelayApplication(ApplicationWorker):
    """Application that relays messages over HTTP.

    HTTP relay configuration options:

    :param str url:
        URL to send incoming messages to.
    :param str event_url:
        URL to send events related to outbound messages to (default: `url`).
    :param str web_path:
        Path to listen for outbound messages on (default: None). If omitted
        no outbound message server is started.
    :param str web_port:
        Port to listen for outbound messages on (default: None).
    :param str web_ssl_key:
        Path to the file containing the SSL key for the web server
        (default: don't use SSL).
    :param str web_ssl_cert:
        Path to the file containing the SSL certificate for the web
        server (default: don't use SSL).
    :param str username:
        Username to use when calling the `url` (default: no authentication).
    :param str password:
        Password to use when calling `url` (default: none).
    :param str auth_method:
        Authentication method to use with `url` (default: 'basic').
        The 'basic' method is currently the only available method.
    :param str http_method:
        HTTP request method to when calling the `url` (default: POST)
    """

    reply_header = 'X-Vumi-HTTPRelay-Reply'

    def validate_config(self):
        self.supported_auth_methods = {
            'basic': self.generate_basic_auth_headers,
        }
        self.url = urlparse.urlparse(self.config['url'])
        self.event_url = urlparse.urlparse(self.config.get('event_url',
                                           self.url.geturl()))
        self.web_path = self.config.get('web_path')
        self.web_port = int(self.config.get('web_port') or 0)
        self.web_ssl_key = self.config.get('web_ssl_key')
        self.web_ssl_cert = self.config.get('web_ssl_cert')
        self.username = self.config.get('username', '')
        self.password = self.config.get('password', '')
        self.http_method = self.config.get('http_method', 'POST')
        self.auth_method = self.config.get('auth_method', 'basic')
        if self.auth_method not in self.supported_auth_methods:
            raise HTTPRelayError('HTTP Authentication method'
                                 ' %r not supported' % (self.auth_method,))

    @inlineCallbacks
    def setup_application(self):
        if self.web_path is not None:
            # start receipt web resource
            self.web_resource = yield self.start_web_resources(
                [
                    (SendResource(self), self.web_path),
                    (HealthResource(), 'health'),
                ],
                self.web_port,
                ssl_key=self.web_ssl_key, ssl_cert=self.web_ssl_cert)
        else:
            self.web_resource = None

    @inlineCallbacks
    def teardown_application(self):
        if self.web_resource is not None:
            yield self.web_resource.loseConnection()

    def generate_basic_auth_headers(self, username, password):
        credentials = ':'.join([username, password])
        auth_string = b64encode(credentials.encode('utf-8'))
        return {
            'Authorization': ['Basic %s' % (auth_string,)]
        }

    def get_auth_headers(self):
        if self.username:
            handler = self.supported_auth_methods.get(self.auth_method)
            return handler(self.username, self.password)
        return {}

    def handle_raw_outbound_message(self, request):
        # TODO: use 'in_reply_to': to look up original message
        to_addr = request.args['to_addr'][0].decode('utf-8')
        content = request.args['content'][0].decode('utf-8')
        return self.send_to(to_addr, content)

    @inlineCallbacks
    def consume_user_message(self, message):
        headers = self.get_auth_headers()
        response = yield http_request_full(self.url.geturl(),
                                           message.to_json(), headers,
                                           self.http_method)
        headers = response.headers
        if response.code == http.OK:
            if headers.hasHeader(self.reply_header):
                raw_headers = headers.getRawHeaders(self.reply_header)
                content = response.delivered_body.strip()
                if (raw_headers[0].lower() == 'true') and content:
                    self.reply_to(message, content)
        else:
            log.err('%s responded with %s' % (self.url.geturl(),
                                              response.code))

    @inlineCallbacks
    def relay_event(self, event):
        headers = self.get_auth_headers()
        yield http_request_full(self.event_url.geturl(),
                                event.to_json(), headers, self.http_method)

    @inlineCallbacks
    def consume_ack(self, event):
        yield self.relay_event(event)

    @inlineCallbacks
    def consume_delivery_report(self, event):
        yield self.relay_event(event)
