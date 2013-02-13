# -*- test-case-name: vumi.application.tests.test_http_relay -*-

from base64 import b64encode

from twisted.python import log
from twisted.web import http
from twisted.internet.defer import inlineCallbacks

from vumi.application.base import ApplicationWorker
from vumi.utils import http_request_full
from vumi.errors import VumiError
from vumi.config import ConfigText, ConfigUrl


class HTTPRelayError(VumiError):
    pass


class HTTPRelayConfig(ApplicationWorker.CONFIG_CLASS):

    # TODO: Make these less static?
    url = ConfigUrl(
        "URL to submit incoming message to.", required=True, static=True)
    event_url = ConfigUrl(
        "URL to submit incoming events to. (Defaults to the same as 'url').",
        static=True)
    http_method = ConfigText(
        "HTTP method for submitting messages.", default='POST', static=True)
    auth_method = ConfigText(
        "HTTP authentication method.", default='basic', static=True)

    username = ConfigText("Username for HTTP authentication.", default='')
    password = ConfigText("Password for HTTP authentication.", default='')


class HTTPRelayApplication(ApplicationWorker):
    CONFIG_CLASS = HTTPRelayConfig

    reply_header = 'X-Vumi-HTTPRelay-Reply'

    def validate_config(self):
        self.supported_auth_methods = {
            'basic': self.generate_basic_auth_headers,
        }
        # XXX: Is this the best way to do this?
        if 'event_url' not in self.config:
            self.config['event_url'] = self.config['url']
        config = self.get_static_config()
        if config.auth_method not in self.supported_auth_methods:
            raise HTTPRelayError(
                    'HTTP Authentication method %s not supported' % (
                    repr(config.auth_method,)))

    def generate_basic_auth_headers(self, username, password):
        credentials = ':'.join([username, password])
        auth_string = b64encode(credentials.encode('utf-8'))
        return {
            'Authorization': ['Basic %s' % (auth_string,)]
        }

    def get_auth_headers(self, config):
        if config.username:
            handler = self.supported_auth_methods.get(config.auth_method)
            return handler(config.username, config.password)
        return {}

    @inlineCallbacks
    def consume_user_message(self, message):
        config = yield self.get_config(message)
        headers = self.get_auth_headers(config)
        response = yield http_request_full(config.url.geturl(),
                            message.to_json(), headers, config.http_method)
        headers = response.headers
        if response.code == http.OK:
            if headers.hasHeader(self.reply_header):
                raw_headers = headers.getRawHeaders(self.reply_header)
                content = response.delivered_body.strip()
                if (raw_headers[0].lower() == 'true') and content:
                    self.reply_to(message, content)
        else:
            log.err('%s responded with %s' % (config.url.geturl(),
                                                response.code))

    @inlineCallbacks
    def relay_event(self, event):
        config = yield self.get_config(event)
        headers = self.get_auth_headers(config)
        yield http_request_full(config.event_url.geturl(),
            event.to_json(), headers, config.http_method)

    @inlineCallbacks
    def consume_ack(self, event):
        yield self.relay_event(event)

    @inlineCallbacks
    def consume_delivery_report(self, event):
        yield self.relay_event(event)
