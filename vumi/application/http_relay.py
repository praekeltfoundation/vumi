# -*- test-case-name: vumi.application.tests.test_http_relay -*-
from twisted.python import log
from twisted.web import http
from twisted.internet.defer import inlineCallbacks
from vumi.application.base import ApplicationWorker
from vumi.utils import http_request_full
from vumi.errors import VumiError
from urllib2 import urlparse
from base64 import b64encode


class HTTPRelayError(VumiError):
    pass


class HTTPRelayApplication(ApplicationWorker):

    reply_header = 'X-Vumi-HTTPRelay-Reply'

    def validate_config(self):
        self.supported_auth_methods = {
            'basic': self.generate_basic_auth_headers,
        }
        self.url = urlparse.urlparse(self.config['url'])
        self.username = self.config.get('username', '')
        self.password = self.config.get('password', '')
        self.http_method = self.config.get('http_method', 'POST')
        self.auth_method = self.config.get('auth_method', 'basic')
        if self.auth_method not in self.supported_auth_methods:
            raise HTTPRelayError(
                    'HTTP Authentication method %s not supported' % (
                    repr(self.auth_method,)))

    def generate_basic_auth_headers(self, username, password):
        credentials = ':'.join([username, password])
        auth_string = b64encode(credentials.encode('utf-8'))
        return {
            'Authorization': ['Basic %s' % (auth_string,)]
        }

    @inlineCallbacks
    def consume_user_message(self, message):
        if self.username:
            handler = self.supported_auth_methods.get(self.auth_method)
            headers = handler(self.username, self.password)
        else:
            headers = {}

        response = yield http_request_full(self.url.geturl(),
                            message.to_json(), headers, self.http_method)
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
