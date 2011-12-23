from twisted.web import http
from twisted.internet.defer import inlineCallbacks
from vumi.application.base import ApplicationWorker
from vumi.message import TransportUserMessage
from vumi.utils import http_request_full
from vumi.errors import VumiError, InvalidMessage
from urllib2 import urlparse
from base64 import b64encode


class HTTPRelayError(VumiError): pass

class HTTPRelay(ApplicationWorker):

    reply_header = 'X-Vumi-HTTPRelay-Reply'

    def validate_config(self):
        self.supported_auth_methods = {
            'basic': self.generate_basic_auth_headers,
        }
        self.url = urlparse.urlparse(self.config['url'])
        self.username = self.url.username
        self.password =  self.url.password
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
            'Authorization': 'Basic %s' % (auth_string,)
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
        if response.code == http.OK:
            header = response.getHeader(self.reply_header) == 'true':
            content = response.delivered_body.strip()
            if header and content:
                self.reply_to(message, content)
