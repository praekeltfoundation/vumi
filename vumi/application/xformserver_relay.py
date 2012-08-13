# -*- test-case-name: vumi.application.tests.test_http_relay -*-
from twisted.python import log
from twisted.web import http
from twisted.internet.defer import inlineCallbacks
from vumi.application.http_relay import HTTPRelayApplication, HTTPRelayError
from vumi.utils import http_request_full
from vumi.errors import VumiError
from urllib2 import urlparse
from base64 import b64encode
import json
import httplib2, urllib


class XformServerRelayApplication(HTTPRelayApplication):

    reply_header = 'X-Vumi-XformServerRelay-Reply'

    def get_session_id(self, message):
        return 1

    def construct_content(self, message):
        form_content = message.get('helper_metadata').get('form_content')
        if form_content:
            return self.construct_new_form(form_content)
        else:
            session_id = self.get_session_id(message)
            content = message.get('content')
            return self.construct_answer(session_id, content)

    def construct_answer(self, session_id, content):
        js = {}
        js['action'] = 'answer'
        js['session-id'] = session_id
        js['answer'] = content
        return json.dumps(js)

    def construct_new_form(self, form_content):
        js = {}
        js['action'] = 'new-form'
        js['form-content'] = form_content
        return json.dumps(js)

    @inlineCallbacks
    def consume_user_message(self, message):
        #url = "http://localhost:4444"
        #http = httplib2.Http()
        #response = http.request(url, "POST", self.construct_content(message))
        #print response
        headers = self.get_auth_headers()
        response = yield http_request_full(self.url.geturl(),
                            self.construct_content(message),
                            headers, self.http_method)
        print response
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

