# -*- test-case-name: vumi.transports.mediafonemc.tests.test_mediafonemc -*-

import json
from urllib import urlencode

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.utils import http_request_full
from vumi.transports.httprpc import HttpRpcTransport


class MediafoneTransport(HttpRpcTransport):
    """
    HTTP transport for Mediafone Cameroun.

    :param str web_path:
        The HTTP path to listen on.
    :param int web_port:
        The HTTP port
    :param str transport_name:
        The name this transport instance will use to create its queues
    :param str username:
        Mediafone account username.
    :param str password:
        Mediafone account password.
    :param str outbound_url:
        The URL to send outbound messages to.

    """

    transport_type = 'sms'

    EXPECTED_FIELDS = set(['to', 'from', 'sms'])

    def setup_transport(self):
        self._username = self.config['username']
        self._password = self.config['password']
        self._outbound_url = self.config['outbound_url']
        return super(MediafoneTransport, self).setup_transport()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        params = {
            'username': self._username,
            'password': self._password,
            'phone': message['to_addr'],
            'msg': message['content'],
            }
        log.msg("Sending outbound message: %s" % (message,))
        url = '%s?%s' % (self._outbound_url, urlencode(params))
        log.msg("Making HTTP request: %s" % (url,))
        response = yield http_request_full(url, '', method='GET')
        log.msg("Response: (%s) %r" % (response.code, response.delivered_body))

    def get_field_values(self, request):
        values = {}
        errors = {}
        for field in request.args:
            if field not in self.EXPECTED_FIELDS:
                errors.setdefault('unexpected_parameter', []).append(field)
            else:
                values[field] = str(request.args.get(field)[0])
        for field in self.EXPECTED_FIELDS:
            if field not in values:
                errors.setdefault('missing_parameter', []).append(field)
        return values, errors

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        values, errors = self.get_field_values(request)
        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            yield self.finish_request(message_id, json.dumps(errors), code=400)
            return
        log.msg(('MediafoneTransport sending from %(from)s to %(to)s '
                 'message "%(sms)s"') % values)
        yield self.publish_message(
            message_id=message_id,
            content=values['sms'],
            to_addr=values['to'],
            from_addr=values['from'],
            provider='vumi',
            transport_type=self.transport_type,
        )
        yield self.finish_request(
            message_id, json.dumps({'message_id': message_id}))
