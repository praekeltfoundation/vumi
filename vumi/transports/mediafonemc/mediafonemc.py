# -*- test-case-name: vumi.transports.mediafonemc.tests.test_mediafonemc -*-

import json
from urllib import urlencode

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.utils import http_request
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
    :param str to_addr:
        This transport does not receive the `to_addr` as part of the incoming
        message, so it needs to be configured here.
    :param str username:
        Mediafone account username.
    :param str password:
        Mediafone account password.
    :param str outbound_url:
        The URL to send outbound messages to.

    """

    transport_type = 'sms'

    EXPECTED_FIELDS = set(['from', 'sms'])

    def setup_transport(self):
        self._to_addr = self.config['to_addr']
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
        response = yield http_request(url, '', method='GET')
        log.msg("Response: %s" % (response,))

    def get_field_values(self, request, *fields):
        values = {}
        errors = {}
        for field in fields:
            value = request.args.get(field, [None])[0]
            if value is None:
                errors.setdefault('missing_parameter', []).append(field)
            else:
                values[field] = str(value)
        for field in request.args:
            if field not in self.EXPECTED_FIELDS:
                errors.setdefault('unexpected_parameter', []).append(field)
        return values, errors

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        values, errors = self.get_field_values(request, 'from', 'sms')
        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            yield self.finish_request(message_id, json.dumps(errors), code=400)
            return
        values['to_addr'] = self._to_addr
        log.msg(('MediafoneTransport sending from %(from)s to %(to_addr)s '
                 'message "%(sms)s"') % values)
        yield self.publish_message(
            message_id=message_id,
            content=values['sms'],
            to_addr=values['to_addr'],
            from_addr=values['from'],
            provider='vumi',
            transport_type=self.transport_type,
        )
        yield self.finish_request(
            message_id, json.dumps({'message_id': message_id}))
