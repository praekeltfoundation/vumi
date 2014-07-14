# -*- test-case-name: vumi.transports.mediaedgegsm.tests.test_mediaedgegsm -*-

import json
from urllib import urlencode

from twisted.python import log
from twisted.web import http
from twisted.internet.defer import inlineCallbacks

from vumi.transports.httprpc import HttpRpcTransport
from vumi.utils import http_request_full, get_operator_name


class MediaEdgeGSMTransport(HttpRpcTransport):
    """
    HTTP transport for MediaEdgeGSM in Ghana.

    :param str web_path:
        The HTTP path to listen on.
    :param int web_port:
        The HTTP port
    :param str transport_name:
        The name this transport instance will use to create its queues
    :param str username:
        MediaEdgeGSM account username.
    :param str password:
        MediaEdgeGSM account password.
    :param str outbound_url:
        The URL to hit for outbound messages that aren't replies.
    :param str outbound_username:
        The username for outbound non-reply messages.
    :param str outbound_password:
        The username for outbound non-reply messages.
    :param dict operator_mappings:
        A nested dictionary mapping MSISDN prefixes to operator names
    """

    transport_type = 'sms'
    content_type = 'text/plain; charset=utf-8'

    ENCODING = 'utf-8'
    EXPECTED_FIELDS = set(['USN', 'PWD', 'PhoneNumber',
        'ServiceNumber', 'Operator', 'SMSBODY'])

    def setup_transport(self):
        self._username = self.config.get('username')
        self._password = self.config.get('password')
        self._outbound_url = self.config.get('outbound_url')
        self._outbound_url_username = self.config.get('outbound_username', '')
        self._outbound_url_password = self.config.get('outbound_password', '')
        self._operator_mappings = self.config.get('operator_mappings', {})
        return super(MediaEdgeGSMTransport, self).setup_transport()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        if message.payload.get('in_reply_to') and 'content' in message.payload:
            super(MediaEdgeGSMTransport, self).handle_outbound_message(message)
        else:
            msisdn = message['to_addr'].lstrip('+')
            params = {
                "USN": self._outbound_url_username,
                "PWD": self._outbound_url_password,
                "SmsID": message['message_id'],
                "PhoneNumber": msisdn,
                "Operator": get_operator_name(msisdn, self._operator_mappings),
                "SmsBody": message['content'],
            }

            url = '%s?%s' % (self._outbound_url, urlencode(params))
            response = yield http_request_full(url, '', method='GET')
            log.msg("Response: (%s) %r" % (response.code,
                response.delivered_body))
            if response.code == http.OK:
                yield self.publish_ack(user_message_id=message['message_id'],
                    sent_message_id=message['message_id'])
            else:
                yield self.publish_nack(user_message_id=message['message_id'],
                    sent_message_id=message['message_id'],
                    reason='Unexpected response code: %s' % (response.code,))

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        values, errors = self.get_field_values(request, self.EXPECTED_FIELDS)

        if self._username and (values.get('USN') != self._username):
            errors['credentials'] = 'invalid'
        if self._password and (values.get('PWD') != self._password):
            errors['credentials'] = 'invalid'

        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            yield self.finish_request(message_id, json.dumps(errors), code=400)
            return
        log.msg(('MediaEdgeGSMTransport sending from %(PhoneNumber)s to '
                    '%(ServiceNumber)s on %(Operator)s message '
                    '"%(SMSBODY)s"') % values)
        yield self.publish_message(
            message_id=message_id,
            content=values['SMSBODY'],
            to_addr=values['ServiceNumber'],
            from_addr=values['PhoneNumber'],
            provider=values['Operator'],
            transport_type=self.transport_type,
        )
