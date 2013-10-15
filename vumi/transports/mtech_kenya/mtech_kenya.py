# -*- test-case-name: vumi.transports.mtech_kenya.tests.test_mtech_kenya -*-

import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks

from vumi.utils import http_request_full
from vumi import log
from vumi.config import ConfigText
from vumi.transports.httprpc import HttpRpcTransport


class MTechKenyaTransportConfig(HttpRpcTransport.CONFIG_CLASS):
    outbound_url = ConfigText('The URL to send outbound messages to.',
                              required=True, static=True)
    mt_username = ConfigText('The username sent with outbound messages',
                             required=True, static=True)
    mt_password = ConfigText('The password sent with outbound messages',
                             required=True, static=True)


class MTechKenyaTransport(HttpRpcTransport):
    """
    HTTP transport for Cellulant SMS.
    """

    transport_type = 'sms'

    CONFIG_CLASS = MTechKenyaTransportConfig

    EXPECTED_FIELDS = set(["shortCode", "MSISDN", "MESSAGE", "messageID"])
    IGNORED_FIELDS = set()

    KNOWN_ERROR_RESPONSE_CODES = {
        401: 'Invalid username or password',
        403: 'Invalid mobile number',
    }

    def validate_config(self):
        config = self.get_static_config()
        self._credentials = {
            'user': config.mt_username,
            'pass': config.mt_password,
        }
        self._outbound_url = config.outbound_url
        return super(MTechKenyaTransport, self).validate_config()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        config = self.get_static_config()
        params = {
            'user': config.mt_username,
            'pass': config.mt_password,
            'messageID': message['message_id'],
            'shortCode': message['from_addr'],
            'MSISDN': message['to_addr'],
            'MESSAGE': message['content'],
        }
        log.msg("Sending outbound message: %s" % (message,))
        url = '%s?%s' % (self._outbound_url, urlencode(params))
        log.msg("Making HTTP request: %s" % (url,))
        response = yield http_request_full(url, '', method='POST')
        log.msg("Response: (%s) %r" % (response.code, response.delivered_body))
        if response.code == 200:
            yield self.publish_ack(user_message_id=message['message_id'],
                                   sent_message_id=message['message_id'])
        else:
            error = self.KNOWN_ERROR_RESPONSE_CODES.get(
                response.code, 'Unknown response code: %s' % (response.code,))
            yield self.publish_nack(message['message_id'], error)

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        values, errors = self.get_field_values(
            request, self.EXPECTED_FIELDS, self.IGNORED_FIELDS)
        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            yield self.finish_request(message_id, json.dumps(errors), code=400)
            return
        log.msg(('MTechKenyaTransport sending from %(MSISDN)s to '
                 '%(shortCode)s message "%(MESSAGE)s"') % values)
        yield self.publish_message(
            message_id=message_id,
            content=values['MESSAGE'],
            to_addr=values['shortCode'],
            from_addr=values['MSISDN'],
            provider='vumi',
            transport_type=self.transport_type,
            transport_metadata={'transport_message_id': values['messageID']},
        )
        yield self.finish_request(
            message_id, json.dumps({'message_id': message_id}))
