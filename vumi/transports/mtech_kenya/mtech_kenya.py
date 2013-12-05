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
    OPTIONAL_FIELDS = set(["linkID", "gateway", "message_type"])

    KNOWN_ERROR_RESPONSE_CODES = {
        401: 'Invalid username or password',
        403: 'Invalid mobile number',
    }

    def make_request(self, params):
        config = self.get_static_config()
        url = '%s?%s' % (config.outbound_url, urlencode(params))
        log.msg("Making HTTP request: %s" % (url,))
        return http_request_full(url, '', method='POST')

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
        link_id = message['transport_metadata'].get('linkID')
        if link_id is not None:
            params['linkID'] = link_id
        response = yield self.make_request(params)
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
            request, self.EXPECTED_FIELDS, self.OPTIONAL_FIELDS)
        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            yield self.finish_request(message_id, json.dumps(errors), code=400)
            return
        log.msg(('MTechKenyaTransport sending from %(MSISDN)s to '
                 '%(shortCode)s message "%(MESSAGE)s"') % values)
        transport_metadata = {'transport_message_id': values['messageID']}
        if values.get('linkID') is not None:
            transport_metadata['linkID'] = values['linkID']
        yield self.publish_message(
            message_id=message_id,
            content=values['MESSAGE'],
            to_addr=values['shortCode'],
            from_addr=values['MSISDN'],
            transport_type=self.transport_type,
            transport_metadata=transport_metadata,
        )
        yield self.finish_request(
            message_id, json.dumps({'message_id': message_id}))


class MTechKenyaTransportV2(MTechKenyaTransport):

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    def make_request(self, params):
        log.msg("Making HTTP request: %s" % (repr(params)))
        config = self.get_static_config()
        return http_request_full(
            config.outbound_url, urlencode(params), method='POST',
            headers=self.headers)
