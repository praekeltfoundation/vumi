# -*- test-case-name: vumi.transports.cellulant.tests.test_cellulant_sms -*-

import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks

from vumi.utils import http_request_full
from vumi import log
from vumi.config import ConfigDict, ConfigText
from vumi.transports.httprpc import HttpRpcTransport


class CellulantSmsTransportConfig(HttpRpcTransport.CONFIG_CLASS):
    """Cellulant transport config.
    """

    credentials = ConfigDict(
        "A dictionary where the `from_addr` is used for the key lookup and the"
        " returned value should be a dictionary containing the username and"
        " password.", required=True, static=True)
    outbound_url = ConfigText(
        "The URL to send outbound messages to.", required=True, static=True)


class CellulantSmsTransport(HttpRpcTransport):
    """
    HTTP transport for Cellulant SMS.
    """

    transport_type = 'sms'

    CONFIG_CLASS = CellulantSmsTransportConfig

    EXPECTED_FIELDS = set(["SOURCEADDR", "DESTADDR", "MESSAGE", "ID"])
    IGNORED_FIELDS = set(["channelID", "keyword", "CHANNELID", "serviceID",
                          "SERVICEID", "unsub", "transactionID"])

    KNOWN_ERROR_RESPONSE_CODES = {
        'E0': 'Insufficient HTTP Params passed',
        'E1': 'Invalid username or password',
        'E2': 'Credits have expired or run out',
        '1005': 'Suspect source address',
    }

    def validate_config(self):
        config = self.get_static_config()
        self._credentials = config.credentials
        self._outbound_url = config.outbound_url
        return super(CellulantSmsTransport, self).validate_config()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        creds = self._credentials.get(message['from_addr'], {})
        username = creds.get('username', '')
        password = creds.get('password', '')
        params = {
            'username': username,
            'password': password,
            'source': message['from_addr'],
            'destination': message['to_addr'],
            'message': message['content'],
            }
        log.msg("Sending outbound message: %s" % (message,))
        url = '%s?%s' % (self._outbound_url, urlencode(params))
        log.msg("Making HTTP request: %s" % (url,))
        response = yield http_request_full(url, '', method='GET')
        log.msg("Response: (%s) %r" % (response.code, response.delivered_body))
        content = response.delivered_body.strip()

        # we'll only send 1 message at a time and so the API can only
        # return this on a valid ack
        if content == '1':
            yield self.publish_ack(user_message_id=message['message_id'],
                                sent_message_id=message['message_id'])
        else:
            error = self.KNOWN_ERROR_RESPONSE_CODES.get(content,
                'Unknown response code: %s' % (content,))
            yield self.publish_nack(message['message_id'], error)

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        values, errors = self.get_field_values(request, self.EXPECTED_FIELDS,
                                                self.IGNORED_FIELDS)
        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            yield self.finish_request(message_id, json.dumps(errors), code=400)
            return
        log.msg(('CellulantSmsTransport sending from %(SOURCEADDR)s to '
                 '%(DESTADDR)s message "%(MESSAGE)s"') % values)
        yield self.publish_message(
            message_id=message_id,
            content=values['MESSAGE'],
            to_addr=values['DESTADDR'],
            from_addr=values['SOURCEADDR'],
            provider='vumi',
            transport_type=self.transport_type,
            transport_metadata={'transport_message_id': values['ID']},
        )
        yield self.finish_request(
            message_id, json.dumps({'message_id': message_id}))
