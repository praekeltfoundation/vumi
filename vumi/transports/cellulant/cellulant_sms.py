# -*- test-case-name: vumi.transports.cellulant.tests.test_cellulant_sms -*-

import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks

from vumi.utils import http_request_full
from vumi.errors import ConfigError
from vumi import log
from vumi.config import ConfigDict, ConfigText
from vumi.transports.httprpc import HttpRpcTransport


class CellulantSmsTransportConfig(HttpRpcTransport.CONFIG_CLASS):
    """Cellulant transport config.
    """

    credentials = ConfigDict(
        "A dictionary where the `from_addr` is used for the key lookup and the"
        " returned value should be a dictionary containing the username and"
        " password.", required=True)
    outbound_url = ConfigText(
        "The URL to send outbound messages to.", required=True)
    validation_mode = ConfigText(
        "The mode to operate in. Can be 'strict' or 'permissive'. If 'strict'"
        " then any parameter received that is not listed in EXPECTED_FIELDS"
        " nor in IGNORED_FIELDS will raise an error. If 'permissive' then no"
        " error is raised as long as all the EXPECTED_FIELDS are present.",
        default='strict')


class CellulantSmsTransport(HttpRpcTransport):
    """
    HTTP transport for Cellulant SMS.
    """

    transport_type = 'sms'

    CONFIG_CLASS = CellulantSmsTransportConfig

    EXPECTED_FIELDS = set(["SOURCEADDR", "DESTADDR", "MESSAGE", "ID"])
    IGNORED_FIELDS = set(["channelID", "keyword", "CHANNELID", "serviceID",
                          "SERVICEID", "unsub", "transactionID"])

    STRICT_MODE = 'strict'
    PERMISSIVE_MODE = 'permissive'
    DEFAULT_VALIDATION_MODE = STRICT_MODE
    KNOWN_VALIDATION_MODES = [STRICT_MODE, PERMISSIVE_MODE]
    KNOWN_ERROR_RESPONSE_CODES = {
        'E0': 'Insufficient HTTP Params passed',
        'E1': 'Invalid username or password',
        'E2': 'Credits have expired or run out',
        '1005': 'Suspect source address',
    }

    def validate_config(self):
        config = self.CONFIG_CLASS(self.config)
        self._credentials = config.credentials
        self._validation_mode = config.validation_mode
        if self._validation_mode not in self.KNOWN_VALIDATION_MODES:
            raise ConfigError('Invalid validation mode: %s' % (
                self._validation_mode,))
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

    def get_field_values(self, request):
        values = {}
        errors = {}
        for field in request.args:
            if field not in (self.EXPECTED_FIELDS | self.IGNORED_FIELDS):
                if self._validation_mode == self.STRICT_MODE:
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
