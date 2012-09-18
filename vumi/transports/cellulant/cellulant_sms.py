# -*- test-case-name: vumi.transports.cellulant.tests.test_cellulant_sms -*-

import json
from urllib import urlencode

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.utils import http_request_full
from vumi.errors import ConfigError
from vumi.transports.httprpc import HttpRpcTransport


class CellulantSmsTransport(HttpRpcTransport):
    """
    HTTP transport for Cellulant SMS.

    :param str web_path:
        The HTTP path to listen on.
    :param int web_port:
        The HTTP port
    :param str transport_name:
        The name this transport instance will use to create its queues
    :param str username:
        CellulantSms account username.
    :param str password:
        CellulantSms account password.
    :param str outbound_url:
        The URL to send outbound messages to.

    """

    transport_type = 'sms'

    EXPECTED_FIELDS = set(["SOURCEADDR", "DESTADDR", "MESSAGE", "ID"])
    IGNORED_FIELDS = set(["channelID", "keyword", "CHANNELID", "serviceID",
                          "SERVICEID", "unsub", "transactionID"])

    STRICT_MODE = 'strict'
    PERMISSIVE_MODE = 'permissive'
    DEFAULT_VALIDATION_MODE = STRICT_MODE
    KNOWN_VALIDATION_MODES = [STRICT_MODE, PERMISSIVE_MODE]

    def validate_config(self):
        self._username = self.config['username']
        self._password = self.config['password']
        self._validation_mode = self.config.get('validation_mode',
            self.STRICT_MODE)
        if self._validation_mode not in self.KNOWN_VALIDATION_MODES:
            raise ConfigError('Invalid validation mode: %s' % (
                self._validation_mode,))
        self._outbound_url = self.config['outbound_url']
        return super(CellulantSmsTransport, self).validate_config()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        params = {
            'username': self._username,
            'password': self._password,
            'source': message['from_addr'],
            'destination': message['to_addr'],
            'message': message['content'],
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
