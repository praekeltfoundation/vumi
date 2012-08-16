# -*- test-case-name: vumi.transports.mediaedgegsm.tests.test_mediaedgegsm -*-

import json

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.transports.httprpc import HttpRpcTransport


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

    """

    transport_type = 'sms'
    content_type = 'text/plain; charset=utf-8'

    EXPECTED_FIELDS = set(['USN', 'PWD', 'PhoneNumber',
        'ServiceNumber', 'Operator', 'SMSBODY'])

    def setup_transport(self):
        self._username = self.config.get('username')
        self._password = self.config.get('password')
        return super(MediaEdgeGSMTransport, self).setup_transport()

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

        if self._username and (values.get('USN') != self._username):
            errors['credentials'] = 'invalid'
        if self._password and (values.get('PWD') != self._password):
            errors['credentials'] = 'invalid'

        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            yield self.finish_request(message_id, json.dumps(errors), code=400)
            return
        log.msg(('MediaEdgeGSMTransport sending from %(PhoneNumber)s to '
            '%(ServiceNumber)s on %(Operator)s message "%(SMSBODY)s"') % values)
        yield self.publish_message(
            message_id=message_id,
            content=values['SMSBODY'],
            to_addr=values['ServiceNumber'],
            from_addr=values['PhoneNumber'],
            provider=values['Operator'],
            transport_type=self.transport_type,
        )
