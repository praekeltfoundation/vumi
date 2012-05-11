# -*- test-case-name: vumi.transports.safaricom.tests.test_safaricom -*-

import json
import redis

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.transports.httprpc import HttpRpcTransport
from vumi.message import TransportUserMessage


class SafaricomTransport(HttpRpcTransport):
    """
    HTTP transport for Mediafone Cameroun.

    :param str web_path:
        The HTTP path to listen on.
    :param int web_port:
        The HTTP port
    :param str transport_name:
        The name this transport instance will use to create its queues
    :param dict redis:
        The configuration parameters for connecting to Redis.
    :param int ussd_session_timeout:
        The number of seconds after which a timeout is forced on a transport
        level.
    """

    transport_type = 'ussd'

    EXPECTED_FIELDS = set(['ORIG', 'DEST', 'SESSION_ID', 'USSD_PARAMS'])

    def validate_config(self):
        super(SafaricomTransport, self).validate_config()
        self.transport_type = self.config.get('transport_type', 'ussd')

    def setup_transport(self):
        super(SafaricomTransport, self).setup_transport()
        self.redis_config = self.config.get('redis', {})
        self.r_prefix = "vumi.transports.safaricom:%s" % self.transport_name
        self.r_session_timeout = int(self.config.get("ussd_session_timeout",
                                                                        600))
        self.connect_to_redis()

    # the connection to redis is a seperate method to allow overriding in tests
    def connect_to_redis(self):
        self.r_server = redis.Redis(**self.redis_config)

    def r_key(self, msisdn, session):
        return "%s:%s:%s" % (self.r_prefix, msisdn, session)

    def set_ussd_for_msisdn_session(self, msisdn, session, ussd):
        self.r_server.set(self.r_key(msisdn, session), ussd)
        self.r_server.expire(self.r_key(msisdn, session),
                self.r_session_timeout)

    def get_ussd_for_msisdn_session(self, msisdn, session):
        return self.r_server.get(self.r_key(msisdn, session))

    def session_exists(self, msisdn, session):
        return self.r_server.exists(self.r_key(msisdn, session))

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
        log.msg(('SafaricomTransport sending from %(ORIG)s to %(DEST)s '
                 'for %(SESSION_ID)s message "%(USSD_PARAMS)s"') % values)

        from_addr = values['ORIG']
        to_addr = values['DEST']
        session_id = values['SESSION_ID']
        content = values['USSD_PARAMS']

        if self.session_exists(from_addr, session_id):
            session_event = TransportUserMessage.SESSION_RESUME
        else:
            session_event = TransportUserMessage.SESSION_NEW

        yield self.publish_message(
            message_id=message_id,
            content=content,
            to_addr=to_addr,
            from_addr=from_addr,
            provider='safaricom',
            session_event=session_event,
            transport_type=self.transport_type,
            transport_metadata={
                'safaricom': {
                    'session_id': session_id,
                }
            }
        )

    def handle_outbound_message(self, message):
        if message.payload.get('in_reply_to') and 'content' in message.payload:
            if message['session_event'] == TransportUserMessage.SESSION_CLOSE:
                command = 'END'
            else:
                command = 'CON'
            self.finish_request(message['in_reply_to'],
                ('%s %s' % (command, message['content'])).encode('utf-8'))
