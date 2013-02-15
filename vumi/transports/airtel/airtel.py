# -*- test-case-name: vumi.transports.airtel.tests.test_airtel -*-

import json

from twisted.internet.defer import inlineCallbacks
from twisted.web import http

from vumi.transports.httprpc import HttpRpcTransport
from vumi.components import SessionManager
from vumi.message import TransportUserMessage
from vumi import log
from vumi.config import ConfigInt, ConfigText, ConfigBool, ConfigDict


class AirtelUSSDTransportConfig(HttpRpcTransport.CONFIG_CLASS):
    airtel_username = ConfigText('The username for this transport',
        required=True, static=True)
    airtel_password = ConfigText('The password for this transport',
        required=True, static=True)
    airtel_charge = ConfigBool(
        'Whether or not to charge for the responses sent.', required=False,
        default=False, static=True)
    airtel_charge_amount = ConfigInt('How much to charge', default=0,
        required=False, static=True)
    redis_manager = ConfigDict('Parameters to connect to Redis with.',
        default={}, required=False, static=True)
    ussd_session_timeout = ConfigInt('Max length of a USSD session',
        default=60 * 10, required=False, static=True)


class AirtelUSSDTransport(HttpRpcTransport):
    """
    Client implementation for the Comviva Flares HTTP Pull API.
    Based on Flares 1.5.0, document version 1.2.0
    """

    transport_type = 'ussd'
    content_type = 'text/plain; charset=utf-8'
    CONFIG_CLASS = AirtelUSSDTransportConfig
    EXPECTED_CLEANUP_FIELDS = set(['userid', 'password', 'MSISDN',
                                    'clean', 'status'])
    EXPECTED_USSD_FIELDS = set(['userid', 'password', 'MSISDN',
                                    'MSC', 'input'])

    def validate_config(self):
        super(AirtelUSSDTransport, self).validate_config()
        config = self.get_static_config()
        self.r_prefix = "vumi.transports.safaricom:%s" % self.transport_name
        self.redis_config = config.redis_manager
        self.r_session_timeout = config.ussd_session_timeout
        self.airtel_username = config.airtel_username
        self.airtel_password = config.airtel_password
        self.airtel_charge = ('Y' if config.airtel_charge else 'N')
        self.airtel_charge_amount = config.airtel_charge_amount

    @inlineCallbacks
    def setup_transport(self):
        super(AirtelUSSDTransport, self).setup_transport()
        self.session_manager = yield SessionManager.from_redis_config(
            self.redis_config, self.r_prefix, self.r_session_timeout)

    def is_cleanup(self, request):
        return 'clean' in request.args

    def is_authenticated(self, request):
        return (request.args['userid'] == [self.airtel_username] and
                request.args['password'] == [self.airtel_password])

    def handle_raw_inbound_message(self, message_id, request):
        if self.is_cleanup(request):
            return self.handle_cleanup_request(message_id, request)
        return self.handle_ussd_request(message_id, request)

    @inlineCallbacks
    def handle_cleanup_request(self, message_id, request):
        values, errors = self.get_field_values(request,
                                                self.EXPECTED_CLEANUP_FIELDS)
        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            self.finish_request(message_id, json.dumps(errors),
                code=http.BAD_REQUEST)
            return

        if not self.is_authenticated(request):
            log.msg('Invalid authentication credentials: %s:%s' % (
                values['userid'], values['password']))
            self.finish_request(message_id, 'Forbidden', code=http.FORBIDDEN)
            return

        session_id = values['MSISDN']
        session = yield self.session_manager.load_session(session_id)
        if not session:
            log.warning('Received cleanup for unknown session: %s' % (
                            session_id,))
            self.finish_request(message_id, 'Unknown Session', code=http.OK)
            return

        from_addr = values['MSISDN']
        to_addr = session['to_addr']
        session_event = TransportUserMessage.SESSION_CLOSE

        yield self.session_manager.clear_session(session_id)
        yield self.publish_message(
                message_id=message_id,
                content='',
                to_addr=to_addr,
                from_addr=from_addr,
                provider='airtel',
                session_event=session_event,
                transport_type=self.transport_type,
                transport_metadata={
                    'airtel': {
                        'clean': values['clean'],
                        'status': values['status'],
                    },
                })
        self.finish_request(message_id, '', code=http.OK)

    @inlineCallbacks
    def handle_ussd_request(self, message_id, request):
        values, errors = self.get_field_values(request,
                                                self.EXPECTED_USSD_FIELDS)
        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            self.finish_request(message_id, json.dumps(errors),
                code=http.BAD_REQUEST)
            return

        if not self.is_authenticated(request):
            log.msg('Invalid authentication credentials: %s:%s' % (
                values['userid'], values['password']))
            self.finish_request(message_id, 'Forbidden', code=http.FORBIDDEN)
            return

        session_id = values['MSISDN']
        from_addr = values['MSISDN']
        ussd_params = values['input']

        session = yield self.session_manager.load_session(session_id)
        if session:
            to_addr = session['to_addr']
            last_ussd_params = session['last_ussd_params']
            new_params = ussd_params[len(last_ussd_params):]
            if new_params:
                if last_ussd_params:
                    content = new_params[1:].rstrip('#')
                else:
                    content = new_params
            else:
                content = ''

            session['last_ussd_params'] = ussd_params.rstrip('#')
            yield self.session_manager.save_session(session_id, session)
            session_event = TransportUserMessage.SESSION_RESUME
        else:
            to_addr = ussd_params
            yield self.session_manager.create_session(session_id,
                from_addr=from_addr, to_addr=to_addr,
                last_ussd_params=ussd_params.rstrip('#'))
            session_event = TransportUserMessage.SESSION_NEW
            content = ''

        yield self.publish_message(
                message_id=message_id,
                content=content,
                to_addr=to_addr,
                from_addr=from_addr,
                provider='airtel',
                session_event=session_event,
                transport_type=self.transport_type,
                transport_metadata={
                    'airtel': {
                        'MSC': values['MSC'],
                    },
                })

    def handle_outbound_message(self, message):
        if message.payload.get('in_reply_to') and 'content' in message.payload:
            if message['session_event'] == TransportUserMessage.SESSION_CLOSE:
                free_flow = 'FB'
            else:
                free_flow = 'FC'
            self.finish_request(message['in_reply_to'],
                message['content'].encode('utf-8'), code=http.OK, headers={
                'Freeflow': [free_flow],
                'charge': [self.airtel_charge],
                'amount': [self.airtel_charge_amount],
                })
            return self.publish_ack(user_message_id=message['message_id'],
                sent_message_id=message['message_id'])
        else:
            return self.publish_nack(user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                reason='Missing in_reply_to or content')
