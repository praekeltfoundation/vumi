# -*- test-case-name: vumi.transports.safaricom.tests.test_safaricom -*-

import json

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.transports.httprpc import HttpRpcTransport
from vumi.message import TransportUserMessage
from vumi.components import SessionManager


class SafaricomTransport(HttpRpcTransport):
    """
    HTTP transport for USSD with Safaricom in Kenya.

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
        self.redis_config = self.config.get('redis_manager', {})
        self.r_prefix = "vumi.transports.safaricom:%s" % self.transport_name
        self.r_session_timeout = int(self.config.get("ussd_session_timeout",
                                                                        600))

    @inlineCallbacks
    def setup_transport(self):
        super(SafaricomTransport, self).setup_transport()
        self.session_manager = yield SessionManager.from_redis_config(
            self.redis_config, self.r_prefix, self.r_session_timeout)

    @inlineCallbacks
    def teardown_transport(self):
        yield self.session_manager.stop()
        yield super(SafaricomTransport, self).teardown_transport()

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
        session_id = values['SESSION_ID']
        from_addr = values['ORIG']
        dest = values['DEST']
        ussd_params = values['USSD_PARAMS']

        session = yield self.session_manager.load_session(session_id)
        if session:
            to_addr = session['to_addr']
            last_ussd_params = session['last_ussd_params']
            new_params = ussd_params.replace(last_ussd_params, '')
            if new_params:
                if last_ussd_params:
                    content = new_params[1:]
                else:
                    content = new_params
            else:
                content = ''

            session['last_ussd_params'] = ussd_params
            yield self.session_manager.save_session(session_id, session)
            session_event = TransportUserMessage.SESSION_RESUME
        else:
            if ussd_params:
                to_addr = '*%s*%s#' % (dest, ussd_params)
            else:
                to_addr = '*%s#' % (dest,)
            yield self.session_manager.create_session(session_id,
                from_addr=from_addr, to_addr=to_addr,
                last_ussd_params=ussd_params)
            session_event = TransportUserMessage.SESSION_NEW
            content = ''

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
