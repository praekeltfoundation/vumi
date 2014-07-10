# -*- test-case-name: vumi.transports.cellulant.tests.test_cellulant -*-

from twisted.internet.defer import inlineCallbacks

from vumi.components.session import SessionManager
from vumi.errors import VumiError
from vumi.transports.httprpc import HttpRpcTransport
from vumi.message import TransportUserMessage
from vumi import log


class CellulantError(VumiError):
    """Used to log errors specific to the Cellulant transport."""


def pack_ussd_message(message):
    next_level = 1  # Ignoring the menu levels
    content = message['content']
    value_of_selection = 'null'
    service_id = 'null'
    if message['session_event'] == TransportUserMessage.SESSION_CLOSE:
        status = 'end'
    else:
        status = 'null'
    extra = 'null'

    return "%s|%s|%s|%s|%s|%s" % (
                next_level,
                content,
                value_of_selection,
                service_id,
                status,
                extra)


class CellulantTransport(HttpRpcTransport):
    """Cellulant USSD (via HTTP) transport."""

    ENCODING = 'utf-8'
    EVENT_MAP = {
        'BEG': TransportUserMessage.SESSION_NEW,
        'ABO': TransportUserMessage.SESSION_CLOSE,
    }

    def validate_config(self):
        super(CellulantTransport, self).validate_config()
        self.transport_type = self.config.get('transport_type', 'ussd')

    @inlineCallbacks
    def setup_transport(self):
        super(CellulantTransport, self).setup_transport()
        r_config = self.config.get('redis_manager', {})
        r_prefix = "vumi.transports.cellulant:%s" % self.transport_name
        session_timeout = int(self.config.get("ussd_session_timeout", 600))
        self.session_manager = yield SessionManager.from_redis_config(
            r_config, r_prefix, session_timeout)

    @inlineCallbacks
    def teardown_transport(self):
        yield self.session_manager.stop()
        yield super(CellulantTransport, self).teardown_transport()

    def set_ussd_for_msisdn_session(self, msisdn, session, ussd):
        return self.session_manager.create_session(
            "%s:%s" % (msisdn, session), ussd=ussd)

    def get_ussd_for_msisdn_session(self, msisdn, session):
        d = self.session_manager.load_session("%s:%s" % (msisdn, session))
        return d.addCallback(lambda s: s.get('ussd', None))

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        op_code = request.args.get('opCode')[0]
        to_addr = None
        if op_code == "BEG":
            to_addr = request.args.get('INPUT')[0]
            content = None
            yield self.set_ussd_for_msisdn_session(
                request.args.get('MSISDN')[0],
                request.args.get('sessionID')[0],
                to_addr)
        else:
            to_addr = yield self.get_ussd_for_msisdn_session(
                request.args.get('MSISDN')[0],
                request.args.get('sessionID')[0])
            content = request.args.get('INPUT')[0]

        if ((request.args.get('ABORT')[0] not in ('0', 'null'))
                or (op_code == 'ABO')):
            # respond to phones aborting a session
            self.finish_request(message_id, '')
            event = TransportUserMessage.SESSION_CLOSE
        else:
            event = self.EVENT_MAP.get(op_code,
                TransportUserMessage.SESSION_RESUME)

        if to_addr is None:
            # we can't continue so finish request and log error
            self.finish_request(message_id, '')
            log.error(CellulantError(
                "Failed redis USSD to_addr lookup for %s" % request.args))
        else:
            transport_metadata = {
                'session_id': request.args.get('sessionID')[0],
            }
            self.publish_message(
                message_id=message_id,
                content=content,
                to_addr=to_addr,
                from_addr=request.args.get('MSISDN')[0],
                session_event=event,
                transport_name=self.transport_name,
                transport_type=self.transport_type,
                transport_metadata=transport_metadata,
            )

    def handle_outbound_message(self, message):
        missing_fields = self.ensure_message_values(message,
                                ['in_reply_to', 'content'])
        if missing_fields:
            return self.reject_message(message, missing_fields)

        self.finish_request(message['in_reply_to'],
                            pack_ussd_message(message).encode(self.ENCODING))
        return self.publish_ack(user_message_id=message['message_id'],
            sent_message_id=message['message_id'])
