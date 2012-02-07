# -*- test-case-name: vumi.transports.cellulant.tests.test_cellulant -*-

import redis

from vumi.transports.httprpc import HttpRpcTransport
from vumi.message import TransportUserMessage


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

    EVENT_MAP = {
        'BEG': TransportUserMessage.SESSION_NEW,
        'ABO': TransportUserMessage.SESSION_CLOSE,
    }

    def validate_config(self):
        super(CellulantTransport, self).validate_config()
        self.transport_type = self.config.get('transport_type', 'ussd')
        self.transport_name = self.config.get('transport_type',
                'cellulant_ussd')

    def setup_transport(self):
        super(CellulantTransport, self).setup_transport()
        self.r_prefix = "vumi.transports.cellulant:%s" % self.transport_name
        self.r_db_index = int(self.config.get("redis_db_index", 13))
        self.r_session_timeout = int(self.config.get("ussd_session_timeout",
                                                                        600))
        self.connect_to_redis()

    def connect_to_redis(self):
        self.r_server = redis.Redis('localhost', db=self.r_db_index)

    def set_ussd_for_msisdn_session(self, msisdn, session, ussd):
        key = "%s:%s:%s" % (self.r_prefix, msisdn, session)
        self.r_server.set(key, ussd)
        self.r_server.expire(key, self.r_session_timeout)

    def get_ussd_for_msisdn_session(self, msisdn, session):
        key = "%s:%s:%s" % (self.r_prefix, msisdn, session)
        return self.r_server.get(key)

    def handle_raw_inbound_message(self, message_id, request):
        op_code = request.args.get('opCode')[0]
        to_addr = None
        if op_code == "BEG":
            to_addr = request.args.get('INPUT')[0]
            self.set_ussd_for_msisdn_session(
                    request.args.get('MSISDN')[0],
                    request.args.get('sessionID')[0],
                    to_addr,
                    )
        else:
            to_addr = self.get_ussd_for_msisdn_session(
                    request.args.get('MSISDN')[0],
                    request.args.get('sessionID')[0],
                    )
        if ((request.args.get('ABORT')[0] not in ('0', 'null'))
            or (op_code == 'ABO')):
            # respond to phones aborting a session
            self.finish_request(message_id, '')
            event = TransportUserMessage.SESSION_CLOSE
        else:
            event = self.EVENT_MAP.get(op_code,
                TransportUserMessage.SESSION_RESUME)

        transport_metadata = {
            'session_id': request.args.get('sessionID')[0],
        }
        self.publish_message(
            message_id=message_id,
            content=request.args.get('INPUT')[0],
            to_addr=to_addr,
            from_addr=request.args.get('MSISDN')[0],
            session_event=event,
            transport_name=self.transport_name,
            transport_type=self.transport_type,
            transport_metadata=transport_metadata,
        )

    def handle_outbound_message(self, message):
        if message.payload.get('in_reply_to') and 'content' in message.payload:
            self.finish_request(message['in_reply_to'],
                                pack_ussd_message(message).encode('utf-8'))
