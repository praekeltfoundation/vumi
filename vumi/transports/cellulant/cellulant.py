# -*- test-case-name: vumi.transports.cellulant.tests.test_cellulant -*-

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
        self.to_addr = self.config['ussd_code']
        self.transport_type = self.config.get('transport_type', 'ussd')

    def handle_raw_inbound_message(self, message_id, request):
        op_code = request.args.get('opCode')[0]
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
            to_addr=self.to_addr,
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
