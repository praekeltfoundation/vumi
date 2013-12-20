from zope.interface import implements

from vumi.transports.smpp.helpers import (IDeliveryReportProcessor,
                                          IDeliverShortMessageProcessor)
from vumi.transports.smpp.utils import unpacked_pdu_opts


class EsmeCallbacksDeliveryReportProcessor(object):
    implements(IDeliveryReportProcessor)

    def __init__(self, protocol):
        self.protocol = protocol
        self.config = self.protocol.config

    def inspect_delivery_report_pdu(self, pdu):
        pdu_opts = unpacked_pdu_opts(pdu)

        # This might be a delivery receipt with PDU parameters. If we get a
        # delivery receipt without these parameters we'll try a regex match
        # later once we've decoded the message properly.
        receipted_message_id = pdu_opts.get('receipted_message_id', None)
        message_state = pdu_opts.get('message_state', None)
        if receipted_message_id is not None and message_state is not None:
            return {
                'receipted_message_id': receipted_message_id,
                'message_state': message_state,
            }

    def handle_delivery_report_pdu(self, receipted_message_id, message_state):
        status = {
            1: 'ENROUTE',
            2: 'DELIVERED',
            3: 'EXPIRED',
            4: 'DELETED',
            5: 'UNDELIVERABLE',
            6: 'ACCEPTED',
            7: 'UNKNOWN',
            8: 'REJECTED',
        }.get(message_state, 'UNKNOWN')
        return self.protocol.esme_callbacks.delivery_report(
            message_id=receipted_message_id, message_state=status)

    def inspect_delivery_report_content(self, content):
        delivery_report = self.config.delivery_report_regex.search(
            content or '')

        if delivery_report:
            # We have a delivery report.
            fields = delivery_report.groupdict()
            return {
                'receipted_message_id': fields['id'],
                'message_state': fields['stat'],
            }

    def handle_delivery_report_content(self, receipted_message_id,
                                       message_state):
        return self.protocol.esme_callbacks.delivery_report(
            message_id=receipted_message_id, message_state=message_state)


class EsmeCallbacksDeliverShortMessageProcessor(object):
    implements(IDeliverShortMessageProcessor)

    def __init__(self, protocol):
        self.protocol = protocol
