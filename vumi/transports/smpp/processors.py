from zope.interface import implements

from vumi.transports.smpp.helpers import (IDeliveryReportProcessor,
                                          IDeliverShortMessageProcessor)


class EsmeCallbacksDeliveryReportProcessor(object):
    implements(IDeliveryReportProcessor)

    def __init__(self, protocol):
        self.protocol = protocol

    def on_delivery_report_pdu(self, receipted_message_id, message_state):
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
            receipted_message_id, status)

    def on_delivery_report_content(self, receipted_message_id, message_state):
        return self.protocol.esme_callbacks.delivery_report(
            receipted_message_id, message_state)


class EsmeCallbacksDeliverShortMessageProcessor(object):
    implements(IDeliverShortMessageProcessor)

    def __init__(self, protocol):
        self.protocol = protocol
