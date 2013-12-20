from zope.interface import implements

from vumi.transports.smpp.helpers import (IDeliveryReportProcessor,
                                          IDeliverShortMessageProcessor)
from vumi.transports.smpp.utils import unpacked_pdu_opts
from vumi.config import Config, ConfigDict, ConfigRegex


class DeliveryReportProcessorConfig(Config):

    DELIVERY_REPORT_REGEX = (
        'id:(?P<id>\S{,65})'
        ' +sub:(?P<sub>...)'
        ' +dlvrd:(?P<dlvrd>...)'
        ' +submit date:(?P<submit_date>\d*)'
        ' +done date:(?P<done_date>\d*)'
        ' +stat:(?P<stat>[A-Z]{7})'
        ' +err:(?P<err>...)'
        ' +[Tt]ext:(?P<text>.{,20})'
        '.*'
    )

    DELIVERY_REPORT_STATUS_MAPPING = {
        # Output values should map to themselves:
        'delivered': 'delivered',
        'failed': 'failed',
        'pending': 'pending',
        # SMPP `message_state` values:
        'ENROUTE': 'pending',
        'DELIVERED': 'delivered',
        'EXPIRED': 'failed',
        'DELETED': 'failed',
        'UNDELIVERABLE': 'failed',
        'ACCEPTED': 'delivered',
        'UNKNOWN': 'pending',
        'REJECTED': 'failed',
        # From the most common regex-extracted format:
        'DELIVRD': 'delivered',
        'REJECTD': 'failed',
        # Currently we will accept this for Yo! TODO: investigate
        '0': 'delivered',
    }

    delivery_report_regex = ConfigRegex(
        'What regex to use for matching delivery reports',
        default=DELIVERY_REPORT_REGEX, static=True)
    delivery_report_status_mapping = ConfigDict(
        "Mapping from delivery report message state to "
        "(`delivered`, `failed`, `pending`)",
        static=True, default=DELIVERY_REPORT_STATUS_MAPPING)


class EsmeCallbacksDeliveryReportProcessor(object):
    implements(IDeliveryReportProcessor)
    CONFIG_CLASS = DeliveryReportProcessorConfig

    def __init__(self, protocol, config):
        self.protocol = protocol
        self._static_config = self.CONFIG_CLASS(config, static=True)

    def get_static_config(self):
        return self._static_config

    def inspect_delivery_report_pdu(self, pdu):
        pdu_opts = unpacked_pdu_opts(pdu)

        # This might be a delivery receipt with PDU parameters. If we get a
        # delivery receipt without these parameters we'll try a regex match
        # later once we've decoded the message properly.
        receipted_message_id = pdu_opts.get('receipted_message_id', None)
        message_state = pdu_opts.get('message_state', None)
        if receipted_message_id is not None and message_state is not None:
            return (receipted_message_id, message_state)

    def handle_delivery_report_pdu(self, pdu_data):
        receipted_message_id, message_state = pdu_data
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
            message_id=receipted_message_id,
            delivery_status=self.delivery_status(status))

    def inspect_delivery_report_content(self, content):
        config = self.get_static_config()
        delivery_report = config.delivery_report_regex.search(
            content or '')

        if delivery_report:
            # We have a delivery report.
            fields = delivery_report.groupdict()
            return (fields['id'], fields['stat'])

    def handle_delivery_report_content(self, pdu_data):
        receipted_message_id, message_state = pdu_data
        return self.protocol.esme_callbacks.delivery_report(
            message_id=receipted_message_id,
            delivery_status=self.delivery_status(message_state))

    def delivery_status(self, state):
        config = self.get_static_config()
        return config.delivery_report_status_mapping.get(state, 'pending')


class EsmeCallbacksDeliverShortMessageProcessor(object):
    implements(IDeliverShortMessageProcessor)

    def __init__(self, protocol, config):
        self.protocol = protocol
        self.config = config
