from uuid import uuid4

from twisted.internet.defer import succeed

from vumi import log
from vumi.config import ConfigDict, ConfigText
from vumi.message import TransportUserMessage
from vumi.transports.smpp.processors import (
    DeliveryReportProcessor, DeliverShortMessageProcessor,
    SubmitShortMessageProcessor, SubmitShortMessageProcessorConfig)
from vumi.transports.smpp.smpp_utils import unpacked_pdu_opts
from vumi.utils import get_operator_number


class EsmeCallbacksDeliveryReportProcessor(DeliveryReportProcessor):

    def __init__(self, transport, config):
        self.esme_callbacks = transport.esme_callbacks
        self.config = self.CONFIG_CLASS(config, static=True)

    def handle_delivery_report_pdu(self, pdu):
        """
        Check if this might be a delivery receipt with PDU parameters.

        There's a chance we'll get a delivery receipt without these
        parameters, if that happens we'll try a regex match in
        ``inspect_delivery_report_content`` once the message
        has (potentially) been reassembled and decoded.
        """
        pdu_opts = unpacked_pdu_opts(pdu)
        receipted_message_id = pdu_opts.get('receipted_message_id', None)
        message_state = pdu_opts.get('message_state', None)
        if receipted_message_id is None or message_state is None:
            return succeed(False)

        status = self.status_map.get(message_state, 'UNKNOWN')

        d = self.esme_callbacks.delivery_report(
            message_id=receipted_message_id,
            delivery_status=status)
        d.addCallback(lambda _: True)
        return d

    def handle_delivery_report_content(self, content):
        delivery_report = self.config.delivery_report_regex.search(
            content or '')

        if not delivery_report:
            return succeed(False)

        # We have a delivery report.
        fields = delivery_report.groupdict()
        receipted_message_id = fields['id']
        message_state = fields['stat']
        d = self.esme_callbacks.delivery_report(
            message_id=receipted_message_id,
            delivery_status=self.delivery_status(message_state))
        d.addCallback(lambda _: True)
        return d


class EsmeCallbacksDeliverShortMessageProcessor(DeliverShortMessageProcessor):

    def __init__(self, transport, config):
        self.redis = transport.redis
        self.esme_callbacks = transport.esme_callbacks
        self.config = self.CONFIG_CLASS(config, static=True)

    def handle_short_message_content(self, source_addr, destination_addr,
                                     short_message, **kw):
        return self.esme_callbacks.deliver_sm(
            source_addr=source_addr, destination_addr=destination_addr,
            short_message=short_message, message_id=uuid4().hex,
            **kw)


class EsmeCallbacksSubmitShortMessageProcessorConfig(
        SubmitShortMessageProcessorConfig):

    COUNTRY_CODE = ConfigText(
        "Used to translate a leading zero in a destination MSISDN into a "
        "country code. Default ''", default="", static=True)
    OPERATOR_PREFIX = ConfigDict(
        "Nested dictionary of prefix to network name mappings. Default {} "
        "(set network to 'UNKNOWN'). E.g. { '27': { '27761': 'NETWORK1' }} ",
        default={}, static=True)
    OPERATOR_NUMBER = ConfigDict(
        "Dictionary of source MSISDN to use for each network listed in "
        "OPERATOR_PREFIX. If a network is not listed, the source MSISDN "
        "specified by the message sender is used. Default {} (always used the "
        "from address specified by the message sender). "
        "E.g. { 'NETWORK1': '27761234567'}", default={}, static=True)


class EsmeCallbacksSubmitShortMessageProcessor(SubmitShortMessageProcessor):

    CONFIG_CLASS = EsmeCallbacksSubmitShortMessageProcessorConfig

    def handle_outbound_message(self, message, esme_client):
        log.debug("Sending SMPP message: %s" % (message))
        # first do a lookup in our YAML to see if we've got a source_addr
        # defined for the given MT number, if not, trust the from_addr
        # in the message
        to_addr = message['to_addr']
        from_addr = message['from_addr']
        text = message['content']
        continue_session = (
            message['session_event'] != TransportUserMessage.SESSION_CLOSE)
        route = get_operator_number(to_addr, self.config.COUNTRY_CODE,
                                    self.config.OPERATOR_PREFIX,
                                    self.config.OPERATOR_NUMBER)
        source_addr = route or from_addr
        session_info = message['transport_metadata'].get('session_info')
        return esme_client.submit_sm(
            # these end up in the PDU
            short_message=text.encode(self.config.submit_sm_encoding),
            data_coding=self.config.submit_sm_data_coding,
            destination_addr=to_addr.encode('ascii'),
            source_addr=source_addr.encode('ascii'),
            session_info=session_info.encode('ascii')
                if session_info is not None else None,
            # these don't end up in the PDU
            message_type=message['transport_type'],
            continue_session=continue_session,
        )
