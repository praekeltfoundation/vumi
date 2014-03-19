# -*- test-case-name: vumi.transports.smpp.tests.test_mica -*-

from vumi.config import ConfigInt
from vumi.components.session import SessionManager
from vumi.message import TransportUserMessage
from vumi.transports.smpp.processors import default

from twisted.internet.defer import inlineCallbacks, returnValue


class DeliverShortMessageProcessorConfig(
        default.DeliverShortMessageProcessorConfig):

    max_session_length = ConfigInt(
        'Maximum length a USSD sessions data is to be kept for in seconds.',
        default=60 * 3, static=True)


class DeliverShortMessageProcessor(default.DeliverShortMessageProcessor):
    """
    Note: this is the expected PDU

    {'body': {'mandatory_parameters': {'data_coding': 0,
                                       'dest_addr_npi': 'unknown',
                                       'dest_addr_ton': 'unknown',
                                       'destination_addr': '',
                                       'esm_class': 0,
                                       'priority_flag': 0,
                                       'protocol_id': 0,
                                       'registered_delivery': 0,
                                       'replace_if_present_flag': 0,
                                       'schedule_delivery_time': '',
                                       'service_type': 'USSD',
                                       'short_message': '*372#',
                                       'sm_default_msg_id': 0,
                                       'sm_length': 5,
                                       'source_addr': 'XXXXXXXXX',
                                       'source_addr_npi': 'unknown',
                                       'source_addr_ton': 'unknown',
                                       'validity_period': ''},
              'optional_parameters': [{'length': 2,
                                       'tag': 'user_message_reference',
                                       'value': 12853},
                                      {'length': 1,
                                       'tag': 'ussd_service_op',
                                       'value': '01'}]},
     'header': {'command_id': 'deliver_sm',
                'command_length': 65,
                'command_status': 'ESME_ROK',
                'sequence_number': 1913}}
    """

    CONFIG_CLASS = DeliverShortMessageProcessorConfig

    def __init__(self, transport, config):
        super(DeliverShortMessageProcessor, self).__init__(transport, config)
        self.transport = transport
        self.redis = transport.redis
        self.config = self.CONFIG_CLASS(config, static=True)
        self.session_manager = SessionManager(
            self.redis, max_session_length=self.config.max_session_length)

    @inlineCallbacks
    def handle_deliver_sm_ussd(self, pdu, pdu_params, pdu_opts):
        service_op = pdu_opts['ussd_service_op']
        session_identifier = pdu_opts['user_message_reference']

        session_event = 'close'
        if service_op == '01':
            # PSSR request. Let's assume it means a new session.
            session_event = 'new'
            ussd_code = pdu_params['short_message']
            content = None

            yield self.session_manager.create_session(
                session_identifier, ussd_code=ussd_code)

        elif service_op == '17':
            # PSSR response. This means session end.
            session_event = 'close'

            session = yield self.session_manager.load_session(
                session_identifier)
            ussd_code = session['ussd_code']
            content = None

            yield self.session_manager.clear_session(session_identifier)

        else:
            session_event = 'continue'

            session = yield self.session_manager.load_session(
                session_identifier)
            ussd_code = session['ussd_code']
            content = pdu_params['short_message']

        # This is stashed on the message and available when replying
        # with a `submit_sm`
        session_info = {
            'session_identifier': session_identifier,
        }

        decoded_msg = self.decode_message(content,
                                          pdu_params['data_coding'])

        result = yield self.handle_short_message_content(
            source_addr=pdu_params['source_addr'],
            destination_addr=ussd_code,
            short_message=decoded_msg,
            message_type='ussd',
            session_event=session_event,
            session_info=session_info)
        returnValue(result)


class SubmitShortMessageProcessor(default.SubmitShortMessageProcessor):

    def handle_outbound_message(self, message, protocol):
        to_addr = message['to_addr']
        from_addr = message['from_addr']
        text = message['content']

        session_event = message['session_event']
        transport_type = message['transport_type']
        optional_parameters = {}

        if transport_type == 'ussd':
            continue_session = (
                session_event != TransportUserMessage.SESSION_CLOSE)
            session_info = message['transport_metadata'].get(
                'session_info', {})
            optional_parameters.update({
                'ussd_service_op': ('02' if continue_session else '17'),
                'user_message_reference': session_info.get(
                    'session_identifier', '')
            })

        if self.config.send_long_messages:
            return protocol.submit_sm_long(
                to_addr.encode('ascii'),
                long_message=text.encode(self.config.submit_sm_encoding),
                data_coding=self.config.submit_sm_data_coding,
                source_addr=from_addr.encode('ascii'),
                optional_parameters=optional_parameters,
            )

        elif self.config.send_multipart_sar:
            return protocol.submit_csm_sar(
                to_addr.encode('ascii'),
                short_message=text.encode(self.config.submit_sm_encoding),
                data_coding=self.config.submit_sm_data_coding,
                source_addr=from_addr.encode('ascii'),
                optional_parameters=optional_parameters,
            )

        elif self.config.send_multipart_udh:
            return protocol.submit_csm_udh(
                to_addr.encode('ascii'),
                short_message=text.encode(self.config.submit_sm_encoding),
                data_coding=self.config.submit_sm_data_coding,
                source_addr=from_addr.encode('ascii'),
                optional_parameters=optional_parameters,
            )

        return protocol.submit_sm(
            to_addr.encode('ascii'),
            short_message=text.encode(self.config.submit_sm_encoding),
            data_coding=self.config.submit_sm_data_coding,
            source_addr=from_addr.encode('ascii'),
            optional_parameters=optional_parameters,
        )
