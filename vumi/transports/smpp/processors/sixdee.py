# -*- test-case-name: vumi.transports.smpp.tests.test_sixdee -*-

from vumi.config import ConfigInt
from vumi.components.session import SessionManager
from vumi.message import TransportUserMessage
from vumi.transports.smpp.processors import default
from vumi import log

from twisted.internet.defer import inlineCallbacks, returnValue


def make_vumi_session_identifier(msisdn, sixdee_session_identifier):
    return '%s+%s' % (msisdn, sixdee_session_identifier)


class DeliverShortMessageProcessorConfig(
        default.DeliverShortMessageProcessorConfig):

    max_session_length = ConfigInt(
        'Maximum length a USSD sessions data is to be kept for in seconds.',
        default=60 * 3, static=True)


class DeliverShortMessageProcessor(default.DeliverShortMessageProcessor):

    CONFIG_CLASS = DeliverShortMessageProcessorConfig

    # NOTE: these keys are hexidecimal because of python-smpp encoding
    #       quirkiness
    ussd_service_op_map = {
        '01': 'new',
        '12': 'continue',
        '81': 'close',  # user abort
    }

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

        # 6D uses its_session_info as follows:
        #
        # * First 15 bit: dialog id (i.e. session id)
        # * Last bit: end session (1 to end, 0 to continue)

        its_session_number = int(pdu_opts['its_session_info'], 16)
        end_session = bool(its_session_number % 2)
        sixdee_session_identifier = "%04x" % (its_session_number & 0xfffe)
        vumi_session_identifier = make_vumi_session_identifier(
            pdu_params['source_addr'], sixdee_session_identifier)

        if end_session:
            session_event = 'close'
        else:
            session_event = self.ussd_service_op_map.get(service_op)

        if session_event == 'new':
            # PSSR request. Let's assume it means a new session.
            ussd_code = pdu_params['short_message']
            content = None

            yield self.session_manager.create_session(
                vumi_session_identifier, ussd_code=ussd_code)

        elif session_event == 'close':
            session = yield self.session_manager.load_session(
                vumi_session_identifier)
            ussd_code = session['ussd_code']
            content = None

            yield self.session_manager.clear_session(vumi_session_identifier)

        else:
            if session_event != 'continue':
                log.warning(('Received unknown %r ussd_service_op, '
                             'assuming continue.') % (service_op,))
                session_event = 'continue'

            session = yield self.session_manager.load_session(
                vumi_session_identifier)
            ussd_code = session['ussd_code']
            content = pdu_params['short_message']

        # This is stashed on the message and available when replying
        # with a `submit_sm`
        session_info = {
            'ussd_service_op': service_op,
            'session_identifier': sixdee_session_identifier,
        }

        decoded_msg = self.dcs_decode(content,
                                      pdu_params['data_coding'])

        result = yield self.handle_short_message_content(
            source_addr=pdu_params['source_addr'],
            destination_addr=ussd_code,
            short_message=decoded_msg,
            message_type='ussd',
            session_event=session_event,
            session_info=session_info)
        returnValue(result)


class SubmitShortMessageProcessorConfig(
        default.SubmitShortMessageProcessorConfig):

    max_session_length = ConfigInt(
        'Maximum length a USSD sessions data is to be kept for in seconds.',
        default=60 * 3, static=True)


class SubmitShortMessageProcessor(default.SubmitShortMessageProcessor):

    CONFIG_CLASS = SubmitShortMessageProcessorConfig

    # NOTE: these values are hexidecimal because of python-smpp encoding
    #       quirkiness
    ussd_service_op_map = {
        'continue': '02',
        'close': '17',  # end
    }

    def __init__(self, transport, config):
        super(SubmitShortMessageProcessor, self).__init__(transport, config)
        self.transport = transport
        self.redis = transport.redis
        self.config = self.CONFIG_CLASS(config, static=True)
        self.session_manager = SessionManager(
            self.redis, max_session_length=self.config.max_session_length)

    @inlineCallbacks
    def handle_outbound_message(self, message, protocol):
        to_addr = message['to_addr']
        from_addr = message['from_addr']
        text = message['content']
        if text is None:
            text = u""
        vumi_message_id = message['message_id']

        session_event = message['session_event']
        transport_type = message['transport_type']
        optional_parameters = {}

        if transport_type == 'ussd':
            continue_session = (
                session_event != TransportUserMessage.SESSION_CLOSE)
            session_info = message['transport_metadata'].get(
                'session_info', {})
            sixdee_session_identifier = session_info.get(
                'session_identifier', '')
            vumi_session_identifier = make_vumi_session_identifier(
                to_addr, sixdee_session_identifier)

            its_session_info = (
                int(sixdee_session_identifier, 16) |
                int(not continue_session))

            service_op = self.ussd_service_op_map[('continue'
                                                   if continue_session
                                                   else 'close')]
            optional_parameters.update({
                'ussd_service_op': service_op,
                'its_session_info': "%04x" % (its_session_info,)
            })

            if not continue_session:
                yield self.session_manager.clear_session(
                    vumi_session_identifier)

        if self.config.send_long_messages:
            resp = yield protocol.submit_sm_long(
                vumi_message_id,
                to_addr.encode('ascii'),
                long_message=text.encode(self.config.submit_sm_encoding),
                data_coding=self.config.submit_sm_data_coding,
                source_addr=from_addr.encode('ascii'),
                optional_parameters=optional_parameters,
            )

        elif self.config.send_multipart_sar:
            resp = yield protocol.submit_csm_sar(
                vumi_message_id,
                to_addr.encode('ascii'),
                short_message=text.encode(self.config.submit_sm_encoding),
                data_coding=self.config.submit_sm_data_coding,
                source_addr=from_addr.encode('ascii'),
                optional_parameters=optional_parameters,
            )

        elif self.config.send_multipart_udh:
            resp = yield protocol.submit_csm_udh(
                vumi_message_id,
                to_addr.encode('ascii'),
                short_message=text.encode(self.config.submit_sm_encoding),
                data_coding=self.config.submit_sm_data_coding,
                source_addr=from_addr.encode('ascii'),
                optional_parameters=optional_parameters,
            )
        else:
            resp = yield protocol.submit_sm(
                vumi_message_id,
                to_addr.encode('ascii'),
                short_message=text.encode(self.config.submit_sm_encoding),
                data_coding=self.config.submit_sm_data_coding,
                source_addr=from_addr.encode('ascii'),
                optional_parameters=optional_parameters,
            )

        returnValue(resp)
