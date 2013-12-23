import json
from uuid import uuid4

from twisted.internet.defer import inlineCallbacks, returnValue

from zope.interface import implements

from vumi.transports.smpp.iprocessors import (IDeliveryReportProcessor,
                                              IDeliverShortMessageProcessor)
from vumi.transports.smpp.smpp_utils import unpacked_pdu_opts, detect_ussd
from vumi.config import Config, ConfigDict, ConfigRegex
from vumi import log

from smpp.pdu_inspector import (detect_multipart, multipart_key,
                                MultipartMessage)


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
        'Regex to use for matching delivery reports',
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
        """
        Check if this might be a delivery receipt with PDU parameters.
        There's a chance we'll get a delivery receipt without these
        parameters, if that happens we'll try a regex match in
        ``inspect_delivery_report_content`` once we've decoded the
        message properly.
        """

        pdu_opts = unpacked_pdu_opts(pdu)
        receipted_message_id = pdu_opts.get('receipted_message_id', None)
        message_state = pdu_opts.get('message_state', None)
        if receipted_message_id is not None and message_state is not None:
            return (receipted_message_id, message_state)

        return None

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

        return None

    def handle_delivery_report_content(self, pdu_data):
        receipted_message_id, message_state = pdu_data
        return self.protocol.esme_callbacks.delivery_report(
            message_id=receipted_message_id,
            delivery_status=self.delivery_status(message_state))

    def delivery_status(self, state):
        config = self.get_static_config()
        return config.delivery_report_status_mapping.get(state, 'pending')


class DeliverShortMessageProcessorConfig(Config):
    data_coding_overrides = ConfigDict(
        "Overrides for data_coding character set mapping. This is useful for "
        "setting the default encoding (0), adding additional undefined "
        "encodings (such as 4 or 8) or overriding encodings in cases where "
        "the SMSC is violating the spec (which happens a lot). Keys should "
        "be integers, values should be strings containing valid Python "
        "character encoding names.", default={}, static=True)


class EsmeCallbacksDeliverShortMessageProcessor(object):
    implements(IDeliverShortMessageProcessor)
    CONFIG_CLASS = DeliverShortMessageProcessorConfig

    def __init__(self, protocol, config):
        self.protocol = protocol
        self._static_config = self.CONFIG_CLASS(config, static=True)

    def get_static_config(self):
        return self._static_config

    def decode_message(self, message, data_coding):
        """
        Messages can arrive with one of a number of specified
        encodings. We only handle a subset of these.

        From the SMPP spec:

        00000000 (0) SMSC Default Alphabet
        00000001 (1) IA5(CCITTT.50)/ASCII(ANSIX3.4)
        00000010 (2) Octet unspecified (8-bit binary)
        00000011 (3) Latin1(ISO-8859-1)
        00000100 (4) Octet unspecified (8-bit binary)
        00000101 (5) JIS(X0208-1990)
        00000110 (6) Cyrllic(ISO-8859-5)
        00000111 (7) Latin/Hebrew (ISO-8859-8)
        00001000 (8) UCS2(ISO/IEC-10646)
        00001001 (9) PictogramEncoding
        00001010 (10) ISO-2022-JP(MusicCodes)
        00001011 (11) reserved
        00001100 (12) reserved
        00001101 (13) Extended Kanji JIS(X 0212-1990)
        00001110 (14) KSC5601
        00001111 (15) reserved

        Particularly problematic are the "Octet unspecified" encodings.
        """
        codecs = {
            1: 'ascii',
            3: 'latin1',
            8: 'utf-16be',  # Actually UCS-2, but close enough.
        }
        config = self.get_static_config()
        codecs.update(config.data_coding_overrides)
        codec = codecs.get(data_coding, None)
        if codec is None or message is None:
            log.msg("WARNING: Not decoding message with data_coding=%s" % (
                    data_coding,))
        else:
            try:
                return message.decode(codec)
            except Exception, e:
                log.msg("Error decoding message with data_coding=%s" % (
                        data_coding,))
                log.err(e)
        return message

    @inlineCallbacks
    def handle_short_message_pdu(self, pdu):
        # TODO: There's the possibility that we'd need to split this
        #       processor into separate `inspect_*` and `handle_*`
        #       functions. That work is currently left for when we have
        #       an implementation that could benefit from that as it
        #       would help us figure out how that functionality should
        #       actually be split up.
        pdu_params = pdu['body']['mandatory_parameters']
        pdu_opts = unpacked_pdu_opts(pdu)

        # We might have a `message_payload` optional field to worry about.
        message_payload = pdu_opts.get('message_payload', None)
        if message_payload is not None:
            pdu_params['short_message'] = message_payload.decode('hex')

        if detect_ussd(pdu_opts):
            # We have a USSD message.
            yield self.handle_deliver_sm_ussd(pdu, pdu_params, pdu_opts)
        elif detect_multipart(pdu):
            # We have a multipart SMS.
            yield self.handle_deliver_sm_multipart(pdu, pdu_params)
        else:
            decoded_msg = self.decode_message(pdu_params['short_message'],
                                              pdu_params['data_coding'])
            yield self.handle_short_message_content(
                source_addr=pdu_params['source_addr'],
                destination_addr=pdu_params['destination_addr'],
                short_message=decoded_msg)

    def handle_short_message_content(self, source_addr, destination_addr,
                                     short_message, **kw):
        dr_processor = self.protocol.dr_processor
        dr_data = dr_processor.inspect_delivery_report_content(short_message)
        if dr_data is not None:
            return dr_processor.handle_delivery_report_content(dr_data)

        return self.protocol.esme_callbacks.deliver_sm(
            source_addr=source_addr, destination_addr=destination_addr,
            short_message=short_message, message_id=uuid4().hex,
            **kw)

    def handle_deliver_sm_ussd(self, pdu, pdu_params, pdu_opts):
        # Some of this stuff might be specific to Tata's setup.

        service_op = pdu_opts['ussd_service_op']

        session_event = 'close'
        if service_op == '01':
            # PSSR request. Let's assume it means a new session.
            session_event = 'new'
        elif service_op == '11':
            # PSSR response. This means session end.
            session_event = 'close'
        elif service_op in ('02', '12'):
            # USSR request or response. I *think* we only get the latter.
            session_event = 'continue'

        # According to the spec, the first octet is the session id and the
        # second is the client dialog id (first 7 bits) and end session flag
        # (last bit).

        # Since we don't use the client dialog id and the spec says it's
        # ESME-defined, treat the whole thing as opaque "session info" that
        # gets passed back in reply messages.

        its_session_number = int(pdu_opts['its_session_info'], 16)
        end_session = bool(its_session_number % 2)
        session_info = "%04x" % (its_session_number & 0xfffe)

        if end_session:
            # We have an explicit "end session" flag.
            session_event = 'close'

        decoded_msg = self.decode_message(pdu_params['short_message'],
                                          pdu_params['data_coding'])
        return self.handle_short_message_content(
            source_addr=pdu_params['source_addr'],
            destination_addr=pdu_params['destination_addr'],
            short_message=decoded_msg,
            message_type='ussd',
            session_event=session_event,
            session_info=session_info)

    @inlineCallbacks
    def handle_deliver_sm_multipart(self, pdu, pdu_params):
        redis_key = "multi_%s" % (multipart_key(detect_multipart(pdu)),)
        log.debug("Redis multipart key: %s" % (redis_key))
        multi = yield self.load_multipart_message(redis_key)
        multi.add_pdu(pdu)
        completed = multi.get_completed()
        if completed:
            yield self.protocol.redis.delete(redis_key)
            log.msg("Reassembled Message: %s" % (completed['message']))
            # We assume that all parts have the same data_coding here, because
            # otherwise there's nothing sensible we can do.
            decoded_msg = self.decode_message(completed['message'],
                                              pdu_params['data_coding'])
            # and we can finally pass the whole message on
            yield self.handle_short_message_content(
                source_addr=completed['from_msisdn'],
                destination_addr=completed['to_msisdn'],
                short_message=decoded_msg)
        else:
            yield self.save_multipart_message(redis_key, multi)

    def _hex_for_redis(self, data_dict):
        for index, part in data_dict.items():
            part['part_message'] = part['part_message'].encode('hex')
        return data_dict

    def _unhex_from_redis(self, data_dict):
        for index, part in data_dict.items():
            part['part_message'] = part['part_message'].decode('hex')
        return data_dict

    @inlineCallbacks
    def load_multipart_message(self, redis_key):
        value = yield self.protocol.redis.get(redis_key)
        value = json.loads(value) if value else {}
        log.debug("Retrieved value: %s" % (repr(value)))
        returnValue(MultipartMessage(self._unhex_from_redis(value)))

    def save_multipart_message(self, redis_key, multipart_message):
        data_dict = self._hex_for_redis(multipart_message.get_array())
        return self.protocol.redis.set(redis_key, json.dumps(data_dict))
