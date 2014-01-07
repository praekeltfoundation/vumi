# -*- test-case-name: vumi.transports.smpp.clientserver.tests.test_new_client -*-

from functools import wraps
from random import randint

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet.task import LoopingCall
from twisted.internet.defer import (
    inlineCallbacks, returnValue, maybeDeferred, succeed)

import binascii
from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (
    BindTransceiver, UnbindResp,
    DeliverSMResp,
    EnquireLink, EnquireLinkResp,
    SubmitSM, QuerySM)

from vumi import log

GSM_MAX_SMS_BYTES = 140


def require_bind(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.isBound():
            raise EsmeProtocolError('%s called in unbound state.' % (func,))
        return func(self, *args, **kwargs)
    return wrapper


def pdu_ok(pdu):
    return command_status(pdu) == 'ESME_ROK'


def pdu_reply(seq_number, reply_pdu):
    reply_pdu['header']['sequence_number'] = seq_number
    return reply_pdu


def seq_no(pdu):
    return pdu['header']['sequence_number']


def command_status(pdu):
    return pdu['header']['command_status']


def command_id(pdu):
    return pdu['header']['command_id']


def message_id(pdu):
    return pdu['body']['mandatory_parameters']['message_id']


class EsmeProtocolError(Exception):
    pass


def chop_pdu_stream(data):
    if len(data) < 16:
        return

    bytes = binascii.b2a_hex(data[0:4])
    cmd_length = int(bytes, 16)
    if len(data) < cmd_length:
        return

    pdu, data = (data[0:cmd_length],
                 data[cmd_length:])
    return pdu, data


class EsmeTransceiver(Protocol):
    BIND_PDU = BindTransceiver

    OPEN_STATE = 'OPEN'
    CLOSED_STATE = 'CLOSED'
    BOUND_STATE_TRX = 'BOUND_TRX'
    BOUND_STATE_TR = 'BOUND_TR'
    BOUND_STATE_RX = 'BOUND_RX'
    BOUND_STATES = set([
        BOUND_STATE_RX,
        BOUND_STATE_TR,
        BOUND_STATE_TRX,
    ])

    clock = reactor

    def __init__(self, config, sm_processor, dr_processor, sequence_generator):
        self.buffer = b''
        self.state = self.CLOSED_STATE
        self.config = config
        self.sm_processor = sm_processor
        self.dr_processor = dr_processor
        self.sequence_generator = sequence_generator
        self.enquire_link_call = LoopingCall(self.enquireLink)
        self.drop_link_call = None
        self.disconnect_call = self.clock.callLater(
            config.smpp_enquire_link_interval, self.disconnect,
            'Disconnecting, no response from SMSC for longer '
            'than %s seconds' % (config.smpp_enquire_link_interval,))

    def getBindParams(self):
        # TODO: validate these bind params somewhere as a config option
        #
        # Which of the keys in SmppTransportConfig are keys that are to
        # be passed on to the ESMETransceiver base class to create a bind with.
        bind_keys = [
            'system_id',
            'password',
            'system_type',
            'interface_version',
            'service_type',
            'dest_addr_ton',
            'dest_addr_npi',
            'source_addr_ton',
            'source_addr_npi',
            'registered_delivery',
        ]
        return dict([(key, getattr(self.config, key))
                     for key in bind_keys if hasattr(self.config, key)])

    def get_next_seq(self):
        """TODO: refactor into proper sequence number generator"""
        return self.sequence_generator.next()

    def push_unacked(self, sequence_number):
        """TODO: refactor into something relevant"""
        return succeed(sequence_number)

    def connectionMade(self):
        self.state = self.OPEN_STATE
        self.onConnectionMade()

    @inlineCallbacks
    def onConnectionMade(self):
        sequence_number = yield self.get_next_seq()
        bind_params = self.getBindParams()
        pdu = self.BIND_PDU(sequence_number, **bind_params)
        self.sendPDU(pdu)
        self.drop_link_call = self.clock.callLater(
            self.config.smpp_bind_timeout, self.dropLink)

    def dropLink(self):
        """Called if the SMPP connection is not bound within
        ``smpp_bind_timeout`` amount of seconds"""
        if self.isBound():
            return

        self.disconnect(
            'Dropping link due to binding delay. Current state: %s' % (
                self.state))

    def disconnect(self, msg=None):
        if msg is not None:
            log.warning(msg)
        self.transport.loseConnection()

    def connectionLost(self, reason):
        self.state = self.CLOSED_STATE
        self.onConnectionLost()

    def onConnectionLost(self):
        if self.enquire_link_call.running:
            self.enquire_link_call.stop()
        if self.drop_link_call is not None and self.drop_link_call.active():
            self.drop_link_call.cancel()

    def isBound(self):
        return self.state in self.BOUND_STATES

    @require_bind
    @inlineCallbacks
    def enquireLink(self):
        """Ping the SMSC to see if they're still around"""
        sequence_number = yield self.get_next_seq()
        self.sendPDU(EnquireLink(sequence_number))
        returnValue(sequence_number)

    def sendPDU(self, pdu):
        return self.transport.write(pdu.get_bin())

    def dataReceived(self, data):
        self.buffer += data
        data = self.handleBuffer()
        while data is not None:
            self.onPdu(unpack_pdu(data))
            data = self.handleBuffer()

    def handleBuffer(self):
        pdu_found = chop_pdu_stream(self.buffer)
        if pdu_found is None:
            return

        data, self.buffer = pdu_found
        return data

    def onPdu(self, pdu):
        handler = getattr(self, 'handle_%s' % (command_id(pdu),),
                          self.onUnsupportedCommandId)
        self.disconnect_call.reset(self.config.smpp_enquire_link_interval)
        return maybeDeferred(handler, pdu)

    def onUnsupportedCommandId(self, pdu):
        """
        Called when an SMPP PDU is received for which no handler function has
        been defined.
        """
        log.warning(
            'Received unsupported SMPP command_id: %r' % (command_id(pdu),))

    def handle_bind_transceiver_resp(self, pdu):
        if not pdu_ok(pdu):
            log.warning('Unable to bind: %r' % (command_status(pdu)))
            self.transport.loseConnection()

        self.state = self.BOUND_STATE_TRX
        return self.onBindTransceiverResp(seq_no(pdu))

    def onBindTransceiverResp(self, sequence_number):
        return self.onSmppBind(sequence_number)

    def onSmppBind(self, sequence_number):
        """Called when the bind has been setup"""
        self.enquire_link_call.start(self.config.smpp_enquire_link_interval)

    def handle_unbind(self, pdu):
        return self.onUnbind(seq_no(pdu))

    def onUnbind(self, sequence_number):
        return self.sendPDU(UnbindResp(sequence_number))

    def handle_submit_sm_resp(self, pdu):
        return self.onSubmitSMResp(
            seq_no(pdu), message_id(pdu), command_status(pdu))

    def onSubmitSMResp(self, sequence_number, message_id, command_status):
        """TODO: to be implemented"""

    @require_bind
    @inlineCallbacks
    def handle_deliver_sm(self, pdu):
        command_status = yield self.onDeliverSM(seq_no(pdu), pdu)
        self.sendPDU(DeliverSMResp(
            seq_no(pdu), command_status=command_status or 'ESME_ROK',
            **self.getBindParams()))

    @inlineCallbacks
    def onDeliverSM(self, sequence_number, pdu):
        was_dr = yield self.dr_processor.handle_delivery_report_pdu(pdu)
        if was_dr:
            return

        was_multipart = yield self.sm_processor.handle_multipart_pdu(pdu)
        if was_multipart:
            return

        was_ussd = yield self.sm_processor.handle_ussd_pdu(pdu)
        if was_ussd:
            return

        content_parts = self.sm_processor.decode_pdus([pdu])
        if not all([isinstance(part, unicode) for part in content_parts]):
            log.msg('Not all parts of the PDU were able to be decoded.',
                    parts=content_parts)
            returnValue('ESME_RDELIVERYFAILURE')

        content = u''.join(content_parts)
        was_cdr = yield self.dr_processor.handle_delivery_report_content(
            content)
        if was_cdr:
            return

        yield self.sm_processor.handle_short_message_pdu(pdu)

    def handle_enquire_link(self, pdu):
        return self.sendPDU(EnquireLinkResp(seq_no(pdu)))

    def handle_enquire_link_resp(self, pdu):
        return self.onEnquireLinkResp(seq_no(pdu))

    def onEnquireLinkResp(self, sequence_number):
        """TODO: to be implemented"""

    @require_bind
    @inlineCallbacks
    def submitSM(self, **kwargs):
        pdu_params = self.getBindParams()
        pdu_params.update(kwargs)
        message = pdu_params['short_message']

        # We use GSM_MAX_SMS_BYTES here because we may have already-encoded
        # UCS-2 data to send and therefore can't use the 160 (7-bit) character
        # limit everyone knows and loves. If we have some other encoding
        # instead, this may result in unnecessarily short message parts. The
        # SMSC is probably going to treat whatever we send it as whatever
        # encoding it likes best and then encode (or mangle) it into a form it
        # thinks should be in the GSM message payload. Basically, when we have
        # to split messages up ourselves here we've already lost and the best
        # we can hope for is not getting hurt too badly by the inevitable
        # breakages.
        if len(message) > GSM_MAX_SMS_BYTES:
            if self.config.send_multipart_sar:
                sequence_numbers = yield self._submit_multipart_sar(
                    **pdu_params)
                returnValue(sequence_numbers)
            elif self.config.send_multipart_udh:
                sequence_numbers = yield self._submit_multipart_udh(
                    **pdu_params)
                returnValue(sequence_numbers)

        sequence_number = yield self._submit_sm(**pdu_params)
        returnValue([sequence_number])

    @inlineCallbacks
    def _submit_sm(self, **pdu_params):
        sequence_number = yield self.get_next_seq()
        message = pdu_params['short_message']
        sar_params = pdu_params.pop('sar_params', None)
        message_type = pdu_params.pop('message_type', 'sms')
        continue_session = pdu_params.pop('continue_session', True)
        session_info = pdu_params.pop('session_info', None)

        pdu = SubmitSM(sequence_number, **pdu_params)
        if message_type == 'ussd':
            update_ussd_pdu(pdu, continue_session, session_info)

        if self.config.send_long_messages and len(message) > 254:
            pdu.add_message_payload(''.join('%02x' % ord(c) for c in message))

        if sar_params:
            pdu.set_sar_msg_ref_num(sar_params['msg_ref_num'])
            pdu.set_sar_total_segments(sar_params['total_segments'])
            pdu.set_sar_segment_seqnum(sar_params['segment_seqnum'])

        self.sendPDU(pdu)
        yield self.push_unacked(sequence_number)
        returnValue(sequence_number)

    @inlineCallbacks
    def _submit_multipart_sar(self, **pdu_params):
        message = pdu_params['short_message']
        split_msg = []
        # We chop the message into 130 byte chunks to leave 10 bytes for the
        # user data header the SMSC is presumably going to add for us. This is
        # a guess based mostly on optimism and the hope that we'll never have
        # to deal with this stuff in production.
        # FIXME: If we have utf-8 encoded data, we might break in the
        # middle of a multibyte character.
        payload_length = GSM_MAX_SMS_BYTES - 10
        while message:
            split_msg.append(message[:payload_length])
            message = message[payload_length:]
        ref_num = randint(1, 255)
        sequence_numbers = []
        for i, msg in enumerate(split_msg):
            params = pdu_params.copy()
            params['short_message'] = msg
            params['sar_params'] = {
                'msg_ref_num': ref_num,
                'total_segments': len(split_msg),
                'segment_seqnum': i + 1,
            }
            sequence_number = yield self._submit_sm(**params)
            sequence_numbers.append(sequence_number)
        returnValue(sequence_numbers)

    @inlineCallbacks
    def _submit_multipart_udh(self, **pdu_params):
        message = pdu_params['short_message']
        split_msg = []
        # We chop the message into 130 byte chunks to leave 10 bytes for the
        # 6-byte user data header we add and a little extra space in case the
        # SMSC does unexpected things with our message.
        # FIXME: If we have utf-8 encoded data, we might break in the
        # middle of a multibyte character.
        payload_length = GSM_MAX_SMS_BYTES - 10
        while message:
            split_msg.append(message[:payload_length])
            message = message[payload_length:]
        ref_num = randint(1, 255)
        sequence_numbers = []
        for i, msg in enumerate(split_msg):
            params = pdu_params.copy()
            # 0x40 is the UDHI flag indicating that this payload contains a
            # user data header.
            params['esm_class'] = 0x40
            # See http://en.wikipedia.org/wiki/User_Data_Header for an
            # explanation of the magic numbers below. We should probably
            # abstract this out into a class that makes it less magic and
            # opaque.
            udh = '\05\00\03%s%s%s' % (
                chr(ref_num), chr(len(split_msg)), chr(i + 1))
            params['short_message'] = udh + msg
            sequence_number = yield self._submit_sm(**params)
            sequence_numbers.append(sequence_number)
        returnValue(sequence_numbers)

    @require_bind
    @inlineCallbacks
    def querySM(self, message_id, source_addr, **kwargs):
        sequence_number = yield self.get_next_seq()
        pdu = QuerySM(
            sequence_number, message_id=message_id, source_addr=source_addr,
            **self.getBindParams())
        self.sendPDU(pdu)
        returnValue([sequence_number])


class EsmeTransceiverFactory(ClientFactory):

    protocol = EsmeTransceiver

    def __init__(self, config, sm_processor, dr_processor, sequence_generator):
        self.config = config
        self.sm_processor = sm_processor
        self.dr_processor = dr_processor
        self.sequence_generator = sequence_generator

    def buildProtocol(self, addr):
        proto = self.protocol(
            self.config, self.sm_processor, self.dr_processor,
            self.sequence_generator)
        proto.factory = self
        return proto
