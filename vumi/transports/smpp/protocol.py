# -*- test-case-name: vumi.transports.smpp.tests.test_protocol -*-

from functools import wraps
from random import randint

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet.task import LoopingCall
from twisted.internet.defer import (
    inlineCallbacks, returnValue, maybeDeferred, succeed)

from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (
    BindTransceiver, BindReceiver, BindTransmitter,
    UnbindResp, Unbind,
    DeliverSMResp,
    EnquireLink, EnquireLinkResp,
    SubmitSM, QuerySM)

from vumi import log
from vumi.transports.smpp.smpp_utils import update_ussd_pdu
from vumi.transports.smpp.pdu_utils import (pdu_ok, seq_no, command_status,
                                            command_id, message_id,
                                            chop_pdu_stream)
from vumi.transports.smpp.config import EsmeConfig

GSM_MAX_SMS_BYTES = 140


def require_bind(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.isBound():
            raise EsmeProtocolError('%s called in unbound state.' % (func,))
        return func(self, *args, **kwargs)
    return wrapper


class EsmeProtocolError(Exception):
    pass


class EsmeTransceiver(Protocol):

    bind_pdu = BindTransceiver
    clock = reactor

    CONFIG_CLASS = EsmeConfig

    OPEN_STATE = 'OPEN'
    CLOSED_STATE = 'CLOSED'
    BOUND_STATE_TRX = 'BOUND_TRX'
    BOUND_STATE_TX = 'BOUND_TX'
    BOUND_STATE_RX = 'BOUND_RX'
    BOUND_STATES = set([
        BOUND_STATE_RX,
        BOUND_STATE_TX,
        BOUND_STATE_TRX,
    ])

    def __init__(self, vumi_transport):
        """
        An SMPP 3.4 client suitable for use by a Vumi Transport.

        :param SmppTransceiverProtocol vumi_transport:
            The transport that is using this protocol to communicate
            with an SMSC.
        """
        self.vumi_transport = vumi_transport
        self.config = self.CONFIG_CLASS(
            self.vumi_transport.get_static_config().smpp_config, static=True)

        self.buffer = b''
        self.state = self.CLOSED_STATE

        self.sm_processor = self.vumi_transport.sm_processor
        self.dr_processor = self.vumi_transport.dr_processor
        self.sequence_generator = self.vumi_transport.sequence_generator
        self.enquire_link_call = LoopingCall(self.enquireLink)
        self.drop_link_call = None
        self.disconnect_call = self.clock.callLater(
            self.config.smpp_enquire_link_interval, self.disconnect,
            'Disconnecting, no response from SMSC for longer '
            'than %s seconds' % (self.config.smpp_enquire_link_interval,))

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

    def connectionMade(self):
        self.state = self.OPEN_STATE
        return self.onConnectionMade()

    def onConnectionMade(self):
        log.msg('Connection Made')

    @inlineCallbacks
    def bind(self,
             system_id,
             password,
             system_type,
             interface_version='34',
             addr_ton='',
             addr_npi='',
             address_range=''):
        """
        Send the `bind_transmitter`, `bind_transceiver` or `bind_receiver`
        PDU to the SMSC in order to establish the connection.

        :param str system_id:
            Identifies the ESME system requesting to bind.
        :param str password:
            The password may be used by the SMSC to authenticate the
            ESME requesting to bind.
        :param str system_type:
            Identifies the type of ESME system requesting to bind
            with the SMSC.
        :param str interface_version:
            Indicates the version of the SMPP protocol supported by the
            ESME.
        :param str addr_ton:
            Indicates Type of Number of the ESME address.
        :param str addr_npi:
            Numbering Plan Indicator for ESME address.
        :param str address_range:
            The ESME address.
        """
        sequence_number = yield self.sequence_generator.next()
        pdu = self.bind_pdu(
            sequence_number, system_id=system_id, password=password,
            system_type=system_type, interface_version=interface_version,
            addr_ton=addr_ton, addr_npi=addr_npi, address_range=address_range)
        self.sendPDU(pdu)
        self.drop_link_call = self.clock.callLater(
            self.config.smpp_bind_timeout, self.dropLink)

    def dropLink(self):
        """Called if the SMPP connection is not bound within
        ``smpp_bind_timeout`` amount of seconds"""
        if self.isBound():
            return

        return self.disconnect(
            'Dropping link due to binding delay. Current state: %s' % (
                self.state))

    def disconnect(self, msg=None):
        if msg is not None:
            log.warning(msg)
        self.transport.loseConnection()

    def connectionLost(self, reason):
        self.state = self.CLOSED_STATE
        self.onConnectionLost(reason)

    def onConnectionLost(self, reason):
        if self.enquire_link_call.running:
            self.enquire_link_call.stop()
        if self.drop_link_call is not None and self.drop_link_call.active():
            self.drop_link_call.cancel()
        if self.disconnect_call.active():
            self.disconnect_call.cancel()

    def isBound(self):
        return self.state in self.BOUND_STATES

    @require_bind
    @inlineCallbacks
    def enquireLink(self):
        """Ping the SMSC to see if they're still around"""
        sequence_number = yield self.sequence_generator.next()
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
            log.warning('Unable to bind: %r' % (command_status(pdu),))
            self.transport.loseConnection()

        self.state = self.BOUND_STATE_TRX
        return self.onBindTransceiverResp(seq_no(pdu))

    def onBindTransceiverResp(self, sequence_number):
        return self.onSmppBind(sequence_number)

    def handle_bind_transmitter_resp(self, pdu):
        if not pdu_ok(pdu):
            log.warning('Unable to bind: %r' % (command_status(pdu),))
            self.transport.loseConnection()

        self.state = self.BOUND_STATE_TX
        return self.onBindTransmitterResp(seq_no(pdu))

    def onBindTransmitterResp(self, sequence_number):
        return self.onSmppBind(sequence_number)

    def handle_bind_receiver_resp(self, pdu):
        if not pdu_ok(pdu):
            log.warning('Unable to bind: %r' % (command_status(pdu),))
            self.transport.loseConnection()

        self.state = self.BOUND_STATE_RX
        return self.onBindReceiverResp(seq_no(pdu))

    def onBindReceiverResp(self, sequence_number):
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
        log.warning(
            'onSubmitSMResp called but not implemented by ESME class.')

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
        # TODO: split out **kwargs into explicit params with
        #       sensible defaults
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
        sequence_number = yield self.sequence_generator.next()
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
        sequence_number = yield self.sequence_generator.next()
        pdu = QuerySM(
            sequence_number, message_id=message_id, source_addr=source_addr,
            **self.getBindParams())
        self.sendPDU(pdu)
        returnValue([sequence_number])

    @require_bind
    @inlineCallbacks
    def unbind(self):
        sequence_number = yield self.sequence_generator.next()
        self.sendPDU(Unbind(sequence_number))
        returnValue([sequence_number])

    def handle_unbind_resp(self, pdu):
        self.onUnbindResp(seq_no(pdu))

    def onUnbindResp(self, sequence_number):
        log.msg('Unbind successful.')


class EsmeTransceiverFactory(ClientFactory):

    protocol = EsmeTransceiver

    def __init__(self, transport):
        self.transport = transport

    def buildProtocol(self, addr):
        proto = self.protocol(self.transport)
        proto.factory = self
        return proto


class EsmeReceiver(EsmeTransceiver):
    bind_pdu = BindReceiver


class EsmeReceiverFactory(EsmeTransceiverFactory):
    protocol = EsmeReceiver


class EsmeTransmitter(EsmeTransceiver):
    bind_pdu = BindTransmitter


class EsmeTransmitterFactory(EsmeTransceiverFactory):
    protocol = EsmeTransmitter
