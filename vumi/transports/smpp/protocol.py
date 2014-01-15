# -*- test-case-name: vumi.transports.smpp.tests.test_protocol -*-

from functools import wraps

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet.task import LoopingCall
from twisted.internet.defer import inlineCallbacks, returnValue, maybeDeferred

from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (
    BindTransceiver, BindReceiver, BindTransmitter,
    UnbindResp, Unbind,
    DeliverSMResp,
    EnquireLink, EnquireLinkResp,
    SubmitSM, QuerySM)

from vumi import log
from vumi.transports.smpp.pdu_utils import (
    pdu_ok, seq_no, command_status, command_id, message_id, chop_pdu_stream)

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
        self.config = self.vumi_transport.get_static_config()

        self.buffer = b''
        self.state = self.CLOSED_STATE

        self.deliver_sm_processor = self.vumi_transport.deliver_sm_processor
        self.dr_processor = self.vumi_transport.dr_processor
        self.sequence_generator = self.vumi_transport.sequence_generator
        self.enquire_link_call = LoopingCall(self.enquireLink)
        self.drop_link_call = None
        self.disconnect_call = self.clock.callLater(
            self.config.smpp_enquire_link_interval, self.disconnect,
            'Disconnecting, no response from SMSC for longer '
            'than %s seconds' % (self.config.smpp_enquire_link_interval,))

    def connectionMade(self):
        self.state = self.OPEN_STATE
        return self.onConnectionMade()

    def onConnectionMade(self):
        """Called immediately after connectionMade()"""
        log.msg('Connection made, current state: %s' % (self.state,))

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
        """
        Called if the SMPP connection is not bound within
        ``smpp_bind_timeout`` amount of seconds
        """
        if self.isBound():
            return

        return self.disconnect(
            'Dropping link due to binding delay. Current state: %s' % (
                self.state))

    def disconnect(self, log_msg=None):
        """
        Forcibly close the connection, logging ``log_msg`` if provided.

        :param str log_msg:
            The entry to write to the log file.
        """
        if log_msg is not None:
            log.warning(log_msg)
        self.transport.loseConnection()

    def connectionLost(self, reason):
        self.state = self.CLOSED_STATE
        self.onConnectionLost(reason)

    def onConnectionLost(self, reason):
        """
        :param Exception reason:
            The reason for the connection closed, generally a
            ``ConnectionDone``
        """
        if self.enquire_link_call.running:
            self.enquire_link_call.stop()
        if self.drop_link_call is not None and self.drop_link_call.active():
            self.drop_link_call.cancel()
        if self.disconnect_call.active():
            self.disconnect_call.cancel()

    def isBound(self):
        """
        Returns ``True`` if the connection is in one of the known
        values of ``self.BOUND_STATES``
        """
        return self.state in self.BOUND_STATES

    @require_bind
    @inlineCallbacks
    def enquireLink(self):
        """
        Ping the SMSC to see if they're still around.
        """
        sequence_number = yield self.sequence_generator.next()
        self.sendPDU(EnquireLink(sequence_number))
        returnValue(sequence_number)

    def sendPDU(self, pdu):
        """
        Send a PDU to the SMSC

        :param smpp.pdu_builder.PDU pdu:
            The PDU object to send.
        """
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
        """
        Handle a PDU that was received & decoded.

        :param dict pdu:
            The dict result one gets when calling ``smpp.pdu.unpack_pdu()``
            on the received PDU
        """
        handler = getattr(self, 'handle_%s' % (command_id(pdu),),
                          self.onUnsupportedCommandId)
        self.disconnect_call.reset(self.config.smpp_enquire_link_interval)
        return maybeDeferred(handler, pdu)

    def onUnsupportedCommandId(self, pdu):
        """
        Called when an SMPP PDU is received for which no handler function has
        been defined.

        :param dict pdu:
            The dict result one gets when calling ``smpp.pdu.unpack_pdu()``
            on the received PDU
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

    def onSubmitSMResp(self, sequence_number, smpp_message_id, command_status):
        """
        Called when a ``submit_sm_resp`` command was received.

        :param int sequence_number:
            The sequence_number of the command, should correlate with the
            sequence_number of the ``submit_sm`` command that this is a
            response to.
        :param str smpp_message_id:
            The message id that the SMSC is using for this message.
            This will be referred to in the delivery reports (if any).
        :param str command_status:
            The SMPP command_status for this command. Will determine if
            the ``submit_sm`` command was successful or not. Refer to the
            SMPP specification for full list of options.

        """
        log.warning(
            'onSubmitSMResp called but not implemented by ESME class.')

    def handle_deliver_sm(self, pdu):
        return self.onDeliverSM(seq_no(pdu), pdu)

    @inlineCallbacks
    def onDeliverSM(self, sequence_number, pdu):

        # These operate before the PDUs ``short_message`` or
        # ``message_payload`` fields have been string decoded.
        # NOTE: order is important!
        pdu_handler_chain = [
            self.dr_processor.handle_delivery_report_pdu,
            self.deliver_sm_processor.handle_multipart_pdu,
            self.deliver_sm_processor.handle_ussd_pdu,
        ]
        for handler in pdu_handler_chain:
            handled = yield handler(pdu)
            if handled:
                self.sendPDU(DeliverSMResp(seq_no(pdu),
                             command_status='ESME_ROK'))
                return

        # At this point we either have a DR in the message payload
        # or have a normal SMS that needs to be decoded and handled.
        content_parts = self.deliver_sm_processor.decode_pdus([pdu])
        if not all([isinstance(part, unicode) for part in content_parts]):
            log.msg('Not all parts of the PDU were able to be decoded.',
                    parts=content_parts)
            self.sendPDU(DeliverSMResp(seq_no(pdu),
                         command_status='ESME_RDELIVERYFAILURE'))
            return

        content = u''.join(content_parts)
        was_cdr = yield self.dr_processor.handle_delivery_report_content(
            content)
        if was_cdr:
            self.sendPDU(DeliverSMResp(seq_no(pdu),
                         command_status='ESME_ROK'))
            return

        handled = yield self.deliver_sm_processor.handle_short_message_pdu(pdu)
        command_status = 'ESME_ROK' if handled else 'ESME_RDELIVERYFAILURE'
        self.sendPDU(DeliverSMResp(seq_no(pdu),
                     command_status=command_status))

    def handle_enquire_link(self, pdu):
        return self.sendPDU(EnquireLinkResp(seq_no(pdu)))

    def handle_enquire_link_resp(self, pdu):
        return self.onEnquireLinkResp(seq_no(pdu))

    def onEnquireLinkResp(self, sequence_number):
        """
        Called when an ``enquire_link_resp`` command was received.

        :param int sequence_number:
            The sequence_number of the received command. Should correlate
            with the sequence_number of the original ``enquire_link``
            command that was sent to the SMSC.
        """

    @require_bind
    @inlineCallbacks
    def submit_sm(self,
                  destination_addr,
                  source_addr='',
                  service_type='',
                  source_addr_ton=0,
                  source_addr_npi=0,
                  dest_addr_ton=0,
                  dest_addr_npi=1,
                  esm_class=0,
                  protocol_id=0,
                  priority_flag=0,
                  schedule_delivery_time='',
                  validity_period='',
                  registered_delivery=1,
                  replace_if_present=0,
                  data_coding=0,
                  sm_default_msg_id=0,
                  sm_length=0,
                  short_message='',
                  optional_parameters=None
                  ):
        """
        Put a `submit_sm` command on the wire.

        :param str source_addr:
            Address of SME which originated this message.
            If unknown leave blank.
        :param str destination_addr:
            Destination address of this short message.
            For mobile terminated messages, this is the directory number
            of the recipient MS.
        :param str service_type:
            The service_type parameter can be used to indicate the SMS
            Application service associated with the message.
            If unknown leave blank.
        :param int source_addr_ton:
            Type of Number for source address.
        :param int source_addr_npi:
            Numbering Plan Indicator for source address.
        :param int dest_addr_ton:
            Type of Number for destination.
        :param int dest_addr_npi:
            Numbering Plan Indicator for destination.
        :param int esm_class:
            Indicates Message Mode & Message Type.
        :param int protocol_id:
            Protocol Identifier. Network specific field.
        :param int priority_flag:
            Designates the priority level of the message.
        :param str schedule_delivery_time:
            The short message is to be scheduled by the SMSC for delivery.
            Leave blank for immediate delivery.
        :param str validity_period:
            The validity period of this message.
            Leave blank for SMSC default.
        :param int registered_delivery:
            Indicator to signify if an SMSC delivery receipt or an SME
            acknowledgement is required.
        :param int replace_if_present:
            Flag indicating if submitted message should replace an
            existing message.
        :param int data_coding:
            Defines the encoding scheme of the short message user data.
        :param int sm_default_msg_id:
            Indicates the short message to send from a list of pre- defined
            ('canned') short messages stored on the SMSC.
            Leave blank if not using an SMSC canned message.
        :param int sm_length:
            Length in octets of the short_message user data.
            This is automatically calculated and set during PDU encoding,
            no need to specify.
        :param int short_message:
            Up to 254 octets of short message user data.
            The exact physical limit for short_message size may vary
            according to the underlying network.
            Applications which need to send messages longer than 254
            octets should use the message_payload parameter. In this
            case the sm_length field should be set to zero.
        :param dict optional_parameters:
            keys and values to be embedded in the PDU as tag-length-values.
            Refer to the SMPP specification and your SMSCs instructions
            on what valid and suitable keys and values are.
        :returns: list of 1 sequence number (int) for consistency with other
                  submit_sm calls.
        :rtype: list

        """
        sequence_number = yield self.sequence_generator.next()
        pdu = SubmitSM(
            sequence_number=sequence_number,
            source_addr=source_addr,
            destination_addr=destination_addr,
            service_type=service_type,
            source_addr_ton=source_addr_ton,
            source_addr_npi=source_addr_npi,
            dest_addr_ton=dest_addr_ton,
            dest_addr_npi=dest_addr_npi,
            esm_class=esm_class,
            protocol_id=protocol_id,
            priority_flag=priority_flag,
            schedule_delivery_time=schedule_delivery_time,
            validity_period=validity_period,
            registered_delivery=registered_delivery,
            replace_if_present=replace_if_present,
            data_coding=data_coding,
            sm_default_msg_id=sm_default_msg_id,
            sm_length=sm_length,
            short_message=short_message)

        if optional_parameters:
            for key, value in optional_parameters.items():
                pdu.add_optional_parameter(key, value)

        self.sendPDU(pdu)
        returnValue([sequence_number])

    def submit_sm_long(self, destination_addr, long_message, **pdu_params):
        """
        Send a `submit_sm` command with the message encoded in the
        ``message_payload`` optional parameter.

        Same parameters apply as for ``submit_sm`` with the exception
        that the ``short_message`` keyword argument is disallowed
        because it conflicts with the ``long_message`` field.

        :returns: list of 1 sequence number, int.
        :rtype: list

        """
        if 'short_message' in pdu_params:
            raise EsmeProtocolError(
                'short_message not allowed when sending a long message'
                'in the message_payload')

        optional_parameters = pdu_params.pop('optional_parameters', {}).copy()
        optional_parameters.update({
            'message_payload': (
                ''.join('%02x' % ord(c) for c in long_message))
        })
        return self.submit_sm(
            destination_addr, short_message='', sm_length=0,
            optional_parameters=optional_parameters, **pdu_params)

    def csm_split_message(self, message):
        """
        Chop the message into 130 byte chunks to leave 10 bytes for the
        user data header the SMSC is presumably going to add for us. This is
        a guess based mostly on optimism and the hope that we'll never have
        to deal with this stuff in production.

        NOTE: If we have utf-8 encoded data, we might break in the
              middle of a multibyte character. This should be ok since
              the message is only decoded after re-assembly of all
              individual segments.

        :param str message:
            The message to split
        :returns: list of strings
        :rtype: list

        """
        payload_length = GSM_MAX_SMS_BYTES - 10
        split_msg = []
        while message:
            split_msg.append(message[:payload_length])
            message = message[payload_length:]
        return split_msg

    @inlineCallbacks
    def submit_csm_sar(self, destination_addr, **pdu_params):
        """
        Submit a concatenated SMS to the SMSC using the optional
        SAR parameter names in the various PDUS.

        :returns: List of sequence numbers (int) for each of the segments.
        :rtype: list
        """

        optional_parameters = pdu_params.pop('optional_parameters', {}).copy()
        split_msg = self.csm_split_message(pdu_params.pop('short_message'))
        ref_num = yield self.sequence_generator.next()
        sequence_numbers = []
        for i, msg in enumerate(split_msg):
            pdu_params = pdu_params.copy()
            optional_parameters.update({
                'sar_msg_ref_num': ref_num,
                'sar_total_segments': len(split_msg),
                'sar_segment_seqnum': i + 1,
            })
            sequence_number = yield self.submit_sm(
                destination_addr, short_message=msg,
                optional_parameters=optional_parameters, **pdu_params)
            sequence_numbers.extend(sequence_number)
        returnValue(sequence_numbers)

    @inlineCallbacks
    def submit_csm_udh(self, destination_addr, **pdu_params):
        """
        Submit a concatenated SMS to the SMSC using UDH headers
        in the message content.

        Same parameters apply as for ``submit_sm`` with the exception
        that the ``esm_class`` keyword argument is disallowed
        because the SMPP spec mandates a value that is to be set for UDH.

        :returns: List of sequence numbers (int) for each of the segments.
        :rtype: list
        """

        if 'esm_class' in pdu_params:
            raise EsmeProtocolError(
                'Cannot specify esm_class, GSM spec sets this at 0x40 '
                'for concatenated messages using UDH.')

        pdu_params = pdu_params.copy()
        split_msg = self.csm_split_message(pdu_params.pop('short_message'))
        ref_num = yield self.sequence_generator.next()
        sequence_numbers = []
        for i, msg in enumerate(split_msg):
            # 0x40 is the UDHI flag indicating that this payload contains a
            # user data header.

            # NOTE: Looking at the SMPP specs I can find no requirement
            #       for this anywhere.
            pdu_params['esm_class'] = 0x40

            # See http://en.wikipedia.org/wiki/User_Data_Header and
            # http://en.wikipedia.org/wiki/Concatenated_SMS for an
            # explanation of the magic numbers below. We should probably
            # abstract this out into a class that makes it less magic and
            # opaque.
            udh = ''.join([
                '\05',  # Full UDH header length
                '\00',  # Information Element Identifier for Concatenated SMS
                '\03',  # header length
                chr(ref_num),
                chr(len(split_msg)),
                chr(i + 1),
            ])
            short_message = udh + msg
            sequence_number = yield self.submit_sm(
                destination_addr, short_message=short_message, **pdu_params)
            sequence_numbers.extend(sequence_number)
        returnValue(sequence_numbers)

    @require_bind
    @inlineCallbacks
    def query_sm(self,
                 message_id,
                 source_addr_ton=0,
                 source_addr_npi=0,
                 source_addr=''
                 ):
        """
        Query the SMSC for the status of an earlier sent message.

        :param str message_id:
            Message ID of the message whose state is to be queried.
            This must be the SMSC assigned Message ID allocated to the
            original short message when submitted to the SMSC by the
            submit_sm, data_sm or submit_multi command, and returned
            in the response PDU by the SMSC.
        :param int source_addr_ton:
            Type of Number of message originator. This is used for
            verification purposes, and must match that supplied in the
            original request PDU (e.g. submit_sm).
        :param int source_addr_npi:
            Numbering Plan Identity of message originator. This is used
            for verification purposes, and must match that supplied in
            the original request PDU (e.g. submit_sm).
        :param str source_addr:
            Address of message originator.
            This is used for verification purposes, and must match that
            supplied in the original request PDU (e.g. submit_sm).
        """
        sequence_number = yield self.sequence_generator.next()
        pdu = QuerySM(
            sequence_number=sequence_number,
            message_id=message_id,
            source_addr=source_addr,
            source_addr_npi=source_addr_npi,
            source_addr_ton=source_addr_ton)
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
