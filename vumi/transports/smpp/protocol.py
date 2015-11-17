# -*- test-case-name: vumi.transports.smpp.tests.test_protocol -*-

from functools import wraps

from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet.task import LoopingCall
from twisted.internet.defer import (
    inlineCallbacks, returnValue, maybeDeferred, DeferredQueue, succeed)

from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (
    BindTransceiver, BindReceiver, BindTransmitter,
    UnbindResp, Unbind,
    DeliverSMResp,
    EnquireLink, EnquireLinkResp,
    SubmitSM, QuerySM)

from vumi.transports.smpp.pdu_utils import (
    pdu_ok, seq_no, command_status, command_id, message_id, chop_pdu_stream)


def require_bind(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.is_bound():
            raise EsmeProtocolError('%s called in unbound state.' % (func,))
        return func(self, *args, **kwargs)
    return wrapper


class EsmeProtocolError(Exception):
    pass


class EsmeProtocol(Protocol):

    noisy = True
    unbind_timeout = 2

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
    _BIND_PDU = {
        'TX': BindTransmitter,
        'RX': BindReceiver,
        'TRX': BindTransceiver,
    }

    def __init__(self, service, bind_type):
        """
        An SMPP 3.4 client suitable for use by a Vumi Transport.

        :param SmppService service:
            The SMPP service that is using this protocol to communicate with an
            SMSC.
        """
        self.service = service
        self.log = service.log
        self.bind_pdu = self._BIND_PDU[bind_type]
        self.clock = service.clock
        self.config = self.service.get_config()

        self.buffer = b''
        self.state = self.CLOSED_STATE

        self.deliver_sm_processor = self.service.deliver_sm_processor
        self.dr_processor = self.service.dr_processor
        self.sequence_generator = self.service.sequence_generator
        self.enquire_link_call = LoopingCall(self.enquire_link)
        self.drop_link_call = None
        self.idle_timeout = self.config.smpp_enquire_link_interval * 2
        self.disconnect_call = None
        self.unbind_resp_queue = DeferredQueue()

    def emit(self, msg):
        if self.noisy:
            self.log.debug(msg)

    @inlineCallbacks
    def connectionMade(self):
        self.state = self.OPEN_STATE
        self.log.msg('Connection made, current state: %s' % (self.state,))
        self.bind(
            system_id=self.config.system_id,
            password=self.config.password,
            system_type=self.config.system_type,
            interface_version=self.config.interface_version,
            address_range=self.config.address_range)
        yield self.service.on_smpp_binding()

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
            The password may be used by the SMSC to authenticate the ESME
            requesting to bind. If this is longer than 8 characters, it will be
            truncated and a warning will be logged.
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
        # Overly long passwords should be truncated.
        if len(password) > 8:
            password = password[:8]
            self.log.warning("Password longer than 8 characters, truncating.")

        sequence_number = yield self.sequence_generator.next()
        pdu = self.bind_pdu(
            sequence_number, system_id=system_id, password=password,
            system_type=system_type, interface_version=interface_version,
            addr_ton=addr_ton, addr_npi=addr_npi, address_range=address_range)
        self.send_pdu(pdu)
        self.drop_link_call = self.clock.callLater(
            self.config.smpp_bind_timeout, self.drop_link)

    @inlineCallbacks
    def drop_link(self):
        """
        Called if the SMPP connection is not bound within
        ``smpp_bind_timeout`` amount of seconds
        """
        if self.is_bound():
            return

        yield self.service.on_smpp_bind_timeout()

        yield self.disconnect(
            'Dropping link due to binding delay. Current state: %s' % (
                self.state))

    def disconnect(self, log_msg=None):
        """
        Forcibly close the connection, logging ``log_msg`` if provided.

        :param str log_msg:
            The entry to write to the log file.
        """
        if log_msg is not None:
            self.log.warning(log_msg)

        if not self.connected:
            return succeed(self.transport.loseConnection())

        d = self.unbind()
        d.addCallback(lambda _: self.unbind_resp_queue.get())
        d.addBoth(lambda *a: self.transport.loseConnection())

        # Give the SMSC a few seconds to respond with an unbind_resp
        self.clock.callLater(self.unbind_timeout, d.cancel)
        return d

    def connectionLost(self, reason):
        """
        :param Exception reason:
            The reason for the connection closed, generally a
            ``ConnectionDone``
        """
        self.state = self.CLOSED_STATE
        if self.enquire_link_call.running:
            self.enquire_link_call.stop()
        if self.drop_link_call is not None and self.drop_link_call.active():
            self.drop_link_call.cancel()
        if self.disconnect_call is not None and self.disconnect_call.active():
            self.disconnect_call.cancel()
        return self.service.on_connection_lost(reason)

    def is_bound(self):
        """
        Returns ``True`` if the connection is in one of the known
        values of ``self.BOUND_STATES``
        """
        return self.state in self.BOUND_STATES

    @require_bind
    @inlineCallbacks
    def enquire_link(self):
        """
        Ping the SMSC to see if they're still around.
        """
        sequence_number = yield self.sequence_generator.next()
        self.send_pdu(EnquireLink(sequence_number))
        returnValue(sequence_number)

    def send_pdu(self, pdu):
        """
        Send a PDU to the SMSC

        :param smpp.pdu_builder.PDU pdu:
            The PDU object to send.
        """
        self.emit('OUTGOING >> %r' % (pdu.get_obj(),))
        return self.transport.write(pdu.get_bin())

    def dataReceived(self, data):
        self.buffer += data
        data = self.handle_buffer()
        while data is not None:
            self.on_pdu(unpack_pdu(data))
            data = self.handle_buffer()

    def handle_buffer(self):
        pdu_found = chop_pdu_stream(self.buffer)
        if pdu_found is None:
            return

        data, self.buffer = pdu_found
        return data

    def on_pdu(self, pdu):
        """
        Handle a PDU that was received & decoded.

        :param dict pdu:
            The dict result one gets when calling ``smpp.pdu.unpack_pdu()``
            on the received PDU
        """
        self.emit('INCOMING << %r' % (pdu,))
        handler = getattr(self, 'handle_%s' % (command_id(pdu),),
                          self.on_unsupported_command_id)
        return maybeDeferred(handler, pdu)

    def on_unsupported_command_id(self, pdu):
        """
        Called when an SMPP PDU is received for which no handler function has
        been defined.

        :param dict pdu:
            The dict result one gets when calling ``smpp.pdu.unpack_pdu()``
            on the received PDU
        """
        self.log.warning(
            'Received unsupported SMPP command_id: %r' % (command_id(pdu),))

    def handle_bind_transceiver_resp(self, pdu):
        if not pdu_ok(pdu):
            self.log.warning('Unable to bind: %r' % (command_status(pdu),))
            self.transport.loseConnection()
            return

        self.state = self.BOUND_STATE_TRX
        return self.on_smpp_bind(seq_no(pdu))

    def handle_bind_transmitter_resp(self, pdu):
        if not pdu_ok(pdu):
            self.log.warning('Unable to bind: %r' % (command_status(pdu),))
            self.transport.loseConnection()
            return

        self.state = self.BOUND_STATE_TX
        return self.on_smpp_bind(seq_no(pdu))

    def handle_bind_receiver_resp(self, pdu):
        if not pdu_ok(pdu):
            self.log.warning('Unable to bind: %r' % (command_status(pdu),))
            self.transport.loseConnection()
            return

        self.state = self.BOUND_STATE_RX
        return self.on_smpp_bind(seq_no(pdu))

    def on_smpp_bind(self, sequence_number):
        """Called when the bind has been setup"""
        self.drop_link_call.cancel()
        self.disconnect_call = self.clock.callLater(
            self.idle_timeout, self.disconnect,
            'Disconnecting, no response from SMSC for longer '
            'than %s seconds' % (self.idle_timeout,))
        self.enquire_link_call.clock = self.clock
        self.enquire_link_call.start(self.config.smpp_enquire_link_interval)
        return self.service.on_smpp_bind()

    def handle_unbind(self, pdu):
        return self.send_pdu(UnbindResp(seq_no(pdu)))

    def handle_submit_sm_resp(self, pdu):
        return self.on_submit_sm_resp(
            seq_no(pdu), message_id(pdu), command_status(pdu))

    def on_submit_sm_resp(self, sequence_number, smpp_message_id,
                          command_status):
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
        message_stash = self.service.message_stash
        d = message_stash.get_sequence_number_message_id(sequence_number)

        # only set the remote message id if the submission was successful, we
        # use remote message ids for delivery reports, so we won't need remote
        # message ids for failed submissions
        if command_status == 'ESME_ROK':
            d.addCallback(
                message_stash.set_remote_message_id, smpp_message_id)

        d.addCallback(
            self._handle_submit_sm_resp_callback, smpp_message_id,
            command_status, sequence_number)
        return d

    def _handle_submit_sm_resp_callback(self, message_id, smpp_message_id,
                                        command_status, sequence_number):
        if message_id is None:
            # We have no message_id, so log a warning instead of calling the
            # callback.
            self.log.warning(
                "Failed to retrieve message id for deliver_sm_resp."
                " ack/nack from %s discarded." % self.service.transport_name)
        else:
            return self.service.handle_submit_sm_resp(
                message_id, smpp_message_id, command_status, sequence_number)

    @inlineCallbacks
    def handle_deliver_sm(self, pdu):
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
                self.send_pdu(DeliverSMResp(seq_no(pdu),
                              command_status='ESME_ROK'))
                return

        # At this point we either have a DR in the message payload
        # or have a normal SMS that needs to be decoded and handled.
        content_parts = self.deliver_sm_processor.decode_pdus([pdu])
        if not all([isinstance(part, unicode) for part in content_parts]):
            command_status = self.config.deliver_sm_decoding_error
            self.log.msg(
                'Not all parts of the PDU were able to be decoded. '
                'Responding with %s.' % (command_status,),
                parts=content_parts)
            self.send_pdu(DeliverSMResp(seq_no(pdu),
                          command_status=command_status))
            return

        content = u''.join(content_parts)
        was_cdr = yield self.dr_processor.handle_delivery_report_content(
            content)
        if was_cdr:
            self.send_pdu(DeliverSMResp(seq_no(pdu),
                          command_status='ESME_ROK'))
            return

        handled = yield self.deliver_sm_processor.handle_short_message_pdu(pdu)
        if handled:
            self.send_pdu(DeliverSMResp(seq_no(pdu),
                          command_status="ESME_ROK"))
            return

        command_status = self.config.deliver_sm_decoding_error
        self.log.warning(
            'Unable to process message. '
            'Responding with %s.' % (command_status,),
            content=content, pdu=pdu.get_obj())

        self.send_pdu(DeliverSMResp(seq_no(pdu),
                      command_status=command_status))

    def handle_enquire_link(self, pdu):
        return self.send_pdu(EnquireLinkResp(seq_no(pdu)))

    def handle_enquire_link_resp(self, pdu):
        self.disconnect_call.reset(self.idle_timeout)

    @require_bind
    @inlineCallbacks
    def submit_sm(self,
                  vumi_message_id,
                  destination_addr,
                  source_addr='',
                  esm_class=0,
                  protocol_id=0,
                  priority_flag=0,
                  schedule_delivery_time='',
                  validity_period='',
                  replace_if_present=0,
                  data_coding=0,
                  sm_default_msg_id=0,
                  sm_length=0,
                  short_message='',
                  optional_parameters=None,
                  **configured_parameters
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
        configured_param_values = {
            'service_type': self.config.service_type,
            'source_addr_ton': self.config.source_addr_ton,
            'source_addr_npi': self.config.source_addr_npi,
            'dest_addr_ton': self.config.dest_addr_ton,
            'dest_addr_npi': self.config.dest_addr_npi,
            'registered_delivery': self.config.registered_delivery,
        }
        configured_param_values.update(configured_parameters)
        sequence_number = yield self.sequence_generator.next()
        pdu = SubmitSM(
            sequence_number=sequence_number,
            source_addr=source_addr,
            destination_addr=destination_addr,
            esm_class=esm_class,
            protocol_id=protocol_id,
            priority_flag=priority_flag,
            schedule_delivery_time=schedule_delivery_time,
            validity_period=validity_period,
            replace_if_present=replace_if_present,
            data_coding=data_coding,
            sm_default_msg_id=sm_default_msg_id,
            sm_length=sm_length,
            short_message=short_message,
            **configured_param_values)

        if optional_parameters:
            for key, value in optional_parameters.items():
                pdu.add_optional_parameter(key, value)

        yield self.send_submit_sm(vumi_message_id, pdu)
        returnValue([sequence_number])

    @inlineCallbacks
    def send_submit_sm(self, vumi_message_id, pdu):
        yield self.service.message_stash.cache_pdu(vumi_message_id, pdu)
        yield self.service.message_stash.set_sequence_number_message_id(
            seq_no(pdu.obj), vumi_message_id)
        self.send_pdu(pdu)

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
        self.send_pdu(pdu)
        returnValue([sequence_number])

    @inlineCallbacks
    def unbind(self):
        sequence_number = yield self.sequence_generator.next()
        self.send_pdu(Unbind(sequence_number))
        yield self.service.on_smpp_unbinding()
        returnValue([sequence_number])

    def handle_unbind_resp(self, pdu):
        self.unbind_resp_queue.put(pdu)


class EsmeProtocolFactory(ClientFactory):

    protocol = EsmeProtocol

    def __init__(self, service, bind_type):
        self.service = service
        self.bind_type = bind_type

    def buildProtocol(self, addr):
        proto = self.protocol(self.service, self.bind_type)
        proto.factory = self
        return proto
