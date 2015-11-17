from twisted.internet.defer import inlineCallbacks, returnValue, succeed
from twisted.internet.task import LoopingCall

from vumi.reconnecting_client import ReconnectingClientService
from vumi.transports.smpp.protocol import (
    EsmeProtocol, EsmeProtocolFactory, EsmeProtocolError)
from vumi.transports.smpp.sequence import RedisSequence


GSM_MAX_SMS_BYTES = 140
GSM_MAX_SMS_7BIT_CHARS = 160


class SmppService(ReconnectingClientService):

    throttle_statuses = ('ESME_RTHROTTLED', 'ESME_RMSGQFUL')

    def __init__(self, endpoint, bind_type, transport):
        self.transport = transport
        self.transport_name = transport.transport_name
        self.log = transport.log
        self.message_stash = self.transport.message_stash
        self.deliver_sm_processor = self.transport.deliver_sm_processor
        self.dr_processor = self.transport.dr_processor
        self.sequence_generator = RedisSequence(transport.redis)

        # Throttling setup.
        self.throttled = False
        self._throttled_pdus = []
        self._unthrottle_delayedCall = None

        self.tps_counter = 0
        self.tps_limit = self.get_config().mt_tps
        if self.tps_limit > 0:
            self.mt_tps_lc = LoopingCall(self.reset_mt_tps)
        else:
            self.mt_tps_lc = None

        # Connection setup.
        factory = EsmeProtocolFactory(self, bind_type)
        ReconnectingClientService.__init__(self, endpoint, factory)

    def get_protocol(self):
        return self._protocol

    def get_bind_state(self):
        if self._protocol is None:
            return EsmeProtocol.CLOSED_STATE
        return self._protocol.state

    def is_bound(self):
        if self._protocol is not None:
            return self._protocol.is_bound()
        return False

    def startService(self):
        if self.mt_tps_lc is not None:
            self.mt_tps_lc.clock = self.clock
            self.mt_tps_lc.start(1, now=True)
        return ReconnectingClientService.startService(self)

    def stopService(self):
        if self.mt_tps_lc and self.mt_tps_lc.running:
            self.mt_tps_lc.stop()
        d = succeed(None)
        if self._protocol is not None:
            d.addCallback(lambda _: self._protocol.disconnect())
        d.addCallback(lambda _: ReconnectingClientService.stopService(self))
        return d

    def get_config(self):
        return self.transport.get_static_config()

    @inlineCallbacks
    def reset_mt_tps(self):
        if self.throttled and self.need_mt_throttling():
            if not self.is_bound():
                # We don't have a bound SMPP connection, so try again later.
                self.log.msg(
                    "Can't stop throttling while unbound, trying later.")
                return
            self.reset_mt_throttle_counter()
            yield self.stop_throttling()

    def reset_mt_throttle_counter(self):
        self.tps_counter = 0

    def incr_mt_throttle_counter(self):
        self.tps_counter += 1

    def need_mt_throttling(self):
        return self.tps_counter >= self.tps_limit

    def check_mt_throttling(self):
        if self.get_config().mt_tps > 0:
            self.incr_mt_throttle_counter()
            if self.need_mt_throttling():
                # We can't yield here, because we need the current message to
                # finish sending before it will return.
                self.start_throttling()

    def _append_throttle_retry(self, seq_no):
        if seq_no not in self._throttled_pdus:
            self._throttled_pdus.append(seq_no)

    def check_stop_throttling(self, delay=None):
        if self._unthrottle_delayedCall is not None:
            # We already have one of these scheduled.
            return
        if delay is None:
            delay = self.get_config().throttle_delay
        self._unthrottle_delayedCall = self.clock.callLater(
            delay, self._check_stop_throttling)

    def check_stop_throttling_cb(self, ignored_result, delay=None):
        self.check_stop_throttling(delay)

    @inlineCallbacks
    def _check_stop_throttling(self):
        """
        Check if we should stop throttling, and stop throttling if we should.

        At a high level, we try each throttled message in our list until all of
        them have been accepted by the SMSC, at which point we stop throttling.

        In more detail:

        We recursively process our list of throttled message_ids until either
        we have none left (at which point we stop throttling) or we find one we
        can successfully look up in our cache.

        When we find a message we can retry, we retry it and return. We remain
        throttled until the SMSC responds. If we're still throttled, the
        message_id gets appended to our list and another check is scheduled for
        later. If we're no longer throttled, this method gets called again
        immediately.

        When there are no more throttled message_ids in our list, we stop
        throttling.
        """
        self._unthrottle_delayedCall = None

        if not self.is_bound():
            # We don't have a bound SMPP connection, so try again later.
            self.log.msg("Can't check throttling while unbound, trying later.")
            self.check_stop_throttling()
            return

        if not self._throttled_pdus:
            # We have no throttled messages waiting, so stop throttling.
            self.log.msg("No more throttled messages to retry.")
            yield self.stop_throttling()
            return

        seq_no = self._throttled_pdus.pop(0)
        pdu_data = yield self.message_stash.get_cached_pdu(seq_no)
        yield self.retry_throttled_pdu(pdu_data, seq_no)

    @inlineCallbacks
    def retry_throttled_pdu(self, pdu_data, seq_no):
        if pdu_data is None:
            # We can't find this pdu, so log it and start again.
            self.log.warning(
                "Could not retrieve throttled pdu: %s" % (seq_no,))
            self.check_stop_throttling(0)
        else:
            # Try handle this message again and leave the rest to our
            # submit_sm_resp handlers.
            self.log.msg("Retrying throttled pdu for message: %s" % (
                pdu_data.vumi_message_id,))
            # This is a new PDU, so it needs a new sequence number.
            new_seq_no = yield self.sequence_generator.next()
            pdu_data.pdu.obj['header']['sequence_number'] = new_seq_no
            yield self._protocol.send_submit_sm(
                pdu_data.vumi_message_id, pdu_data.pdu)
            yield self.message_stash.delete_cached_pdu(seq_no)

    @inlineCallbacks
    def start_throttling(self):
        if self.throttled:
            return
        self.log.msg("Throttling outbound messages.")
        self.throttled = True
        yield self.transport.pause_connectors()
        yield self.transport.on_throttled()

    @inlineCallbacks
    def stop_throttling(self):
        if not self.throttled:
            return
        self.log.msg("No longer throttling outbound messages.")
        self.throttled = False
        self.transport.unpause_connectors()
        yield self.transport.on_throttled_end()

    @inlineCallbacks
    def on_smpp_bind(self):
        self.transport.unpause_connectors()
        yield self.transport.on_smpp_bind()

    @inlineCallbacks
    def on_smpp_binding(self):
        yield self.transport.on_smpp_binding()

    @inlineCallbacks
    def on_smpp_unbinding(self):
        yield self.transport.on_smpp_unbinding()

    @inlineCallbacks
    def on_smpp_bind_timeout(self):
        yield self.transport.on_smpp_bind_timeout()

    @inlineCallbacks
    def on_connection_lost(self, reason):
        yield self.transport.pause_connectors()
        yield self.transport.on_connection_lost(reason)

    def handle_submit_sm_resp(self, message_id, smpp_id, pdu_status, seq_no):
        if pdu_status in self.throttle_statuses:
            return self.handle_submit_sm_throttled(seq_no)
        func = self.transport.handle_submit_sm_failure
        if pdu_status == 'ESME_ROK':
            func = self.transport.handle_submit_sm_success
        ms = self.message_stash
        d = func(message_id, smpp_id, pdu_status)
        d.addCallback(lambda _: ms.delete_cached_pdu(seq_no))
        d.addCallback(lambda _: ms.delete_sequence_number_message_id(seq_no))
        return d.addCallback(self.check_stop_throttling_cb, 0)

    def handle_submit_sm_throttled(self, message_id):
        self._append_throttle_retry(message_id)
        d = self.start_throttling()
        return d.addCallback(self.check_stop_throttling_cb)

    def submit_sm(self, *args, **kw):
        """
        See :meth:`EsmeProtocol.submit_sm`.
        """
        protocol = self.get_protocol()
        if protocol is None:
            raise EsmeProtocolError('submit_sm called while not connected.')
        self.check_mt_throttling()
        return protocol.submit_sm(*args, **kw)

    def submit_sm_long(self, vumi_message_id, destination_addr, long_message,
                       **pdu_params):
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
            vumi_message_id, destination_addr, short_message='', sm_length=0,
            optional_parameters=optional_parameters, **pdu_params)

    def _fits_in_one_message(self, message):
        if len(message) <= GSM_MAX_SMS_BYTES:
            return True

        # NOTE: We already have byte strings here, so we assume that printable
        #       ASCII characters are all the same as single-width GSM 03.38
        #       characters.
        if len(message) <= GSM_MAX_SMS_7BIT_CHARS:
            # TODO: We need better character handling and counting stuff.
            return all(0x20 <= ord(ch) <= 0x7f for ch in message)

        return False

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
        if self._fits_in_one_message(message):
            return [message]

        payload_length = GSM_MAX_SMS_BYTES - 10
        split_msg = []
        while message:
            split_msg.append(message[:payload_length])
            message = message[payload_length:]
        return split_msg

    @inlineCallbacks
    def submit_csm_sar(self, vumi_message_id, destination_addr, **pdu_params):
        """
        Submit a concatenated SMS to the SMSC using the optional
        SAR parameter names in the various PDUS.

        :returns: List of sequence numbers (int) for each of the segments.
        :rtype: list
        """

        split_msg = self.csm_split_message(pdu_params.pop('short_message'))

        if len(split_msg) == 1:
            # There is only one part, so send it without SAR stuff.
            sequence_numbers = yield self.submit_sm(
                vumi_message_id, destination_addr, short_message=split_msg[0],
                **pdu_params)
            returnValue(sequence_numbers)

        optional_parameters = pdu_params.pop('optional_parameters', {}).copy()
        ref_num = yield self.sequence_generator.next()
        sequence_numbers = []
        yield self.message_stash.init_multipart_info(
            vumi_message_id, len(split_msg))
        for i, msg in enumerate(split_msg):
            pdu_params = pdu_params.copy()
            optional_parameters.update({
                # Reference number must be between 00 & FFFF
                'sar_msg_ref_num': (ref_num % 0xFFFF),
                'sar_total_segments': len(split_msg),
                'sar_segment_seqnum': i + 1,
            })
            sequence_number = yield self.submit_sm(
                vumi_message_id, destination_addr, short_message=msg,
                optional_parameters=optional_parameters, **pdu_params)
            sequence_numbers.extend(sequence_number)
        returnValue(sequence_numbers)

    @inlineCallbacks
    def submit_csm_udh(self, vumi_message_id, destination_addr, **pdu_params):
        """
        Submit a concatenated SMS to the SMSC using user data headers (UDH)
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

        if len(split_msg) == 1:
            # There is only one part, so send it without UDH stuff.
            sequence_numbers = yield self.submit_sm(
                vumi_message_id, destination_addr, short_message=split_msg[0],
                **pdu_params)
            returnValue(sequence_numbers)

        ref_num = yield self.sequence_generator.next()
        sequence_numbers = []
        yield self.message_stash.init_multipart_info(
            vumi_message_id, len(split_msg))
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
                # Reference number must be between 00 & FF
                chr(ref_num % 0xFF),
                chr(len(split_msg)),
                chr(i + 1),
            ])
            short_message = udh + msg
            sequence_number = yield self.submit_sm(
                vumi_message_id, destination_addr, short_message=short_message,
                **pdu_params)
            sequence_numbers.extend(sequence_number)
        returnValue(sequence_numbers)
