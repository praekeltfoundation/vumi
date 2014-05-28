# -*- test-case-name: vumi.transports.smpp.deprecated.clientserver.tests.test_client -*-

import json
import uuid
from random import randint

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet.task import LoopingCall
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredQueue

import binascii
from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (
    BindTransceiver, BindTransmitter, BindReceiver, DeliverSMResp, SubmitSM,
    EnquireLink, EnquireLinkResp, QuerySM, UnbindResp)
from smpp.pdu_inspector import (
    MultipartMessage, detect_multipart, multipart_key)

from vumi import log


GSM_MAX_SMS_BYTES = 140


def unpacked_pdu_opts(unpacked_pdu):
    pdu_opts = {}
    for opt in unpacked_pdu['body'].get('optional_parameters', []):
        pdu_opts[opt['tag']] = opt['value']
    return pdu_opts


def detect_ussd(pdu_opts):
    # TODO: Push this back to python-smpp?
    return ('ussd_service_op' in pdu_opts)


def update_ussd_pdu(sm_pdu, continue_session, session_info=None):
    if session_info is None:
        session_info = '0000'
    session_info = "%04x" % (int(session_info, 16) + int(not continue_session))
    sm_pdu._PDU__add_optional_parameter('ussd_service_op', '02')
    sm_pdu._PDU__add_optional_parameter('its_session_info', session_info)
    return sm_pdu


class EsmeTransceiver(Protocol):
    BIND_PDU = BindTransceiver
    CONNECTED_STATE = 'BOUND_TRX'

    callLater = reactor.callLater

    def __init__(self, config, bind_params, redis, esme_callbacks):
        self.config = config
        self.bind_params = bind_params
        self.esme_callbacks = esme_callbacks
        self.state = 'CLOSED'
        log.msg('STATE: %s' % (self.state,))
        self.smpp_bind_timeout = self.config.smpp_bind_timeout
        self.smpp_enquire_link_interval = \
                self.config.smpp_enquire_link_interval
        self.datastream = ''
        self.redis = redis
        self._lose_conn = None
        # The PDU queue ensures that PDUs are processed in the order
        # they arrive. `self._process_pdu_queue()` loops forever
        # pulling PDUs off the queue and handling each before grabbing
        # the next.
        self._pdu_queue = DeferredQueue()
        self._process_pdu_queue()  # intentionally throw away deferred

    @inlineCallbacks
    def get_next_seq(self):
        """Get the next available SMPP sequence number.

        The valid range of sequence number is 0x00000001 to 0xFFFFFFFF.

        We start trying to wrap at 0xFFFF0000 so we can keep returning values
        (up to 0xFFFF of them) even while someone else is in the middle of
        resetting the counter.
        """
        seq = yield self.redis.incr('smpp_last_sequence_number')

        if seq >= 0xFFFF0000:
            # We're close to the upper limit, so try to reset. It doesn't
            # matter if we actually succeed or not, since we're going to return
            # `seq` anyway.
            yield self._reset_seq_counter()

        returnValue(seq)

    @inlineCallbacks
    def _reset_seq_counter(self):
        """Reset the sequence counter in a safe manner.

        NOTE: There is a potential race condition in this implementation. If we
        acquire the lock and it expires while we still think we hold it, it's
        possible for the sequence number to be reset by someone else between
        the final vlue check and the reset call. This seems like a very
        unlikely situation, so we'll leave it like that for now.

        A better solution is to replace this whole method with a lua script
        that we send to redis, but scripting support is still very new at the
        time of writing.
        """
        # SETNX can be used as a lock.
        locked = yield self.redis.setnx('smpp_last_sequence_number_wrap', 1)

        # If someone crashed in exactly the wrong place, the lock may be
        # held by someone else but have no expire time. A race condition
        # here may set the TTL multiple times, but that's fine.
        if (yield self.redis.ttl('smpp_last_sequence_number_wrap')) < 0:
            # The TTL only gets set if the lock exists and recently had no TTL.
            yield self.redis.expire('smpp_last_sequence_number_wrap', 10)

        if not locked:
            # We didn't actually get the lock, so our job is done.
            return

        if (yield self.redis.get('smpp_last_sequence_number')) < 0xFFFF0000:
            # Our stored sequence number is no longer outside the allowed
            # range, so someone else must have reset it before we got the lock.
            return

        # We reset the counter by deleting the key. The next INCR will recreate
        # it for us.
        yield self.redis.delete('smpp_last_sequence_number')

    def pop_data(self):
        data = None
        if(len(self.datastream) >= 16):
            command_length = int(binascii.b2a_hex(self.datastream[0:4]), 16)
            if(len(self.datastream) >= command_length):
                data = self.datastream[0:command_length]
                self.datastream = self.datastream[command_length:]
        return data

    @inlineCallbacks
    def handle_data(self, data):
        pdu = unpack_pdu(data)
        command_id = pdu['header']['command_id']
        if command_id not in ('enquire_link', 'enquire_link_resp'):
            log.debug('INCOMING <<<< %s' % binascii.b2a_hex(data))
            log.debug('INCOMING <<<< %s' % pdu)
        handler = getattr(self, 'handle_%s' % (command_id,),
                          self._command_handler_not_found)
        yield handler(pdu)

    @inlineCallbacks
    def _process_pdu_queue(self):
        data = yield self._pdu_queue.get()
        while data is not None:
            yield self.handle_data(data)
            data = yield self._pdu_queue.get()

    def _command_handler_not_found(self, pdu):
        log.err('No command handler available for %s' % (pdu,))

    @inlineCallbacks
    def connectionMade(self):
        self.state = 'OPEN'
        log.msg('STATE: %s' % (self.state))
        seq = yield self.get_next_seq()
        pdu = self.BIND_PDU(seq, **self.bind_params)
        log.msg(pdu.get_obj())
        self.send_pdu(pdu)
        self.schedule_lose_connection(self.CONNECTED_STATE)

    def schedule_lose_connection(self, expected_status):
        self._lose_conn = self.callLater(self.smpp_bind_timeout,
                                         self.lose_unbound_connection,
                                         expected_status)

    def lose_unbound_connection(self, required_state):
        if self.state != required_state:
            log.msg('Breaking connection due to binding delay, %s != %s\n' % (
                self.state, required_state))
            self._lose_conn = None
            self.transport.loseConnection()
        else:
            log.msg('Successful bind: %s, cancelling bind timeout' % (
                self.state))

    def connectionLost(self, *args, **kwargs):
        self.state = 'CLOSED'
        self.stop_enquire_link()
        self.cancel_drop_connection_call()
        log.msg('STATE: %s' % (self.state))
        self.esme_callbacks.disconnect()

    def dataReceived(self, data):
        self.datastream += data
        data = self.pop_data()
        while data is not None:
            self._pdu_queue.put(data)
            data = self.pop_data()

    def send_pdu(self, pdu):
        data = pdu.get_bin()
        unpacked = unpack_pdu(data)
        command_id = unpacked['header']['command_id']
        if command_id not in ('enquire_link', 'enquire_link_resp'):
            log.debug('OUTGOING >>>> %s' % unpacked)
        self.transport.write(data)

    @inlineCallbacks
    def start_enquire_link(self):
        self.lc_enquire = LoopingCall(self.enquire_link)
        self.lc_enquire.start(self.smpp_enquire_link_interval)
        self.cancel_drop_connection_call()
        yield self.esme_callbacks.connect(self)

    @inlineCallbacks
    def stop_enquire_link(self):
        lc_enquire = getattr(self, 'lc_enquire', None)
        if lc_enquire and lc_enquire.running:
            lc_enquire.stop()
            log.msg('Stopped enquire link looping call')
            yield lc_enquire.deferred

    def cancel_drop_connection_call(self):
        if self._lose_conn is not None:
            self._lose_conn.cancel()
            self._lose_conn = None

    @inlineCallbacks
    def handle_unbind(self, pdu):
        yield self.send_pdu(UnbindResp(
            sequence_number=pdu['header']['sequence_number']))
        self.transport.loseConnection()

    @inlineCallbacks
    def handle_bind_transceiver_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            self.state = 'BOUND_TRX'
            yield self.start_enquire_link()
        log.msg('STATE: %s' % (self.state))

    @inlineCallbacks
    def handle_submit_sm_resp(self, pdu):
        yield self.pop_unacked()
        message_id = pdu.get('body', {}).get(
                'mandatory_parameters', {}).get('message_id')
        yield self.esme_callbacks.submit_sm_resp(
                sequence_number=pdu['header']['sequence_number'],
                command_status=pdu['header']['command_status'],
                command_id=pdu['header']['command_id'],
                message_id=message_id)

    def _decode_message(self, message, data_coding):
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
        codecs.update(self.config.data_coding_overrides)
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
    def handle_deliver_sm(self, pdu):
        if self.state not in ['BOUND_RX', 'BOUND_TRX']:
            log.err('WARNING: Received deliver_sm in wrong state: %s' % (
                self.state))
            return

        if pdu['header']['command_status'] != 'ESME_ROK':
            return

        # TODO: Only ACK messages once we've processed them?
        sequence_number = pdu['header']['sequence_number']
        pdu_resp = DeliverSMResp(sequence_number, **self.bind_params)
        yield self.send_pdu(pdu_resp)

        pdu_params = pdu['body']['mandatory_parameters']
        pdu_opts = unpacked_pdu_opts(pdu)

        # This might be a delivery receipt with PDU parameters. If we get a
        # delivery receipt without these parameters we'll try a regex match
        # later once we've decoded the message properly.
        receipted_message_id = pdu_opts.get('receipted_message_id', None)
        message_state = pdu_opts.get('message_state', None)
        if receipted_message_id is not None and message_state is not None:
            yield self.esme_callbacks.delivery_report(
                message_id=receipted_message_id,
                message_state={
                    1: 'ENROUTE',
                    2: 'DELIVERED',
                    3: 'EXPIRED',
                    4: 'DELETED',
                    5: 'UNDELIVERABLE',
                    6: 'ACCEPTED',
                    7: 'UNKNOWN',
                    8: 'REJECTED',
                }.get(message_state, 'UNKNOWN'),
            )

        # We might have a `message_payload` optional field to worry about.
        message_payload = pdu_opts.get('message_payload', None)
        if message_payload is not None:
            pdu_params['short_message'] = message_payload.decode('hex')

        if detect_ussd(pdu_opts):
            # We have a USSD message.
            yield self._handle_deliver_sm_ussd(pdu, pdu_params, pdu_opts)
        elif detect_multipart(pdu):
            # We have a multipart SMS.
            yield self._handle_deliver_sm_multipart(pdu, pdu_params)
        else:
            # We have a standard SMS.
            yield self._handle_deliver_sm_sms(pdu_params)

    def _deliver_sm(self, source_addr, destination_addr, short_message, **kw):
        delivery_report = self.config.delivery_report_regex.search(
            short_message or '')
        if delivery_report:
            # We have a delivery report.
            fields = delivery_report.groupdict()
            return self.esme_callbacks.delivery_report(
                message_id=fields['id'],
                message_state=fields['stat'])

        message_id = str(uuid.uuid4())
        return self.esme_callbacks.deliver_sm(
            source_addr=source_addr,
            destination_addr=destination_addr,
            short_message=short_message,
            message_id=message_id,
            **kw)

    def _handle_deliver_sm_ussd(self, pdu, pdu_params, pdu_opts):
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

        decoded_msg = self._decode_message(pdu_params['short_message'],
                                           pdu_params['data_coding'])
        return self._deliver_sm(
            source_addr=pdu_params['source_addr'],
            destination_addr=pdu_params['destination_addr'],
            short_message=decoded_msg,
            message_type='ussd',
            session_event=session_event,
            session_info=session_info)

    def _handle_deliver_sm_sms(self, pdu_params):
        decoded_msg = self._decode_message(pdu_params['short_message'],
                                           pdu_params['data_coding'])
        return self._deliver_sm(
            source_addr=pdu_params['source_addr'],
            destination_addr=pdu_params['destination_addr'],
            short_message=decoded_msg)

    @inlineCallbacks
    def load_multipart_message(self, redis_key):
        value = yield self.redis.get(redis_key)
        value = json.loads(value) if value else {}
        log.debug("Retrieved value: %s" % (repr(value)))
        returnValue(MultipartMessage(self._unhex_from_redis(value)))

    def save_multipart_message(self, redis_key, multipart_message):
        data_dict = self._hex_for_redis(multipart_message.get_array())
        return self.redis.set(redis_key, json.dumps(data_dict))

    def _hex_for_redis(self, data_dict):
        for index, part in data_dict.items():
            part['part_message'] = part['part_message'].encode('hex')
        return data_dict

    def _unhex_from_redis(self, data_dict):
        for index, part in data_dict.items():
            part['part_message'] = part['part_message'].decode('hex')
        return data_dict

    @inlineCallbacks
    def _handle_deliver_sm_multipart(self, pdu, pdu_params):
        redis_key = "multi_%s" % (multipart_key(detect_multipart(pdu)),)
        log.debug("Redis multipart key: %s" % (redis_key))
        multi = yield self.load_multipart_message(redis_key)
        multi.add_pdu(pdu)
        completed = multi.get_completed()
        if completed:
            yield self.redis.delete(redis_key)
            log.msg("Reassembled Message: %s" % (completed['message']))
            # We assume that all parts have the same data_coding here, because
            # otherwise there's nothing sensible we can do.
            decoded_msg = self._decode_message(completed['message'],
                                               pdu_params['data_coding'])
            # and we can finally pass the whole message on
            yield self._deliver_sm(
                source_addr=completed['from_msisdn'],
                destination_addr=completed['to_msisdn'],
                short_message=decoded_msg)
        else:
            yield self.save_multipart_message(redis_key, multi)

    def handle_enquire_link(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            log.msg("enquire_link OK")
            sequence_number = pdu['header']['sequence_number']
            pdu_resp = EnquireLinkResp(sequence_number)
            self.send_pdu(pdu_resp)
        else:
            log.msg("enquire_link NOT OK: %r" % (pdu,))

    def handle_enquire_link_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            log.msg("enquire_link_resp OK")
        else:
            log.msg("enquire_link_resp NOT OK: %r" % (pdu,))

    def get_unacked_count(self):
        return self.redis.llen("unacked").addCallback(int)

    @inlineCallbacks
    def push_unacked(self, sequence_number=-1):
        yield self.redis.lpush("unacked", sequence_number)
        log.msg("unacked pushed to: %s" % ((yield self.get_unacked_count())))

    @inlineCallbacks
    def pop_unacked(self):
        yield self.redis.lpop("unacked")
        log.msg("unacked popped to: %s" % ((yield self.get_unacked_count())))

    @inlineCallbacks
    def submit_sm(self, **kwargs):
        if self.state not in ['BOUND_TX', 'BOUND_TRX']:
            log.err(('WARNING: submit_sm in wrong state: %s, '
                     'dropping message: %s' % (self.state, kwargs)))
            returnValue(0)

        pdu_params = self.bind_params.copy()
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

        self.send_pdu(pdu)
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

    @inlineCallbacks
    def enquire_link(self, **kwargs):
        if self.state in ['BOUND_TX', 'BOUND_RX', 'BOUND_TRX']:
            sequence_number = yield self.get_next_seq()
            pdu = EnquireLink(
                sequence_number, **dict(self.bind_params, **kwargs))
            self.send_pdu(pdu)
            returnValue(sequence_number)
        returnValue(0)

    @inlineCallbacks
    def query_sm(self, message_id, source_addr, **kwargs):
        if self.state in ['BOUND_TX', 'BOUND_TRX']:
            sequence_number = yield self.get_next_seq()
            pdu = QuerySM(sequence_number,
                    message_id=message_id,
                    source_addr=source_addr,
                    **dict(self.bind_params, **kwargs))
            self.send_pdu(pdu)
            returnValue(sequence_number)
        returnValue(0)


class EsmeTransmitter(EsmeTransceiver):
    BIND_PDU = BindTransmitter
    CONNECTED_STATE = 'BOUND_TX'

    @inlineCallbacks
    def handle_bind_transmitter_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            self.state = 'BOUND_TX'
            yield self.start_enquire_link()
        log.msg('STATE: %s' % (self.state))


class EsmeReceiver(EsmeTransceiver):
    BIND_PDU = BindReceiver
    CONNECTED_STATE = 'BOUND_RX'

    @inlineCallbacks
    def handle_bind_receiver_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            self.state = 'BOUND_RX'
            yield self.start_enquire_link()
        log.msg('STATE: %s' % (self.state))


class EsmeTransceiverFactory(ClientFactory):

    def __init__(self, config, bind_params, redis, esme_callbacks):
        self.config = config
        self.bind_params = bind_params
        self.redis = redis
        self.esme = None
        self.esme_callbacks = esme_callbacks
        self.initialDelay = self.config.initial_reconnect_delay
        self.maxDelay = max(45, self.initialDelay)

    def startedConnecting(self, connector):
        log.msg('Started to connect.')

    def buildProtocol(self, addr):
        log.msg('Connected')
        self.esme = EsmeTransceiver(
            self.config, self.bind_params, self.redis, self.esme_callbacks)
        return self.esme

    @inlineCallbacks
    def clientConnectionLost(self, connector, reason):
        log.msg('Lost connection.  Reason:', reason)
        ClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        log.err(reason, 'Connection failed')
        ClientFactory.clientConnectionFailed(self, connector, reason)


class EsmeTransmitterFactory(EsmeTransceiverFactory):

    def buildProtocol(self, addr):
        log.msg('Connected')
        self.esme = EsmeTransmitter(
            self.config, self.bind_params, self.redis, self.esme_callbacks)
        return self.esme


class EsmeReceiverFactory(EsmeTransceiverFactory):

    def buildProtocol(self, addr):
        log.msg('Connected')
        self.esme = EsmeReceiver(
            self.config, self.bind_params, self.redis, self.esme_callbacks)
        return self.esme


class EsmeCallbacks(object):
    """Callbacks for ESME factory and protocol."""

    def __init__(self, connect=None, disconnect=None, submit_sm_resp=None,
                 delivery_report=None, deliver_sm=None):
        self.connect = connect or self.fallback
        self.disconnect = disconnect or self.fallback
        self.submit_sm_resp = submit_sm_resp or self.fallback
        self.delivery_report = delivery_report or self.fallback
        self.deliver_sm = deliver_sm or self.fallback

    def fallback(self, *args, **kwargs):
        pass


class ESME(object):
    """
    The top 'Client' object
    Potentially should be able to bind as:
        * Transceiver
        * Transmitter and/or Receiver
    but currently only Transceiver is implemented
    """
    def __init__(self, config, bind_params, redis, esme_callbacks):
        self.config = config
        self.bind_params = bind_params
        self.redis = redis
        self.esme_callbacks = esme_callbacks

    def bindTransciever(self):
        self.factory = EsmeTransceiverFactory(
            self.config, self.bind_params,
            self.redis, self.esme_callbacks)
