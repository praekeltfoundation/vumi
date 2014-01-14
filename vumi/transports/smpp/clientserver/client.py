# -*- test-case-name: vumi.transports.smpp.clientserver.tests.test_client -*-

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

from vumi import log
from vumi.transports.smpp.smpp_utils import update_ussd_pdu
from vumi.transports.smpp.sequence import RedisSequence


GSM_MAX_SMS_BYTES = 140


class EsmeTransceiver(Protocol):
    BIND_PDU = BindTransceiver
    CONNECTED_STATE = 'BOUND_TRX'

    callLater = reactor.callLater

    def __init__(self, vumi_transport):
        self.vumi_transport = vumi_transport
        self.config = self.vumi_transport.get_static_config()
        self.bind_params = self.vumi_transport.get_smpp_bind_params()
        self.esme_callbacks = self.vumi_transport.esme_callbacks
        self.state = 'CLOSED'
        log.msg('STATE: %s' % (self.state,))
        self.smpp_bind_timeout = self.config.smpp_bind_timeout
        self.smpp_enquire_link_interval = \
                self.config.smpp_enquire_link_interval
        self.datastream = ''
        self.redis = self.vumi_transport.redis
        self.sequence_generator = RedisSequence(self.redis)
        self._lose_conn = None

        self.dr_processor = self.config.delivery_report_processor(
            self.vumi_transport,
            self.config.delivery_report_processor_config)
        self.sm_processor = self.config.short_message_processor(
            self.vumi_transport,
            self.config.short_message_processor_config)

        # The PDU queue ensures that PDUs are processed in the order
        # they arrive. `self._process_pdu_queue()` loops forever
        # pulling PDUs off the queue and handling each before grabbing
        # the next.
        self._pdu_queue = DeferredQueue()
        self._process_pdu_queue()  # intentionally throw away deferred

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
        seq = yield self.sequence_generator.next()
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

    @inlineCallbacks
    def handle_deliver_sm(self, pdu):
        if self.state not in ['BOUND_RX', 'BOUND_TRX']:
            log.err('WARNING: Received deliver_sm in wrong state: %s' % (
                self.state))
            return

        if pdu['header']['command_status'] != 'ESME_ROK':
            return

        sequence_number = pdu['header']['sequence_number']
        # These operate before the PDUs ``short_message`` or
        # ``message_payload`` fields have been string decoded.
        # NOTE: order is important!
        pdu_handler_chain = [
            self.dr_processor.handle_delivery_report_pdu,
            self.sm_processor.handle_multipart_pdu,
            self.sm_processor.handle_ussd_pdu,
        ]
        for handler in pdu_handler_chain:
            handled = yield handler(pdu)
            if handled:
                yield self.send_pdu(DeliverSMResp(
                    sequence_number, command_status='ESME_ROK',
                    **self.bind_params))
                return

        content_parts = self.sm_processor.decode_pdus([pdu])
        if not all([isinstance(part, unicode) for part in content_parts]):
            log.msg('Not all parts of the PDU were able to be decoded.',
                    parts=content_parts)
            yield self.send_pdu(DeliverSMResp(
                sequence_number, command_status='ESME_RDELIVERYFAILURE',
                **self.bind_params))
            return

        content = u''.join(content_parts)
        was_cdr = yield self.dr_processor.handle_delivery_report_content(
            content)
        if was_cdr:
            yield self.send_pdu(DeliverSMResp(
                sequence_number, command_status='ESME_ROK',
                **self.bind_params))
            return

        handled = yield self.sm_processor.handle_short_message_pdu(pdu)
        command_status = 'ESME_ROK' if handled else 'ESME_RDELIVERYFAILURE'
        yield self.send_pdu(DeliverSMResp(
            sequence_number, command_status=command_status,
            **self.bind_params))

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
            sequence_number = yield self.sequence_generator.next()
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

    def __init__(self, transport):
        config = transport.get_static_config()
        self.esme = None
        self.transport = transport
        self.initialDelay = config.initial_reconnect_delay
        self.maxDelay = max(45, self.initialDelay)

    def startedConnecting(self, connector):
        log.msg('Started to connect.')

    def buildProtocol(self, addr):
        log.msg('Connected')
        self.esme = EsmeTransceiver(self.transport)
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
        self.esme = EsmeTransmitter(self.transport)
        return self.esme


class EsmeReceiverFactory(EsmeTransceiverFactory):

    def buildProtocol(self, addr):
        log.msg('Connected')
        self.esme = EsmeReceiver(self.transport)
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
