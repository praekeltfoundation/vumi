# -*- test-case-name: vumi.transports.smpp.clientserver.tests.test_client -*-

import abc
import json
import uuid

from twisted.python import log
from twisted.internet import reactor
from twisted.internet.protocol import Protocol, ReconnectingClientFactory
from twisted.internet.task import LoopingCall

import binascii
from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (BindTransceiver,
                                BindTransmitter,
                                BindReceiver,
                                DeliverSMResp,
                                SubmitSM,
                                SubmitMulti,
                                EnquireLink,
                                EnquireLinkResp,
                                QuerySM,
                                )
from smpp.pdu_inspector import (MultipartMessage,
                                detect_multipart,
                                multipart_key,
                                )


class KeyValueBase(object):
    """
    This is an API sepecification to ensure that any Key Value store adheres
    to the portion of the redis API used by the SMPP client
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get(self, key):
        """Retrieve data from the store by key and return a string."""
        return

    @abc.abstractmethod
    def set(self, key, value):
        """Save string value to datastore under key."""
        return

    @abc.abstractmethod
    def delete(self, key):
        """Delete record for data stored under key."""
        return

    @abc.abstractmethod
    def incr(self, key):
        """Increment value stored under key and return an integer."""
        return


class KeyValueStore(object):
    """
    A minimal implementation of KeyValueBase
    It is specifically not a subclass of KeyValueBase
    so that the ABC register() method can be used in testing
    This can be used in testing, but should not be used in production
    """

    def __init__(self):
        self._data = {}

    def get(self, key):
        try:
            return str(self._data[key])
        except:
            return None

    def set(self, key, value):
        self._data[key] = str(value)

    def delete(self, key):
        value = self.get(key)
        if value:
            del self._data[key]
            return True
        else:
            return False

    def incr(self, key):
        old = self.get(key)
        if old is None:
            old = 0
        new = int(old) + 1
        self.set(key, new)
        return new

    # This method is not required by the KeyValueBase ABC
    def is_empty(self):
        if len(self._data) == 0:
            return True
        else:
            return False


class EsmeTransceiver(Protocol):

    callLater = reactor.callLater

    def __init__(self, config, kvs, esme_callbacks):
        self.config = config
        self.esme_callbacks = esme_callbacks
        self.defaults = config.to_dict()
        self.state = 'CLOSED'
        log.msg('STATE: %s' % (self.state))
        self.smpp_bind_timeout = self.config.smpp_bind_timeout
        self.smpp_enquire_link_interval = \
                self.config.smpp_enquire_link_interval
        self.datastream = ''
        self.r_server = kvs
        self.r_prefix = "%s@%s:%s" % (
                self.config.system_id,
                self.config.host,
                self.config.port)
        self.sequence_number_prefix = "vumi_smpp_last_sequence_number#%s" % (
                self.r_prefix)
        log.msg("r_prefix = %s" % self.r_prefix)
        # self.get_next_seq()
        self._lose_conn = None

    def set_seq(self, seq):
        """Set the sequence number to a specific value.

        NOTE: This should not be used outside of tests.
        """
        self.r_server.set(self.sequence_number_prefix, int(seq))

    def get_next_seq(self):
        seq = self.r_server.incr(self.sequence_number_prefix)
        # SMPP supports a max sequence_number of: FFFFFFFF = 4,294,967,295
        # so start recycling @ 4,000,000,000 just to keep the numbers round
        if seq > 4000000000:
            self.r_server.delete(self.sequence_number_prefix)
            return self.get_next_seq()
        else:
            return seq

    def pop_data(self):
        data = None
        if(len(self.datastream) >= 16):
            command_length = int(binascii.b2a_hex(self.datastream[0:4]), 16)
            if(len(self.datastream) >= command_length):
                data = self.datastream[0:command_length]
                self.datastream = self.datastream[command_length:]
        return data

    def handle_data(self, data):
        pdu = unpack_pdu(data)
        log.msg('INCOMING <<<< %s' % binascii.b2a_hex(data))
        log.msg('INCOMING <<<< %s' % pdu)
        command_id = pdu['header']['command_id']
        handler = getattr(self, 'handle_%s' % (command_id,),
            self._command_handler_not_found)
        handler(pdu)
        log.msg('STATE: %s' % (self.state,))

    def _command_handler_not_found(self, pdu):
        log.err('No command handler available for %s' % (pdu,))

    def connectionMade(self):
        self.state = 'OPEN'
        log.msg('STATE: %s' % (self.state))
        pdu = BindTransceiver(self.get_next_seq(), **self.defaults)
        log.msg(pdu.get_obj())
        self.send_pdu(pdu)
        self.schedule_lose_connection('BOUND_TRX')

    def schedule_lose_connection(self, expected_status):
        self._lose_conn = self.callLater(self.smpp_bind_timeout,
            self.lose_unbound_connection, expected_status)

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

    def dataReceived(self, data):
        self.datastream += data
        data = self.pop_data()
        while data != None:
            self.handle_data(data)
            data = self.pop_data()

    def send_pdu(self, pdu):
        data = pdu.get_bin()
        log.msg('OUTGOING >>>> %s' % unpack_pdu(data))
        self.transport.write(data)

    def start_enquire_link(self):
        self.lc_enquire = LoopingCall(self.enquire_link)
        self.lc_enquire.start(self.smpp_enquire_link_interval)
        self.cancel_drop_connection_call()
        self.esme_callbacks.connect(self)

    def stop_enquire_link(self):
        lc_enquire = getattr(self, 'lc_enquire', None)
        if lc_enquire and lc_enquire.running:
            lc_enquire.stop()
            log.msg('Stopped enquire link looping call')

    def cancel_drop_connection_call(self):
        if self._lose_conn is not None:
            self._lose_conn.cancel()
            self._lose_conn = None

    def handle_bind_transceiver_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            self.state = 'BOUND_TRX'
            self.start_enquire_link()
        log.msg('STATE: %s' % (self.state))

    def handle_submit_sm_resp(self, pdu):
        self.pop_unacked()
        message_id = pdu.get('body', {}).get(
                'mandatory_parameters', {}).get('message_id')
        self.esme_callbacks.submit_sm_resp(
                sequence_number=pdu['header']['sequence_number'],
                command_status=pdu['header']['command_status'],
                command_id=pdu['header']['command_id'],
                message_id=message_id)
        if pdu['header']['command_status'] == 'ESME_ROK':
            pass

    def handle_submit_multi_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            pass

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
        codec = {
            1: 'ascii',
            3: 'latin1',
            8: 'utf-16be',  # Actually UCS-2, but close enough.
            }.get(data_coding, None)
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

    def handle_deliver_sm(self, pdu):
        if self.state not in ['BOUND_RX', 'BOUND_TRX']:
            log.err('WARNING: Received deliver_sm in wrong state: %s' % (
                self.state))

        if pdu['header']['command_status'] == 'ESME_ROK':
            sequence_number = pdu['header']['sequence_number']
            message_id = str(uuid.uuid4())
            pdu_resp = DeliverSMResp(sequence_number,
                    **self.defaults)
            self.send_pdu(pdu_resp)
            pdu_params = pdu['body']['mandatory_parameters']
            delivery_report = self.config.delivery_report_re.search(
                    pdu_params['short_message'] or ''
                    )
            if delivery_report:
                self.esme_callbacks.delivery_report(
                        destination_addr=pdu_params['destination_addr'],
                        source_addr=pdu_params['source_addr'],
                        delivery_report=delivery_report.groupdict(),
                        )
            elif detect_multipart(pdu):
                redis_key = "%s#multi_%s" % (
                        self.r_prefix, multipart_key(detect_multipart(pdu)))
                log.msg("Redis multipart key: %s" % (redis_key))
                value = json.loads(self.r_server.get(redis_key) or 'null')
                log.msg("Retrieved value: %s" % (repr(value)))
                multi = MultipartMessage(value)
                multi.add_pdu(pdu)
                completed = multi.get_completed()
                if completed:
                    self.r_server.delete(redis_key)
                    log.msg("Reassembled Message: %s" % (completed['message']))
                    # and we can finally pass the whole message on
                    self.esme_callbacks.deliver_sm(
                            destination_addr=completed['to_msisdn'],
                            source_addr=completed['from_msisdn'],
                            short_message=completed['message'],
                            message_id=message_id,
                            )
                else:
                    self.r_server.set(redis_key, json.dumps(multi.get_array()))
            else:
                decoded_msg = self._decode_message(pdu_params['short_message'],
                                                   pdu_params['data_coding'])
                self.esme_callbacks.deliver_sm(
                        destination_addr=pdu_params['destination_addr'],
                        source_addr=pdu_params['source_addr'],
                        short_message=decoded_msg,
                        message_id=message_id,
                        )

    def handle_enquire_link(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            sequence_number = pdu['header']['sequence_number']
            pdu_resp = EnquireLinkResp(sequence_number)
            self.send_pdu(pdu_resp)

    def handle_enquire_link_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            pass

    def get_unacked_count(self):
        return int(self.r_server.llen("%s#unacked" % self.r_prefix))

    def push_unacked(self, sequence_number=-1):
        self.r_server.lpush("%s#unacked" % self.r_prefix, sequence_number)
        log.msg("%s#unacked pushed to: %s" % (
                self.r_prefix, self.get_unacked_count()))

    def pop_unacked(self):
        self.r_server.lpop("%s#unacked" % self.r_prefix)
        log.msg("%s#unacked popped to: %s" % (
                self.r_prefix, self.get_unacked_count()))

    def submit_sm(self, **kwargs):
        if self.state not in ['BOUND_TX', 'BOUND_TRX']:
            log.err(('WARNING: submit_sm in wrong state: %s, '
                            'dropping message: %s' % (self.state, kwargs)))
            return 0
        else:
            sequence_number = self.get_next_seq()
            pdu = SubmitSM(sequence_number, **dict(self.defaults, **kwargs))
            self.send_pdu(pdu)
            self.push_unacked(sequence_number)
            return sequence_number

    def submit_multi(self, dest_address=[], **kwargs):
        if self.state in ['BOUND_TX', 'BOUND_TRX']:
            sequence_number = self.get_next_seq()
            pdu = SubmitMulti(sequence_number, **dict(self.defaults, **kwargs))
            for item in dest_address:
                if isinstance(item, str):
                    # assume strings are addresses not lists
                    pdu.addDestinationAddress(
                            item,
                            dest_addr_ton=self.defaults['dest_addr_ton'],
                            dest_addr_npi=self.defaults['dest_addr_npi'],
                            )
                elif isinstance(item, dict):
                    if item.get('dest_flag') == 1:
                        pdu.addDestinationAddress(
                                item.get('destination_addr', ''),
                                dest_addr_ton=item.get('dest_addr_ton',
                                    self.defaults['dest_addr_ton']),
                                dest_addr_npi=item.get('dest_addr_npi',
                                    self.defaults['dest_addr_npi']),
                                )
                    elif item.get('dest_flag') == 2:
                        pdu.addDistributionList(item.get('dl_name'))
            self.send_pdu(pdu)
            return sequence_number
        return 0

    def enquire_link(self, **kwargs):
        if self.state in ['BOUND_TX', 'BOUND_RX', 'BOUND_TRX']:
            sequence_number = self.get_next_seq()
            pdu = EnquireLink(sequence_number, **dict(self.defaults, **kwargs))
            self.send_pdu(pdu)
            return sequence_number
        return 0

    def query_sm(self, message_id, source_addr, **kwargs):
        if self.state in ['BOUND_TX', 'BOUND_TRX']:
            sequence_number = self.get_next_seq()
            pdu = QuerySM(sequence_number,
                    message_id=message_id,
                    source_addr=source_addr,
                    **dict(self.defaults, **kwargs))
            self.send_pdu(pdu)
            return sequence_number
        return 0


class EsmeTransmitter(EsmeTransceiver):

    def connectionMade(self):
        self.state = 'OPEN'
        log.msg('STATE: %s' % (self.state))
        pdu = BindTransmitter(self.get_next_seq(), **self.defaults)
        log.msg(pdu.get_obj())
        self.send_pdu(pdu)
        self.schedule_lose_connection('BOUND_TX')

    def handle_bind_transmitter_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            self.state = 'BOUND_TX'
            self.start_enquire_link()
        log.msg('STATE: %s' % (self.state))


class EsmeReceiver(EsmeTransceiver):

    def connectionMade(self):
        self.state = 'OPEN'
        log.msg('STATE: %s' % (self.state))
        pdu = BindReceiver(self.get_next_seq(), **self.defaults)
        log.msg(pdu.get_obj())
        self.send_pdu(pdu)
        self.schedule_lose_connection('BOUND_RX')

    def handle_bind_receiver_resp(self, pdu):
        if pdu['header']['command_status'] == 'ESME_ROK':
            self.state = 'BOUND_RX'
            self.start_enquire_link()
        log.msg('STATE: %s' % (self.state))


class EsmeTransceiverFactory(ReconnectingClientFactory):

    def __init__(self, config, kvs, esme_callbacks):
        self.config = config
        self.kvs = kvs
        self.esme = None
        self.esme_callbacks = esme_callbacks
        self.initialDelay = self.config.initial_reconnect_delay
        self.maxDelay = max(45, self.initialDelay)

    def startedConnecting(self, connector):
        log.msg('Started to connect.')

    def buildProtocol(self, addr):
        log.msg('Connected')
        self.esme = EsmeTransceiver(self.config, self.kvs, self.esme_callbacks)
        self.resetDelay()
        return self.esme

    def clientConnectionLost(self, connector, reason):
        log.msg('Lost connection.  Reason:', reason)
        self.esme_callbacks.disconnect()
        ReconnectingClientFactory.clientConnectionLost(
                self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        log.msg('Connection failed. Reason:', reason)
        ReconnectingClientFactory.clientConnectionFailed(
                self, connector, reason)


class EsmeTransmitterFactory(EsmeTransceiverFactory):

    def buildProtocol(self, addr):
        log.msg('Connected')
        self.esme = EsmeTransmitter(self.config, self.kvs, self.esme_callbacks)
        self.resetDelay()
        return self.esme


class EsmeReceiverFactory(EsmeTransceiverFactory):

    def buildProtocol(self, addr):
        log.msg('Connected')
        self.esme = EsmeReceiver(self.config, self.kvs, self.esme_callbacks)
        self.resetDelay()
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
    def __init__(self, client_config, kvs, esme_callbacks):
        self.config = client_config
        self.kvs = kvs
        self.esme_callbacks = esme_callbacks

    def bindTransciever(self):
        self.factory = EsmeTransceiverFactory(self.config, self.kvs,
                                              self.esme_callbacks)
