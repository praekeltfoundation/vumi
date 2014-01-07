# -*- test-case-name: vumi.transports.smpp.clientserver.tests.test_new_client -*-

from functools import wraps

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet.task import LoopingCall
from twisted.internet.defer import (
    inlineCallbacks, returnValue, maybeDeferred)

import binascii
from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (
    BindTransceiver, EnquireLink, UnbindResp)

from vumi import log


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


class EsmeProtocolError(Exception):
    pass


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

        log.warning('Dropping link due to binding delay. Current state: %s' % (
            self.state))
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
        if len(self.buffer) < 16:
            return

        bytes = binascii.b2a_hex(self.buffer[0:4])
        cmd_length = int(bytes, 16)
        if len(self.buffer) < cmd_length:
            return

        data, self.buffer = (self.buffer[0:cmd_length],
                             self.buffer[cmd_length:])
        return data

    def onPdu(self, pdu):
        command_id = pdu['header']['command_id']
        handler = getattr(self, 'handle_%s' % (command_id,),
                          self.onUnsupportedCommandId)
        return maybeDeferred(handler, pdu)

    def onUnsupportedCommandId(self, pdu):
        """
        Called when an SMPP PDU is received for which no handler function has
        been defined.
        """
        command_id = pdu['header']['command_id']
        log.warning('Received unsupported SMPP command_id: %r' % (command_id,))

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
        sequence_number = pdu['header']['sequence_number']
        message_id = pdu['body']['mandatory_parameters']['message_id']
        return self.onSubmitSmResp(sequence_number, message_id)


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
