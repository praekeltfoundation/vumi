from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.defer import (
    inlineCallbacks, returnValue, Deferred, succeed)
from twisted.internet.error import ConnectionDone
from twisted.test import proto_helpers

from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (
    BindTransceiver, BindTransceiverResp,
    BindTransmitter, BindTransmitterResp,
    BindReceiver, BindReceiverResp,
    DeliverSM, EnquireLinkResp)
from vumi.transports.smpp.pdu_utils import seq_no, chop_pdu_stream


class DummyService(Service):

    def __init__(self, endpoint, factory):
        self.factory = factory
        self.protocol = None
        self.wait_on_protocol_deferreds = []

    def startService(self):
        self.protocol = self.factory.buildProtocol(('120.0.0.1', 0))
        while self.wait_on_protocol_deferreds:
            deferred = self.wait_on_protocol_deferreds.pop()
            deferred.callback(self.protocol)

    def stopService(self):
        if self.protocol and self.protocol.transport:
            self.protocol.transport.loseConnection()
            self.protocol.connectionLost(reason=ConnectionDone)
        self.protocol = None

    def get_protocol(self):
        if self.protocol is not None:
            return succeed(self.protocol)
        else:
            d = Deferred()
            self.wait_on_protocol_deferreds.append(d)
            return d

    def is_bound(self):
        if self.protocol is not None:
            return self.protocol.is_bound()
        return False


@inlineCallbacks
def bind_protocol(string_transport, protocol, clear=True, bind_pdu=None):
    """
    Reply to a waiting (or given) bind PDU.
    """
    if bind_pdu is None:
        [bind_pdu] = yield wait_for_pdus(string_transport, 1)
    resp_pdu_class = {
        BindTransceiver: BindTransceiverResp,
        BindReceiver: BindReceiverResp,
        BindTransmitter: BindTransmitterResp,
    }.get(protocol.bind_pdu)
    protocol.dataReceived(
        resp_pdu_class(seq_no(bind_pdu)).get_bin())
    [enquire_link] = yield wait_for_pdus(string_transport, 1)
    protocol.dataReceived(
        EnquireLinkResp(seq_no(enquire_link)).get_bin())
    if clear:
        string_transport.clear()
    returnValue(bind_pdu)


def wait_for_pdus(string_transport, count):
    """
    Wait for and return some PDUs.
    """
    d = Deferred()

    def cb(pdus):
        data_stream = string_transport.value()
        pdu_found = chop_pdu_stream(data_stream)
        if pdu_found is not None:
            pdu_data, remainder = pdu_found
            pdu = unpack_pdu(pdu_data)
            pdus.append(pdu)
            string_transport.clear()
            string_transport.write(remainder)

        if len(pdus) == count:
            d.callback(pdus)
        else:
            reactor.callLater(0, cb, pdus)

    cb([])

    return d


class SMPPHelper(object):
    def __init__(self, transport):
        assert isinstance(transport.service, DummyService)
        self.string_transport = proto_helpers.StringTransport()
        self.transport = transport
        self.protocol = transport.service.protocol

    def disconnect(self):
        """
        Stop the service.
        """
        self.transport.service.stopService()
        self.protocol = self.transport.service.protocol

    def connect(self):
        """
        Start the service and make a connection to the string transport.
        """
        self.transport.service.startService()
        self.protocol = self.transport.service.protocol
        self.protocol.makeConnection(self.string_transport)

    def bind(self, bind_pdu=None):
        """
        Reply to a waiting (or given) bind PDU. Will connect if necessary.
        """
        if self.protocol.state == self.protocol.CLOSED_STATE:
            self.protocol.makeConnection(self.string_transport)
        return bind_protocol(
            self.string_transport, self.protocol, bind_pdu=bind_pdu)

    def send_pdu(self, pdu):
        """put it on the wire and don't wait for a response"""
        self.protocol.dataReceived(pdu.get_bin())

    def handle_pdu(self, pdu):
        """short circuit the wire so we get a deferred so we know
        when it's been handled, also allows us to test PDUs that are invalid
        because we're skipping the encode/decode step."""
        return self.protocol.on_pdu(pdu.obj)

    def send_mo(self, sequence_number, short_message, data_coding=1, **kwargs):
        return self.send_pdu(
            DeliverSM(sequence_number, short_message=short_message,
                      data_coding=data_coding, **kwargs))

    def wait_for_pdus(self, count):
        return wait_for_pdus(self.string_transport, count)

    def no_pdus(self):
        return self.string_transport.value() == ''
