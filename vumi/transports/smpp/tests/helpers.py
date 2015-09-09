from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred

from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (
    BindTransceiver, BindTransceiverResp,
    BindTransmitter, BindTransmitterResp,
    BindReceiver, BindReceiverResp,
    EnquireLinkResp)
from vumi.transports.smpp.pdu_utils import seq_no, chop_pdu_stream


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
