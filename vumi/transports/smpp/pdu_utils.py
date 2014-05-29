import binascii

from vumi.transports.smpp.smpp_utils import unpacked_pdu_opts


def pdu_ok(pdu):
    return command_status(pdu) == 'ESME_ROK'


def seq_no(pdu):
    return pdu['header']['sequence_number']


def command_status(pdu):
    return pdu['header']['command_status']


def command_id(pdu):
    return pdu['header']['command_id']


def message_id(pdu):
    # If we have an unsuccessful response, we may not get a message_id.
    if 'body' not in pdu and not pdu_ok(pdu):
        return None
    return pdu['body']['mandatory_parameters']['message_id']


def short_message(pdu):
    return pdu['body']['mandatory_parameters']['short_message']


def pdu_tlv(pdu, tag):
    return unpacked_pdu_opts(pdu)[tag]


def chop_pdu_stream(data):
    if len(data) < 16:
        return

    bytes = binascii.b2a_hex(data[0:4])
    cmd_length = int(bytes, 16)
    if len(data) < cmd_length:
        return

    pdu, data = (data[0:cmd_length],
                 data[cmd_length:])
    return pdu, data
