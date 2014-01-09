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
    return pdu['body']['mandatory_parameters']['message_id']


def short_message(pdu):
    return pdu['body']['mandatory_parameters']['short_message']


def pdu_tlv(pdu, tag):
    return unpacked_pdu_opts(pdu)[tag]
