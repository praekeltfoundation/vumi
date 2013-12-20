def unpacked_pdu_opts(unpacked_pdu):
    pdu_opts = {}
    for opt in unpacked_pdu['body'].get('optional_parameters', []):
        pdu_opts[opt['tag']] = opt['value']
    return pdu_opts


def detect_ussd(pdu_opts):
    # TODO: Push this back to python-smpp?
    return ('ussd_service_op' in pdu_opts)

