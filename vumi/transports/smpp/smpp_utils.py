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
    sm_pdu.add_optional_parameter('ussd_service_op', '02')
    sm_pdu.add_optional_parameter('its_session_info', session_info)
    return sm_pdu
