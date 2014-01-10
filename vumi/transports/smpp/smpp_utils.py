from vumi import log


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


def decode_pdus(pdus, data_coding_overrides):
    content = []
    for pdu in pdus:
        pdu_params = pdu['body']['mandatory_parameters']
        pdu_opts = unpacked_pdu_opts(pdu)

        # We might have a `message_payload` optional field to worry about.
        message_payload = pdu_opts.get('message_payload', None)
        if message_payload is not None:
            short_message = message_payload.decode('hex')
        else:
            short_message = pdu_params['short_message']

        content.append(
            decode_message(short_message, pdu_params['data_coding'],
                           data_coding_overrides))
    return content


def decode_message(message, data_coding, data_coding_overrides):
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
    codecs.update(data_coding_overrides)
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
