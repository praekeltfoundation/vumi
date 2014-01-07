from vumi.config import ConfigText, ConfigInt, ConfigBool, Config


class EsmeConfig(Config):

    system_id = ConfigText(
        'User id used to connect to the SMPP server.', required=True,
        static=True)
    password = ConfigText(
        'Password for the system id.', required=True, static=True)
    system_type = ConfigText(
        "Additional system metadata that is passed through to the SMPP "
        "server on connect.", default="", static=True)
    interface_version = ConfigText(
        "SMPP protocol version. Default is '34' (i.e. version 3.4).",
        default="34", static=True)
    service_type = ConfigText(
        'The SMPP service type', default="", static=True)
    dest_addr_ton = ConfigInt(
        'Destination TON (type of number)', default=0, static=True)
    dest_addr_npi = ConfigInt(
        'Destination NPI (number plan identifier). '
        'Default 1 (ISDN/E.164/E.163)', default=1, static=True)
    source_addr_ton = ConfigInt(
        'Source TON (type of number)', default=0, static=True)
    source_addr_npi = ConfigInt(
        'Source NPI (number plan identifier)', default=0, static=True)
    registered_delivery = ConfigBool(
        'Whether or not to request delivery reports', default=True,
        static=True)
    smpp_bind_timeout = ConfigInt(
        'How long to wait for a succesful bind', default=30, static=True)
    smpp_enquire_link_interval = ConfigInt(
        "Number of seconds to delay before reconnecting to the server after "
        "being disconnected. Default is 5s. Some WASPs, e.g. Clickatell "
        "require a 30s delay before reconnecting. In these cases a 45s "
        "initial_reconnect_delay is recommended.", default=55, static=True)
    initial_reconnect_delay = ConfigInt(
        'How long to wait between reconnecting attempts', default=5,
        static=True)
    third_party_id_expiry = ConfigInt(
        'How long (seconds) to keep 3rd party message IDs around to allow for '
        'matching submit_sm_resp and delivery report messages. Defaults to '
        '1 week',
        default=(60 * 60 * 24 * 7), static=True)
    submit_sm_expiry = ConfigInt(
        'How long (seconds) to wait for the SMSC to return with a '
        '`submit_sm_resp`. Defaults to 24 hours',
        default=(60 * 60 * 24), static=True)
    submit_sm_encoding = ConfigText(
        'How to encode the SMS before putting on the wire', static=True,
        default='utf-8')
    submit_sm_data_coding = ConfigInt(
        'What data_coding value to tell the SMSC we\'re using when putting'
        'an SMS on the wire', static=True, default=0)
    send_long_messages = ConfigBool(
        "If `True`, messages longer than 254 characters will be sent in the "
        "`message_payload` optional field instead of the `short_message` "
        "field. Default is `False`, simply because that maintains previous "
        "behaviour.", default=False, static=True)
    send_multipart_sar = ConfigBool(
        "If `True`, messages longer than 140 bytes will be sent as a series "
        "of smaller messages with the sar_* parameters set. Default is "
        "`False`.", default=False, static=True)
    send_multipart_udh = ConfigBool(
        "If `True`, messages longer than 140 bytes will be sent as a series "
        "of smaller messages with the user data headers. Default is `False`.",
        default=False, static=True)
    split_bind_prefix = ConfigText(
        "This is the Redis prefix to use for storing things like sequence "
        "numbers and message ids for delivery report handling. It defaults "
        "to `<system_id>@<host>:<port>`. "
        "*ONLY* if the connection is split into two separate binds for RX "
        "and TX then make sure this is the same value for both binds. "
        "This _only_ needs to be done for TX & RX since messages sent via "
        "the TX bind are handled by the RX bind and they need to share the "
        "same prefix for the lookup for message ids in delivery reports to "
        "work.", default='', static=True)

    def post_validate(self):
        long_message_params = (
            'send_long_messages', 'send_multipart_sar', 'send_multipart_udh')
        set_params = [p for p in long_message_params if getattr(self, p)]
        if len(set_params) > 1:
            params = ', '.join(set_params)
            self.raise_config_error(
                "The following parameters are mutually exclusive: %s" % params)
