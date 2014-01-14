from vumi.config import (ConfigText, ConfigInt, ConfigBool,
                         ConfigClientEndpoint, ConfigDict, ConfigFloat,
                         ConfigClassName)
from vumi.transports.smpp.iprocessors import (IDeliveryReportProcessor,
                                              IDeliverShortMessageProcessor)
from vumi.transports.base import Transport


class SmppTransportConfig(Transport.CONFIG_CLASS):

    twisted_endpoint = ConfigClientEndpoint(
        'The SMPP endpoint to connect to.',
        required=True, static=True)
    initial_reconnect_delay = ConfigInt(
        'How long to wait between reconnecting attempts', default=5,
        static=True)
    throttle_delay = ConfigFloat(
        "Delay (in seconds) before retrying a message after receiving "
        "`ESME_RTHROTTLED` or `ESME_RMSGQFUL`.", default=0.1, static=True)
    submit_sm_expiry = ConfigInt(
        'How long (seconds) to wait for the SMSC to return with a '
        '`submit_sm_resp`. Defaults to 24 hours',
        default=(60 * 60 * 24), static=True)
    third_party_id_expiry = ConfigInt(
        'How long (seconds) to keep 3rd party message IDs around to allow for '
        'matching submit_sm_resp and delivery report messages. Defaults to '
        '1 week',
        default=(60 * 60 * 24 * 7), static=True)
    COUNTRY_CODE = ConfigText(
        "Used to translate a leading zero in a destination MSISDN into a "
        "country code. Default ''", default="", static=True)
    OPERATOR_PREFIX = ConfigDict(
        "Nested dictionary of prefix to network name mappings. Default {} "
        "(set network to 'UNKNOWN'). E.g. { '27': { '27761': 'NETWORK1' }} ",
        default={}, static=True)
    OPERATOR_NUMBER = ConfigDict(
        "Dictionary of source MSISDN to use for each network listed in "
        "OPERATOR_PREFIX. If a network is not listed, the source MSISDN "
        "specified by the message sender is used. Default {} (always used the "
        "from address specified by the message sender). "
        "E.g. { 'NETWORK1': '27761234567'}", default={}, static=True)
    redis_manager = ConfigDict(
        'How to connect to Redis', default={}, static=True)
    split_bind_prefix = ConfigText(
        "This is the Redis prefix to use for storing things like sequence "
        "numbers and message ids for delivery report handling. It defaults "
        "to `<system_id>@<transport_name>`. "
        "*ONLY* if the connection is split into two separate binds for RX "
        "and TX then make sure this is the same value for both binds. "
        "This _only_ needs to be done for TX & RX since messages sent via "
        "the TX bind are handled by the RX bind and they need to share the "
        "same prefix for the lookup for message ids in delivery reports to "
        "work.", default='', static=True)
    delivery_report_processor = ConfigClassName(
        'Which delivery report processor to use. '
        'Should implement `IDeliveryReportProcessor`.',
        default=('vumi.transports.smpp.processors.'
                 'EsmeCallbacksDeliveryReportProcessor'),
        static=True, implements=IDeliveryReportProcessor)
    delivery_report_processor_config = ConfigDict(
        'The configuration for the ``delivery_report_processor``',
        default={}, static=True)
    short_message_processor = ConfigClassName(
        'Which short message processor to use. '
        'Should implement `IDeliverShortMessageProcessor`.',
        default=('vumi.transports.smpp.processors.'
                 'EsmeCallbacksDeliverShortMessageProcessor'),
        static=True, implements=IDeliverShortMessageProcessor)
    short_message_processor_config = ConfigDict(
        'The configuration for the ``short_message_processor``',
        default={}, static=True)
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
    submit_sm_encoding = ConfigText(
        'How to encode the SMS before putting on the wire', static=True,
        default='utf-8')
    submit_sm_data_coding = ConfigInt(
        'What data_coding value to tell the SMSC we\'re using when putting'
        'an SMS on the wire', static=True, default=0)

    def post_validate(self):
        long_message_params = (
            'send_long_messages', 'send_multipart_sar', 'send_multipart_udh')
        set_params = [p for p in long_message_params if getattr(self, p)]
        if len(set_params) > 1:
            params = ', '.join(set_params)
            self.raise_config_error(
                "The following parameters are mutually exclusive: %s" % params)
