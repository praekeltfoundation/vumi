from vumi.config import (
    ConfigText, ConfigInt, ConfigBool, ConfigClientEndpoint, ConfigDict,
    ConfigFloat, ConfigClassName, ClientEndpointFallback)
from vumi.transports.smpp.iprocessors import (
    IDeliveryReportProcessor, IDeliverShortMessageProcessor,
    ISubmitShortMessageProcessor)
from vumi.codecs.ivumi_codecs import IVumiCodec
from vumi.transports.base import Transport


class SmppTransportConfig(Transport.CONFIG_CLASS):

    twisted_endpoint = ConfigClientEndpoint(
        'The SMPP endpoint to connect to.',
        required=True, static=True,
        fallbacks=[ClientEndpointFallback()])
    initial_reconnect_delay = ConfigInt(
        'How long (in seconds) to wait between reconnecting attempts. '
        'Defaults to 5 seconds.', default=5, static=True)
    throttle_delay = ConfigFloat(
        "Delay (in seconds) before retrying a message after receiving "
        "`ESME_RTHROTTLED` or `ESME_RMSGQFUL`.", default=0.1, static=True)
    deliver_sm_decoding_error = ConfigText(
        'The error to respond with when we were unable to decode all parts '
        'of a PDU.', default='ESME_RDELIVERYFAILURE', static=True)
    submit_sm_expiry = ConfigInt(
        'How long (in seconds) to wait for the SMSC to return with a '
        '`submit_sm_resp`. Defaults to 24 hours.',
        default=(60 * 60 * 24), static=True)
    third_party_id_expiry = ConfigInt(
        'How long (in seconds) to keep 3rd party message IDs around to allow '
        'for matching submit_sm_resp and delivery report messages. Defaults '
        'to 1 week.',
        default=(60 * 60 * 24 * 7), static=True)
    redis_manager = ConfigDict(
        'How to connect to Redis.', default={}, static=True)
    split_bind_prefix = ConfigText(
        "This is the Redis prefix to use for storing things like sequence "
        "numbers and message ids for delivery report handling. It defaults "
        "to `<system_id>@<transport_name>`. "
        "This should *ONLY* be done for TX & RX since messages sent via "
        "the TX bind are handled by the RX bind and they need to share the "
        "same prefix for the lookup for message ids in delivery reports to "
        "work.", default='', static=True)
    codec_class = ConfigClassName(
        'Which class should be used to handle character encoding/decoding. '
        'MUST implement `IVumiCodec`.',
        default='vumi.codecs.VumiCodec',
        static=True, implements=IVumiCodec)
    delivery_report_processor = ConfigClassName(
        'Which delivery report processor to use. '
        'MUST implement `IDeliveryReportProcessor`.',
        default=('vumi.transports.smpp.processors.'
                 'DeliveryReportProcessor'),
        static=True, implements=IDeliveryReportProcessor)
    delivery_report_processor_config = ConfigDict(
        'The configuration for the `delivery_report_processor`.',
        default={}, static=True)
    deliver_short_message_processor = ConfigClassName(
        'Which deliver short message processor to use. '
        'MUST implement `IDeliverShortMessageProcessor`.',
        default='vumi.transports.smpp.processors.DeliverShortMessageProcessor',
        static=True, implements=IDeliverShortMessageProcessor)
    deliver_short_message_processor_config = ConfigDict(
        'The configuration for the `deliver_short_message_processor`.',
        default={}, static=True)
    submit_short_message_processor = ConfigClassName(
        'Which submit short message processor to use. '
        'Should implements `ISubmitShortMessageProcessor`.',
        default='vumi.transports.smpp.processors.SubmitShortMessageProcessor',
        static=True, implements=ISubmitShortMessageProcessor)
    submit_short_message_processor_config = ConfigDict(
        'The configuration for the `submit_short_message_processor`.',
        default={}, static=True)
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
        'The SMPP service type.', default="", static=True)
    address_range = ConfigText(
        "Address range to receive. (SMSC-specific format, default empty.)",
        default="", static=True)
    dest_addr_ton = ConfigInt(
        'Destination TON (type of number).', default=0, static=True)
    dest_addr_npi = ConfigInt(
        'Destination NPI (number plan identifier). '
        'Default 1 (ISDN/E.164/E.163).', default=1, static=True)
    source_addr_ton = ConfigInt(
        'Source TON (type of number).', default=0, static=True)
    source_addr_npi = ConfigInt(
        'Source NPI (number plan identifier).', default=0, static=True)
    registered_delivery = ConfigBool(
        'Whether or not to request delivery reports. Default True.',
        default=True, static=True)
    smpp_bind_timeout = ConfigInt(
        'How long (in seconds) to wait for a succesful bind. Default 30.',
        default=30, static=True)
    smpp_enquire_link_interval = ConfigInt(
        "How long (in seconds) to delay before reconnecting to the server "
        "after being disconnected. Some WASPs, e.g. Clickatell require a 30s "
        "delay before reconnecting. In these cases a 45s "
        "`initial_reconnect_delay` is recommended. Default 55.",
        default=55, static=True)
    mt_tps = ConfigInt(
        'Mobile Terminated Transactions per Second. The Maximum Vumi '
        'messages per second to attempt to put on the wire. '
        'Defaults to 0 which means no throttling is applied. '
        '(NOTE: 1 Vumi message may result in multiple PDUs)',
        default=0, static=True, required=False)

    # TODO: Deprecate these fields when confmodel#5 is done.
    host = ConfigText(
        "*DEPRECATED* 'host' and 'port' fields may be used in place of the"
        " 'twisted_endpoint' field.", static=True)
    port = ConfigInt(
        "*DEPRECATED* 'host' and 'port' fields may be used in place of the"
        " 'twisted_endpoint' field.", static=True)
