# -*- test-case-name: vumi.transports.smpp.deprecated.tests.test_smpp -*-

import warnings

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi import log
from vumi.reconnecting_client import ReconnectingClientService
from vumi.utils import get_operator_number
from vumi.transports.base import Transport
from vumi.transports.smpp.deprecated.clientserver.client import (
    EsmeTransceiverFactory, EsmeTransmitterFactory, EsmeReceiverFactory,
    EsmeCallbacks)
from vumi.transports.failures import FailureMessage
from vumi.message import Message, TransportUserMessage
from vumi.persist.txredis_manager import TxRedisManager
from vumi.config import (
    ConfigText, ConfigInt, ConfigBool, ConfigDict, ConfigFloat, ConfigRegex,
    ConfigClientEndpoint, ClientEndpointFallback)


class SmppTransportConfig(Transport.CONFIG_CLASS):

    DELIVERY_REPORT_REGEX = (
        'id:(?P<id>\S{,65})'
        ' +sub:(?P<sub>...)'
        ' +dlvrd:(?P<dlvrd>...)'
        ' +submit date:(?P<submit_date>\d*)'
        ' +done date:(?P<done_date>\d*)'
        ' +stat:(?P<stat>[A-Z]{7})'
        ' +err:(?P<err>...)'
        ' +[Tt]ext:(?P<text>.{,20})'
        '.*'
    )

    DELIVERY_REPORT_STATUS_MAPPING = {
        # Output values should map to themselves:
        'delivered': 'delivered',
        'failed': 'failed',
        'pending': 'pending',
        # SMPP `message_state` values:
        'ENROUTE': 'pending',
        'DELIVERED': 'delivered',
        'EXPIRED': 'failed',
        'DELETED': 'failed',
        'UNDELIVERABLE': 'failed',
        'ACCEPTED': 'delivered',
        'UNKNOWN': 'pending',
        'REJECTED': 'failed',
        # From the most common regex-extracted format:
        'DELIVRD': 'delivered',
        'REJECTD': 'failed',
        # Currently we will accept this for Yo! TODO: investigate
        '0': 'delivered',
    }

    twisted_endpoint = ConfigClientEndpoint(
        'The SMPP endpoint to connect to.',
        required=True, static=True,
        fallbacks=[ClientEndpointFallback()])
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
    delivery_report_regex = ConfigRegex(
        'What regex to use for matching delivery reports',
        default=DELIVERY_REPORT_REGEX, static=True)
    delivery_report_status_mapping = ConfigDict(
        "Mapping from delivery report message state to "
        "(`delivered`, `failed`, `pending`)",
        static=True, default=DELIVERY_REPORT_STATUS_MAPPING)
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
    data_coding_overrides = ConfigDict(
        "Overrides for data_coding character set mapping. This is useful for "
        "setting the default encoding (0), adding additional undefined "
        "encodings (such as 4 or 8) or overriding encodings in cases where "
        "the SMSC is violating the spec (which happens a lot). Keys should "
        "be integers, values should be strings containing valid Python "
        "character encoding names.", default={}, static=True)
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
    throttle_delay = ConfigFloat(
        "Delay (in seconds) before retrying a message after receiving "
        "`ESME_RTHROTTLED` or `ESME_RMSGQFUL`.", default=0.1, static=True)
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

    # TODO: Deprecate these fields when confmodel#5 is done.
    host = ConfigText(
        "*DEPRECATED* 'host' and 'port' fields may be used in place of the"
        " 'twisted_endpoint' field.", static=True)
    port = ConfigInt(
        "*DEPRECATED* 'host' and 'port' fields may be used in place of the"
        " 'twisted_endpoint' field.", static=True)

    def post_validate(self):
        long_message_params = (
            'send_long_messages', 'send_multipart_sar', 'send_multipart_udh')
        set_params = [p for p in long_message_params if getattr(self, p)]
        if len(set_params) > 1:
            params = ', '.join(set_params)
            self.raise_config_error(
                "The following parameters are mutually exclusive: %s" % params)


class SmppTransport(Transport):
    """
    An SMPP Transceiver Transport.
    """
    CONFIG_CLASS = SmppTransportConfig

    # Which of the keys in SmppTransportConfig are keys that are to
    # be passed on to the ESMETransceiver base class to create a bind with.
    SMPP_BIND_CONFIG_KEYS = [
        'system_id',
        'password',
        'system_type',
        'interface_version',
        'service_type',
        'dest_addr_ton',
        'dest_addr_npi',
        'source_addr_ton',
        'source_addr_npi',
        'registered_delivery',
    ]

    # We only want to start this after we finish connecting to SMPP.
    start_message_consumer = False

    callLater = reactor.callLater

    @inlineCallbacks
    def setup_transport(self):
        warnings.warn(
            'This SMPP implementation is deprecated. Please use the '
            'implementations available in vumi.transports.smpp.'
            'smpp_transport instead.', category=DeprecationWarning)
        config = self.get_static_config()
        log.msg("Starting the SmppTransport for %s" % (
            config.twisted_endpoint))

        self.submit_sm_encoding = config.submit_sm_encoding
        self.submit_sm_data_coding = config.submit_sm_data_coding
        default_prefix = "%s@%s" % (config.system_id, config.transport_name)

        r_config = config.redis_manager
        r_prefix = config.split_bind_prefix or default_prefix

        redis = yield TxRedisManager.from_config(r_config)
        self.redis = redis.sub_manager(r_prefix)

        self.r_message_prefix = "message_json"
        self.throttled = False

        self.esme_callbacks = EsmeCallbacks(
            connect=self.esme_connected,
            disconnect=self.esme_disconnected,
            submit_sm_resp=self.submit_sm_resp,
            delivery_report=self.delivery_report,
            deliver_sm=self.deliver_sm)

        self._reconn_service = None
        if not hasattr(self, 'esme_client'):
            # start the Smpp transport (if we don't have one)
            self.factory = self.make_factory()
            self._reconn_service = ReconnectingClientService(
                config.twisted_endpoint, self.factory)
            self._reconn_service.startService()

    @inlineCallbacks
    def teardown_transport(self):
        if self._reconn_service is not None:
            yield self._reconn_service.stopService()
        yield self.redis._close()

    def get_smpp_bind_params(self):
        """Inspects the SmppTransportConfig and returns a dictionary
        that can be passed to an EsmeTransceiver (or subclass there of)
        to create a bind with"""
        config = self.get_static_config()
        return dict([(key, getattr(config, key))
                    for key in self.SMPP_BIND_CONFIG_KEYS])

    def make_factory(self):
        return EsmeTransceiverFactory(
            self.get_static_config(), self.get_smpp_bind_params(),
            self.redis, self.esme_callbacks)

    def esme_connected(self, client):
        log.msg("ESME Connected, adding handlers")
        self.esme_client = client
        # Start the consumer
        self.unpause_connectors()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.debug("Consumed outgoing message %r" % (message,))
        log.debug("Unacknowledged message count: %s" % (
                (yield self.esme_client.get_unacked_count()),))
        yield self.r_set_message(message)
        yield self._submit_outbound_message(message)

    @inlineCallbacks
    def _submit_outbound_message(self, message):
        sequence_numbers = yield self.send_smpp(message)
        # TODO: Handle multiple acks for a single message that we split up.
        for sequence_number in sequence_numbers:
            yield self.r_set_id_for_sequence(
                sequence_number, message.payload.get("message_id"))

    def esme_disconnected(self):
        log.msg("ESME Disconnected")
        return self.pause_connectors()

    # Redis message storing methods

    def r_message_key(self, message_id):
        return "%s#%s" % (self.r_message_prefix, message_id)

    def r_set_message(self, message):
        config = self.get_static_config()
        message_id = message.payload['message_id']
        message_key = self.r_message_key(message_id)
        d = self.redis.set(message_key, message.to_json())
        d.addCallback(lambda _: self.redis.expire(message_key,
                                                  config.submit_sm_expiry))
        return d

    def r_get_message_json(self, message_id):
        return self.redis.get(self.r_message_key(message_id))

    @inlineCallbacks
    def r_get_message(self, message_id):
        json_string = yield self.r_get_message_json(message_id)
        if json_string:
            returnValue(Message.from_json(json_string))
        else:
            returnValue(None)

    def r_delete_message(self, message_id):
        return self.redis.delete(self.r_message_key(message_id))

    # Redis sequence number storing methods

    def r_get_id_for_sequence(self, sequence_number):
        return self.redis.get(str(sequence_number))

    def r_delete_for_sequence(self, sequence_number):
        return self.redis.delete(str(sequence_number))

    def r_set_id_for_sequence(self, sequence_number, id):
        return self.redis.set(str(sequence_number), id)

    # Redis 3rd party id to vumi id mapping

    def r_third_party_id_key(self, third_party_id):
        return "3rd_party_id#%s" % (third_party_id,)

    def r_get_id_for_third_party_id(self, third_party_id):
        return self.redis.get(self.r_third_party_id_key(third_party_id))

    def r_delete_for_third_party_id(self, third_party_id):
        return self.redis.delete(
                self.r_third_party_id_key(third_party_id))

    @inlineCallbacks
    def r_set_id_for_third_party_id(self, third_party_id, id):
        config = self.get_static_config()
        rkey = self.r_third_party_id_key(third_party_id)
        yield self.redis.set(rkey, id)
        yield self.redis.expire(rkey, config.third_party_id_expiry)

    def _start_throttling(self):
        if self.throttled:
            return
        log.err("Throttling outbound messages.")
        self.throttled = True
        self.pause_connectors()

    def _stop_throttling(self):
        if not self.throttled:
            return
        log.err("No longer throttling outbound messages.")
        self.throttled = False
        self.unpause_connectors()

    @inlineCallbacks
    def submit_sm_resp(self, *args, **kwargs):
        transport_msg_id = kwargs['message_id']
        sent_sms_id = (
            yield self.r_get_id_for_sequence(kwargs['sequence_number']))
        if sent_sms_id is None:
            log.err("Sequence number lookup failed for:%s" % (
                kwargs['sequence_number'],))
        else:
            yield self.r_set_id_for_third_party_id(
                transport_msg_id, sent_sms_id)
            yield self.r_delete_for_sequence(kwargs['sequence_number'])
            status = kwargs['command_status']
            if status == 'ESME_ROK':
                # The sms was submitted ok
                yield self.submit_sm_success(sent_sms_id, transport_msg_id)
                yield self._stop_throttling()
            elif status in ('ESME_RTHROTTLED', 'ESME_RMSGQFUL'):
                yield self._start_throttling()
                yield self.submit_sm_throttled(sent_sms_id)
            else:
                # We have an error
                yield self.submit_sm_failure(sent_sms_id,
                                             status or 'Unspecified')
                yield self._stop_throttling()

    @inlineCallbacks
    def submit_sm_success(self, sent_sms_id, transport_msg_id):
        yield self.r_delete_message(sent_sms_id)
        log.debug("Mapping transport_msg_id=%s to sent_sms_id=%s" % (
            transport_msg_id, sent_sms_id))
        log.debug("PUBLISHING ACK: (%s -> %s)" % (
            sent_sms_id, transport_msg_id))
        self.publish_ack(
            user_message_id=sent_sms_id,
            sent_message_id=transport_msg_id)

    @inlineCallbacks
    def submit_sm_failure(self, sent_sms_id, reason, failure_code=None):
        error_message = yield self.r_get_message(sent_sms_id)
        if error_message is None:
            log.err("Could not retrieve failed message:%s" % (
                sent_sms_id))
        else:
            yield self.r_delete_message(sent_sms_id)
            yield self.publish_nack(sent_sms_id, reason)
            yield self.failure_publisher.publish_message(FailureMessage(
                    message=error_message.payload,
                    failure_code=None,
                    reason=reason))

    @inlineCallbacks
    def submit_sm_throttled(self, sent_sms_id):
        message = yield self.r_get_message(sent_sms_id)
        if message is None:
            log.err("Could not retrieve throttled message:%s" % (
                sent_sms_id))
        else:
            config = self.get_static_config()
            self.callLater(config.throttle_delay,
                           self._submit_outbound_message, message)

    def delivery_status(self, state):
        config = self.get_static_config()
        return config.delivery_report_status_mapping.get(state, 'pending')

    @inlineCallbacks
    def delivery_report(self, message_id, message_state):
        delivery_status = self.delivery_status(message_state)
        message_id = yield self.r_get_id_for_third_party_id(message_id)
        if message_id is None:
            log.warning("Failed to retrieve message id for delivery report."
                        " Delivery report from %s discarded."
                        % self.transport_name)
            return
        log.msg("PUBLISHING DELIV REPORT: %s %s" % (message_id,
                                                    delivery_status))
        returnValue((yield self.publish_delivery_report(
                    user_message_id=message_id,
                    delivery_status=delivery_status)))

    def deliver_sm(self, *args, **kwargs):
        message_type = kwargs.get('message_type', 'sms')
        message = {
            'message_id': kwargs['message_id'],
            'to_addr': kwargs['destination_addr'],
            'from_addr': kwargs['source_addr'],
            'content': kwargs['short_message'],
            'transport_type': message_type,
            'transport_metadata': {},
            }

        if message_type == 'ussd':
            session_event = {
                'new': TransportUserMessage.SESSION_NEW,
                'continue': TransportUserMessage.SESSION_RESUME,
                'close': TransportUserMessage.SESSION_CLOSE,
                }[kwargs['session_event']]
            message['session_event'] = session_event
            session_info = kwargs.get('session_info')
            message['transport_metadata']['session_info'] = session_info

        log.msg("PUBLISHING INBOUND: %s" % (message,))
        # TODO: This logs messages that fail to serialize to JSON
        #       Usually this happens when an SMPP message has content
        #       we can't decode (e.g. data_coding == 4). We should
        #       remove the try-except once we handle such messages
        #       better.
        return self.publish_message(**message).addErrback(log.err)

    def send_smpp(self, message):
        log.debug("Sending SMPP message: %s" % (message))
        # first do a lookup in our YAML to see if we've got a source_addr
        # defined for the given MT number, if not, trust the from_addr
        # in the message
        to_addr = message['to_addr']
        from_addr = message['from_addr']
        text = message['content']
        continue_session = (
            message['session_event'] != TransportUserMessage.SESSION_CLOSE)
        config = self.get_static_config()
        route = get_operator_number(to_addr, config.COUNTRY_CODE,
                                    config.OPERATOR_PREFIX,
                                    config.OPERATOR_NUMBER)
        source_addr = route or from_addr
        session_info = message['transport_metadata'].get('session_info')
        return self.esme_client.submit_sm(
            # these end up in the PDU
            short_message=text.encode(self.submit_sm_encoding),
            data_coding=self.submit_sm_data_coding,
            destination_addr=to_addr.encode('ascii'),
            source_addr=source_addr.encode('ascii'),
            session_info=session_info.encode('ascii')
                if session_info is not None else None,
            # these don't end up in the PDU
            message_type=message['transport_type'],
            continue_session=continue_session,
        )

    def stopWorker(self):
        log.msg("Stopping the SMPPTransport")
        return super(SmppTransport, self).stopWorker()

    def send_failure(self, message, exception, reason):
        """Send a failure report."""
        log.msg("Failed to send: %s reason: %s" % (message, reason))
        return super(SmppTransport, self).send_failure(message,
                                                       exception, reason)


class SmppTxTransport(SmppTransport):
    """An Smpp Transmitter Transport"""
    def make_factory(self):
        return EsmeTransmitterFactory(
            self.get_static_config(), self.get_smpp_bind_params(),
            self.redis, self.esme_callbacks)


class SmppRxTransport(SmppTransport):
    """An Smpp Receiver Transport"""
    def make_factory(self):
        return EsmeReceiverFactory(
            self.get_static_config(), self.get_smpp_bind_params(),
            self.redis, self.esme_callbacks)
