# -*- test-case-name: vumi.transports.smpp.tests.test_smpp -*-

from datetime import datetime

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi import log
from vumi.utils import get_operator_number
from vumi.transports.base import Transport
from vumi.transports.smpp.clientserver.client import (
    EsmeTransceiverFactory, EsmeTransmitterFactory, EsmeReceiverFactory,
    EsmeCallbacks)
from vumi.transports.smpp.clientserver.config import ClientConfig
from vumi.transports.failures import FailureMessage
from vumi.message import Message, TransportUserMessage
from vumi.persist.txredis_manager import TxRedisManager


class SmppTransport(Transport):
    """
    An SMPP transport.

    The SMPP transport has many configuration parameters. These are
    divided up into sections below.

    SMPP server account configuration options:

    :type system_id: str
    :param system_id:
        User id used to connect to the SMPP server.
    :type password: str
    :param password:
        Password for the system id.
    :type system_type: str, optional
    :param system_type:
        Additional system metadata that is passed through to the SMPP server
        on connect.
    :type host: str
    :param host:
        Hostname of the SMPP server.
    :type port: int
    :param port:
        Port the SMPP server is listening on.
    :type initial_reconnect_delay: int, optional
    :param initial_reconnect_delay:
        Number of seconds to delay before reconnecting to the server after
        being disconnected. Default is 5s. Some WASPs, e.g. Clickatell,
        require a 30s delay before reconnecting. In these cases a 45s
        initial_reconnect_delay is recommended.
    :type split_bind_prefix: str, optional
    :param split_bind_prefix:
        This is the Redis prefix to use for storing things like sequence
        numbers and message ids for delivery report handling.
        It defaults to `<system_id>@<host>:<port>`.

        *ONLY* if the connection is split into two separate binds for RX and TX
        then make sure this is the same value for both binds.
        This _only_ needs to be done for TX & RX since messages sent via the TX
        bind are handled by the RX bind and they need to share the same prefix
        for the lookup for message ids in delivery reports to work.
    :type throttle_delay: float, optional
    :param throttle_delay:
        Delay (in seconds) before retrying a message after receiving
        `ESME_RTHROTTLED`. Default 0.1

    SMPP protocol configuration options:

    :type interface_version: str, optional
    :param interface_version:
        SMPP protocol version. Default is '34' (i.e. version 3.4).
    :type dest_addr_ton:
    :param dest_addr_ton:
        Destination TON (type of number). Default .
    :type dest_addr_npi:
    :param dest_addr_npi:
        Destination NPI (number plan identifier). Default 1 (ISDN/E.164/E.163).
    :type source_addr_ton:
    :param source_addr_ton:
        Source TON (type of number). Default is 0 (Unknown)
    :type source_addr_npi:
    :param source_addr_npi:
        Source NPI (number plan identifier). Default is 0 (Unknown)
    :type registered_delivery:
    :param registered_delivery:
        Whether to ask for delivery reports. Default 1 (request delivery
        reports).

    :param dict data_coding_overrides:
        Overrides for data_coding character set mapping. This is useful for
        setting the default encoding (0), adding additional undefined encodings
        (such as 4 or 8) or overriding encodings in cases where the SMSC is
        violating the spec (which happens a lot). Keys should be integers,
        values should be strings containing valid Python character encoding
        names.

    :param bool send_long_messages:
        If `True`, messages longer than 254 characters will be sent in the
        `message_payload` optional field instead of the `short_message` field.
        Default is `False`, simply because that maintains previous behaviour.

    The list of SMPP protocol configuration options given above is not
    exhaustive. Any other options specified are passed through to the
    python-smpp library PDU (protocol data unit) builder.

    Cellphone number routing options:

    :type COUNTRY_CODE: str, optional
    :param COUNTRY_CODE:
        Used to translate a leading zero in a destination MSISDN into a
        country code. Default '',
    :type OPERATOR_PREFIX: str, optional
    :param OPERATOR_PREFIX:
        Nested dictionary of prefix to network name mappings. Default {} (set
        network to 'UNKNOWN'). E.g. { '27': { '27761': 'NETWORK1' }}.
    :type OPERATOR_NUMBER:
    :param OPERATOR_NUMBER:
        Dictionary of source MSISDN to use for each network listed in
        OPERATOR_PREFIX. If a network is not listed, the source MSISDN
        specified by the message sender is used. Default {} (always used the
        from address specified by the message sender). E.g. { 'NETWORK1':
        '27761234567'}.
    """

    # We only want to start this after we finish connecting to SMPP.
    start_message_consumer = False

    callLater = reactor.callLater

    def validate_config(self):
        self.client_config = ClientConfig.from_config(self.config)
        self.throttle_delay = float(self.config.get('throttle_delay', 0.1))

    @inlineCallbacks
    def setup_transport(self):
        log.msg("Starting the SmppTransport with %s" % self.config)

        self.third_party_id_expiry = self.config.get(
                "third_party_id_expiry",
                60 * 60 * 24 * 7  # 1 week
                )

        r_config = self.config.get('redis_manager', {})
        default_prefix = "%s@%s:%s" % (
                self.client_config.system_id,
                self.client_config.host,
                self.client_config.port,
                )
        r_prefix = self.config.get('split_bind_prefix', default_prefix)
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

        if not hasattr(self, 'esme_client'):
            # start the Smpp transport (if we don't have one)
            self.factory = self.make_factory()
            reactor.connectTCP(
                self.client_config.host,
                self.client_config.port,
                self.factory)

    @inlineCallbacks
    def teardown_transport(self):
        if hasattr(self, 'factory'):
            self.factory.stopTrying()
            self.factory.esme.transport.loseConnection()
        yield self.redis._close()

    def make_factory(self):
        return EsmeTransceiverFactory(
            self.client_config, self.redis, self.esme_callbacks)

    def esme_connected(self, client):
        log.msg("ESME Connected, adding handlers")
        self.esme_client = client
        # Start the consumer
        return self._setup_message_consumer()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.debug("Consumed outgoing message", message)
        log.debug("Unacknowledged message count: %s" % (
                (yield self.esme_client.get_unacked_count()),))
        yield self.r_set_message(message)
        yield self._submit_outbound_message(message)

    @inlineCallbacks
    def _submit_outbound_message(self, message):
        sequence_number = yield self.send_smpp(message)
        yield self.r_set_id_for_sequence(
            sequence_number, message.payload.get("message_id"))

    def esme_disconnected(self):
        log.msg("ESME Disconnected")
        return self._teardown_message_consumer()

    # Redis message storing methods

    def r_message_key(self, message_id):
        return "%s#%s" % (self.r_message_prefix, message_id)

    def r_set_message(self, message):
        message_id = message.payload['message_id']
        return self.redis.set(
            self.r_message_key(message_id), message.to_json())

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
        rkey = self.r_third_party_id_key(third_party_id)
        yield self.redis.set(rkey, id)
        yield self.redis.expire(rkey, self.third_party_id_expiry)

    def _start_throttling(self):
        if self.throttled:
            return
        log.err("Throttling outbound messages.")
        self.throttled = True
        return self._teardown_message_consumer()

    def _stop_throttling(self):
        if not self.throttled:
            return
        log.err("No longer throttling outbound messages.")
        self.throttled = False
        return self._setup_message_consumer()

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
            elif status == 'ESME_RTHROTTLED':
                yield self._start_throttling()
                yield self.submit_sm_throttled(sent_sms_id)
            else:
                # We have an error
                yield self.submit_sm_failure(sent_sms_id, status)
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
            self.failure_publisher.publish_message(FailureMessage(
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
            self.callLater(self.throttle_delay,
                           self._submit_outbound_message, message)

    def delivery_status(self, state):
        if state in [
                "DELIVRD",
                "0"  # Currently we will accept this for Yo! TODO: investigate
                ]:
            return "delivered"
        if state in [
                "REJECTD"
                ]:
            return "failed"
        return "pending"

    @inlineCallbacks
    def delivery_report(self, *args, **kwargs):
        transport_metadata = {
                "message": kwargs['delivery_report'],
                "date": datetime.strptime(
                    kwargs['delivery_report']['done_date'], "%y%m%d%H%M%S")
                }
        delivery_status = self.delivery_status(
            kwargs['delivery_report']['stat'])
        message_id = yield self.r_get_id_for_third_party_id(
            kwargs['delivery_report']['id'])
        log.msg("PUBLISHING DELIV REPORT: %s %s" % (message_id,
                                                    delivery_status))
        returnValue((yield self.publish_delivery_report(
                    user_message_id=message_id,
                    delivery_status=delivery_status,
                    transport_metadata=transport_metadata)))

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
        route = get_operator_number(to_addr,
                self.config.get('COUNTRY_CODE', ''),
                self.config.get('OPERATOR_PREFIX', {}),
                self.config.get('OPERATOR_NUMBER', {})) or from_addr
        return self.esme_client.submit_sm(
                short_message=text.encode('utf-8'),
                destination_addr=str(to_addr),
                source_addr=route,
                message_type=message['transport_type'],
                continue_session=continue_session,
                session_info=message['transport_metadata'].get('session_info'),
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
    def make_factory(self):
        return EsmeTransmitterFactory(
            self.client_config, self.redis, self.esme_callbacks)


class SmppRxTransport(SmppTransport):
    def make_factory(self):
        return EsmeReceiverFactory(
            self.client_config, self.redis, self.esme_callbacks)
