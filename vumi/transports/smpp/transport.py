# -*- test-case-name: vumi.transports.smpp.tests.test_smpp -*-

from datetime import datetime

import redis
from twisted.python import log
from twisted.internet import reactor

from vumi.utils import get_operator_number
from vumi.transports.base import Transport
from vumi.transports.smpp.clientserver.client import (EsmeTransceiverFactory,
                                                      EsmeTransmitterFactory,
                                                      EsmeReceiverFactory,
                                                      EsmeCallbacks)
from vumi.transports.smpp.clientserver.config import ClientConfig
from vumi.transports.failures import FailureMessage
from vumi.message import Message


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

    def validate_config(self):
        self.client_config = ClientConfig.from_config(self.config)

    def setup_transport(self):
        log.msg("Starting the SmppTransport with %s" % self.config)

        self.third_party_id_expiry = self.config.get(
                "third_party_id_expiry",
                60 * 60 * 24 * 7  # 1 week
                )

        # Connect to Redis
        if not hasattr(self, 'r_server'):
            # Only set up redis if we don't have a test stub already
            self.r_server = redis.Redis(**self.config.get('redis', {}))

        self.r_prefix = "%s@%s:%s" % (
                self.client_config.system_id,
                self.client_config.host,
                self.client_config.port,
                )
        self.r_message_prefix = "%s#message_json" % self.r_prefix
        log.msg("Connected to Redis, prefix: %s" % self.r_prefix)

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

    def make_factory(self):
        return EsmeTransceiverFactory(self.client_config,
                                        self.r_server,
                                        self.esme_callbacks)

    def esme_connected(self, client):
        log.msg("ESME Connected, adding handlers")
        self.esme_client = client
        # Start the consumer
        return self._setup_message_consumer()

    def handle_outbound_message(self, message):
        log.msg("Consumed outgoing message", message)
        log.msg("Unacknowledged message count: %s" % (
            self.esme_client.get_unacked_count()))
        self.r_set_message(message)
        sequence_number = self.send_smpp(message)
        self.r_set_id_for_sequence(sequence_number,
                                   message.payload.get("message_id"))

    def esme_disconnected(self):
        log.msg("ESME Disconnected")
        return self._teardown_message_consumer()

    # Redis message storing methods

    def r_message_key(self, message_id):
        return "%s#%s" % (self.r_message_prefix, message_id)

    def r_set_message(self, message):
        message_id = message.payload['message_id']
        self.r_server.set(self.r_message_key(message_id), message.to_json())

    def r_get_message_json(self, message_id):
        return self.r_server.get(self.r_message_key(message_id))

    def r_get_message(self, message_id):
        json_string = self.r_get_message_json(message_id)
        if json_string:
            return Message.from_json(json_string)
        else:
            return None

    def r_delete_message(self, message_id):
        return self.r_server.delete(self.r_message_key(message_id))

    # Redis sequence number storing methods

    def r_sequence_number_key(self, sequence_number):
        return "%s#%s" % (self.r_prefix, sequence_number)

    def r_get_id_for_sequence(self, sequence_number):
        return self.r_server.get(self.r_sequence_number_key(sequence_number))

    def r_delete_for_sequence(self, sequence_number):
        return self.r_server.delete(
                self.r_sequence_number_key(sequence_number))

    def r_set_id_for_sequence(self, sequence_number, id):
        self.r_server.set(self.r_sequence_number_key(sequence_number), id)

    # Redis 3rd party id to vumi id mapping

    def r_third_party_id_key(self, third_party_id):
        return "%s#3rd_party_id#%s" % (self.r_prefix, third_party_id)

    def r_get_id_for_third_party_id(self, third_party_id):
        return self.r_server.get(self.r_third_party_id_key(third_party_id))

    def r_delete_for_third_party_id(self, third_party_id):
        return self.r_server.delete(
                self.r_third_party_id_key(third_party_id))

    def r_set_id_for_third_party_id(self, third_party_id, id):
        rkey = self.r_third_party_id_key(third_party_id)
        self.r_server.set(rkey, id)
        self.r_server.expire(rkey, self.third_party_id_expiry)

    def submit_sm_resp(self, *args, **kwargs):
        transport_msg_id = kwargs['message_id']
        sent_sms_id = self.r_get_id_for_sequence(kwargs['sequence_number'])
        if sent_sms_id is None:
            log.err("Sequence number lookup failed for:%s" % (
                kwargs['sequence_number']))
        else:
            self.r_set_id_for_third_party_id(transport_msg_id, sent_sms_id)
            self.r_delete_for_sequence(kwargs['sequence_number'])
            if kwargs['command_status'] == 'ESME_ROK':
                # The sms was submitted ok
                self.submit_sm_success(sent_sms_id, transport_msg_id)
            else:
                # We have an error
                self.submit_sm_failure(sent_sms_id, kwargs['command_status'])

    def submit_sm_success(self, sent_sms_id, transport_msg_id):
        self.r_delete_message(sent_sms_id)
        log.msg("Mapping transport_msg_id=%s to sent_sms_id=%s" % (
            transport_msg_id, sent_sms_id))
        log.msg("PUBLISHING ACK: (%s -> %s)" % (
            sent_sms_id, transport_msg_id))
        self.publish_ack(
            user_message_id=sent_sms_id,
            sent_message_id=transport_msg_id)

    def submit_sm_failure(self, sent_sms_id, reason, failure_code=None):
        error_message = self.r_get_message(sent_sms_id)
        if error_message is None:
            log.err("Could not retrieve failed message:%s" % (
                sent_sms_id))
        else:
            self.r_delete_message(sent_sms_id)
            self.failure_publisher.publish_message(FailureMessage(
                    message=error_message.payload,
                    failure_code=None,
                    reason=reason))

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

    def delivery_report(self, *args, **kwargs):
        transport_metadata = {
                "message": kwargs['delivery_report'],
                "date": datetime.strptime(
                    kwargs['delivery_report']['done_date'], "%y%m%d%H%M%S")
                }
        delivery_status = self.delivery_status(
            kwargs['delivery_report']['stat'])
        message_id = self.r_get_id_for_third_party_id(
                                        kwargs['delivery_report']['id'])
        log.msg("PUBLISHING DELIV REPORT: %s %s" % (message_id,
                                                    delivery_status))
        return self.publish_delivery_report(
            user_message_id=message_id,
            delivery_status=delivery_status,
            transport_metadata=transport_metadata)

    def deliver_sm(self, *args, **kwargs):
        message = dict(
                message_id=kwargs.get('message_id'),
                to_addr=kwargs.get('destination_addr'),
                from_addr=kwargs.get('source_addr'),
                transport_type='sms',
                content=kwargs.get('short_message'))
        log.msg("PUBLISHING INBOUND: %s" % (message,))
        # TODO: This logs messages that fail to serialize to JSON
        #       Usually this happens when an SMPP message has content
        #       we can't decode (e.g. data_coding == 4). We should
        #       remove the try-except once we handle such messages
        #       better.
        try:
            return self.publish_message(**message)
        except Exception, e:
            log.err(e)

    def send_smpp(self, message):
        log.msg("Sending SMPP message: %s" % (message))
        # first do a lookup in our YAML to see if we've got a source_addr
        # defined for the given MT number, if not, trust the from_addr
        # in the message
        to_addr = message['to_addr']
        from_addr = message['from_addr']
        text = message['content']
        route = get_operator_number(to_addr,
                self.config.get('COUNTRY_CODE', ''),
                self.config.get('OPERATOR_PREFIX', {}),
                self.config.get('OPERATOR_NUMBER', {})) or from_addr
        sequence_number = self.esme_client.submit_sm(
                short_message=text.encode('utf-8'),
                destination_addr=str(to_addr),
                source_addr=route,
                )
        return sequence_number

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
        return EsmeTransmitterFactory(self.client_config,
                                      self.r_server,
                                      self.esme_callbacks)


class SmppRxTransport(SmppTransport):
    def make_factory(self):
        return EsmeReceiverFactory(self.client_config,
                                   self.r_server,
                                   self.esme_callbacks)
