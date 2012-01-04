# -*- test-case-name: vumi.transports.smpp.test_smpp -*-

from datetime import datetime

import redis
from twisted.python import log
from twisted.internet import reactor

# from vumi.service import Worker
from vumi.utils import get_operator_number, get_deploy_int
from vumi.transports.base import Transport
from vumi.transports.smpp.client import EsmeTransceiverFactory
from vumi.message import Message


class SmppTransport(Transport):
    """
    An SMPP transport.

    The SMPP transport has many configuration parameters. These are
    divided up into sections below.

    SMPP sequence number configuration options:

    :type smpp_increment: int
    :param smpp_increment:
        Increment for SMPP sequence number (must be >= number of
        SMPP workers on a single SMPP account).
    :type smpp_offset: int
    :param smpp_offset:
        Offset for this worker's SMPP sequence numbers (no duplicates
        on a single SMPP account and must be <= increment)

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

    def setup_transport(self):
        log.msg("Starting the SmppTransport with %s" % self.config)

        # TODO: move this to a config file
        dbindex = get_deploy_int(self._amqp_client.vhost)
        self.smpp_offset = int(self.config['smpp_offset'])

        # Connect to Redis
        if not hasattr(self, 'r_server'):
            # Only set up redis if we don't have a test stub already
            self.r_server = redis.Redis("localhost", db=dbindex)
        self.r_prefix = "%(system_id)s@%(host)s:%(port)s" % self.config
        log.msg("Connected to Redis, prefix: %s" % self.r_prefix)
        last_sequence_number = int(self.r_get_last_sequence()
                                   or self.smpp_offset)
        log.msg("Last sequence_number: %s" % last_sequence_number)

        if not hasattr(self, 'esme_client'):
            # start the Smpp transport (if we don't have one)
            factory = EsmeTransceiverFactory(self.config,
                                             self._amqp_client.vumi_options)
            factory.loadDefaults(self.config)
            factory.setLastSequenceNumber(last_sequence_number)
            factory.setConnectCallback(self.esme_connected)
            factory.setDisconnectCallback(self.esme_disconnected)
            factory.setSubmitSMRespCallback(self.submit_sm_resp)
            factory.setDeliveryReportCallback(self.delivery_report)
            factory.setDeliverSMCallback(self.deliver_sm)
            factory.setSendFailureCallback(self.send_failure)
            log.msg(factory.defaults)
            reactor.connectTCP(
                factory.defaults['host'],
                factory.defaults['port'],
                factory)

    def esme_connected(self, client):
        log.msg("ESME Connected, adding handlers")
        self.esme_client = client
        self.esme_client.update_error_handlers({
            "mess_tempfault": self.mess_tempfault,
            "conn_throttle": self.conn_throttle,
            })

        # Start the consumer
        return self._setup_message_consumer()

    def handle_outbound_message(self, message):
        log.msg("Consumed outgoing message", message)
        log.msg("Unacknowledged message count: %s" % (
            self.esme_client.get_unacked_count()))
        #self.conn_throttle(unacked=self.esme_client.get_unacked_count())
        sequence_number = self.send_smpp(message)
        self.r_set_last_sequence(sequence_number)
        self.r_set_id_for_sequence(sequence_number,
                                   message.payload.get("message_id"))

    def esme_disconnected(self):
        log.msg("ESME Disconnected")
        return self._teardown_message_consumer()

    def r_get_id_for_sequence(self, sequence_number):
        return self.r_server.get("%s#%s" % (self.r_prefix, sequence_number))

    def r_delete_for_sequence(self, sequence_number):
        return self.r_server.delete("%s#%s" % (self.r_prefix, sequence_number))

    def r_set_id_for_sequence(self, sequence_number, id):
        self.r_server.set("%s#%s" % (self.r_prefix, sequence_number), id)

    def r_get_last_sequence(self):
        return self.r_server.get("%s_%s#last_sequence_number" % (
            self.r_prefix, self.smpp_offset))

    def r_set_last_sequence(self, sequence_number):
        self.r_server.set("%s_%s#last_sequence_number" % (
            self.r_prefix, self.smpp_offset),
                sequence_number)

    def submit_sm_resp(self, *args, **kwargs):
        transport_msg_id = kwargs['message_id']
        sent_sms_id = self.r_get_id_for_sequence(kwargs['sequence_number'])
        self.r_delete_for_sequence(kwargs['sequence_number'])
        log.msg("Mapping transport_msg_id=%s to sent_sms_id=%s" % (
            transport_msg_id, sent_sms_id))
        log.msg("PUBLISHING ACK: (%s -> %s)" % (sent_sms_id, transport_msg_id))
        return self.publish_ack(
            user_message_id=sent_sms_id,
            sent_message_id=transport_msg_id)

    def delivery_status(self, state):
        if state in [
                "DELIVRD"
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
        message_id = kwargs['delivery_report']['id']
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

    def mess_tempfault(self, *args, **kwargs):
        pdu = kwargs.get('pdu')
        sequence_number = pdu['header']['sequence_number']
        id = self.r_get_id_for_sequence(sequence_number)
        reason = pdu['header']['command_status']
        # TODO: Get real message here.
        self.send_failure(Message(id=id), reason)

    def conn_throttle(self, *args, **kwargs):
        log.msg("*********** conn_throttle: %s" % kwargs)
        unacked = kwargs.get('unacked', 0)
        if unacked > 100:
            # do something
            pass
        pdu = kwargs.get('pdu')
        if pdu:
            # do as above
            pass
