# -*- test-case-name: vumi.workers.smpp.test.test_smpp_transport -*-

from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor

from vumi.service import Worker
from vumi.message import Message
from vumi.workers.smpp.client import EsmeTransceiverFactory
from vumi.utils import get_operator_number, get_deploy_int

from datetime import datetime
import redis


class SmppTransport(Worker):
    """
    The SmppTransport
    """

    def startWorker(self):
        log.msg("Starting the SmppTransport with %s" % self.config)

        # TODO: move this to a config file
        dbindex = get_deploy_int(self._amqp_client.vhost)

        # Connect to Redis
        self.r_server = redis.Redis("localhost", db=dbindex)
        self.r_prefix = "%(system_id)s@%(host)s:%(port)s" % self.config
        log.msg("Connected to Redis, prefix: %s" % self.r_prefix)

        # start the Smpp transport
        factory = EsmeTransceiverFactory(self.config,
                                            self._amqp_client.vumi_options)
        factory.loadDefaults(self.config)

        self.smpp_offset = self.config['smpp_offset']
        self.transport_name = self.config.get('TRANSPORT_NAME', 'fallback')

        self.sequence_key = "%s_%s#last_sequence_number" % (self.r_prefix,
                                                            self.smpp_offset)
        log.msg("sequence_key = %s" % (self.sequence_key))
        last_sequence_number = int(self.r_server.get(self.sequence_key) or 0)

        factory.setLatestSequenceNumber(last_sequence_number)
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

    @inlineCallbacks
    def esme_connected(self, client):
        log.msg("ESME Connected, adding handlers")
        self.esme_client = client
        self.esme_client.set_handler(self)

        # Start the publisher
        self.publisher = yield self.publish_to('smpp.fallback')

        # Start the failure publisher
        rkey = 'sms.outbound.%s.failures' % self.transport_name
        self.failure_publisher = yield self.publish_to(rkey)

        # Start the consumer
        yield self.consume('sms.outbound.%s' % self.transport_name,
                self.consume_message)

    def consume_message(self, message):
        log.msg("Consumed outgoing message", message)
        sequence_number = self.send_smpp(**message.payload)
        self.r_server.set("%s_%s#last_sequence_number" % (
            self.r_prefix, self.smpp_offset),
                sequence_number)
        self.r_server.set("%s#%s" % (self.r_prefix, sequence_number),
                message.payload.get("id"))

    @inlineCallbacks
    def esme_disconnected(self):
        log.msg("ESME Disconnected")
        pass

    @inlineCallbacks
    def submit_sm_resp(self, *args, **kwargs):
        redis_key = "%s#%s" % (self.r_prefix, kwargs['sequence_number'])
        log.msg("redis_key = %s" % redis_key)
        sent_sms_id = self.r_server.get(redis_key)
        transport_msg_id = kwargs['message_id']
        self.r_server.delete(redis_key)
        log.msg("Mapping transport_msg_id=%s to sent_sms_id=%s" % (
            transport_msg_id, sent_sms_id))
        self.publisher.publish_message(Message(**{
            'id': sent_sms_id,
            'transport_message_id': transport_msg_id
            }), routing_key='sms.ack.%s' % self.transport_name)
        yield log.msg("SUBMIT SM RESP %s" % repr(kwargs))

    @inlineCallbacks
    def delivery_report(self, *args, **kwargs):
        log.msg("DELIVERY REPORT", kwargs)
        dictionary = {
            'transport_name': self.transport_name,
            'transport_msg_id': kwargs['delivery_report']['id'],
            'transport_status': kwargs['delivery_report']['stat'],
            'transport_delivered_at': datetime.strptime(
                kwargs['delivery_report']['done_date'],
                "%y%m%d%H%M%S")
        }
        yield self.publisher.publish_message(Message(**dictionary),
            routing_key='sms.receipt.%s' % (self.transport_name,))

    @inlineCallbacks
    def deliver_sm(self, *args, **kwargs):
        yield self.publisher.publish_message(Message(**kwargs),
            routing_key='sms.inbound.%s.%s' % (
                self.transport_name, kwargs.get('destination_addr')))

    def send_smpp(self, id, to_msisdn, message, *args, **kwargs):
        # TODO: Do we want this in the transport or should it be part of the
        #       campaign logic?

        log.msg("Sending SMPP to: %s message: %s" % (to_msisdn, repr(message)))
        # first do a lookup in our YAML to see if we've got a source_addr
        # defined for the given MT number, if not, trust the from_msisdn
        # in the message
        route = get_operator_number(to_msisdn,
                self.config.get('COUNTRY_CODE', ''),
                self.config.get('OPERATOR_PREFIX', {}),
                self.config.get('OPERATOR_NUMBER', {})) \
            or kwargs.get('from_msisdn', '')
        sequence_number = self.esme_client.submit_sm(
                short_message=message.encode('utf-8'),
                destination_addr=str(to_msisdn),
                source_addr=route,
                )
        return sequence_number

    def stopWorker(self):
        log.msg("Stopping the SMPPTransport")

    def send_failure(self, message, reason=None):
        """Send a failure report."""
        log.msg("Failed to send: %s reason: %s" % (message, reason))
        try:
            self.failure_publisher.publish_message(Message(
                    message=message.payload, reason=reason))
        except Exception, e:
            log.err("Error publishing failure:", message, reason, e)
