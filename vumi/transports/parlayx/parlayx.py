# -*- test-case-name: vumi.transports.parlayx.tests.test_parlayx -*-
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi import log
from vumi.config import ConfigText, ConfigInt
from vumi.transports.base import Transport
from vumi.transports.failures import TemporaryFailure, PermanentFailure
from vumi.transports.parlayx.client import (
    ParlayXClient, ServiceException, PolicyException)
from vumi.transports.parlayx.server import SmsNotificationService
from vumi.transports.parlayx.soaputil import SoapFault


class ParlayXTransportConfig(Transport.CONFIG_CLASS):
    web_notification_path = ConfigText(
        'Path to listen for delivery and receipt notifications on',
        static=True)
    web_notification_port = ConfigInt(
        'Port to listen for delivery and receipt notifications on',
        default=0, static=True)
    notification_endpoint_uri = ConfigText(
        'URI of the ParlayX SmsNotificationService in Vumi', static=True)
    short_code = ConfigText(
        'Service activation number or short code to receive deliveries for',
        static=True)
    remote_send_uri = ConfigText(
        'URI of the remote ParlayX SendSmsService', static=True)
    remote_notification_uri = ConfigText(
        'URI of the remote ParlayX SmsNotificationService', static=True)


class ParlayXTransport(Transport):
    CONFIG_CLASS = ParlayXTransportConfig
    transport_type = 'sms'

    def _create_client(self, config):
        return ParlayXClient(
            short_code=config.short_code,
            endpoint=config.notification_endpoint_uri,
            send_uri=config.remote_send_uri,
            notification_uri=config.remote_notification_uri)

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        log.info('Starting ParlayX transport: %s' % (self.transport_name,))
        self.web_resource = yield self.start_web_resources(
            [(SmsNotificationService(self.handle_raw_inbound_message,
                                     self.publish_delivery_report),
              config.web_notification_path)],
            config.web_notification_port)
        self._parlayx_client = self._create_client(config)
        yield self._parlayx_client.start_sms_notification()

    def teardown_transport(self):
        log.info('Stopping ParlayX transport: %s' % (self.transport_name,))
        d = self.web_resource.loseConnection()
        d.addBoth(lambda ignored: self._parlayx_client.stop_sms_notification())
        return d

    def handle_outbound_message(self, message):
        """
        Send a text message via the ParlayX client.
        """
        log.info('Sending SMS via ParlayX: %r' % (message.to_json(),))
        d = self._parlayx_client.send_sms(
            message['to_addr'],
            message['content'],
            message['message_id'])
        d.addErrback(self.handle_outbound_message_failure, message)
        d.addCallback(
            lambda requestIdentifier: self.publish_ack(
                message['message_id'], requestIdentifier))
        return d

    @inlineCallbacks
    def handle_outbound_message_failure(self, f, message):
        """
        Handle outbound message failures.

        `ServiceException`, `PolicyException` and client-class SOAP faults
        result in `PermanentFailure` being raised; server-class SOAP faults
        instances result in `TemporaryFailure` being raised; and other failures
        are passed through.
        """
        log.error(f, 'Sending SMS failure on ParlayX: %r' % (
            self.transport_name,))

        if not f.check(ServiceException, PolicyException):
            if f.check(SoapFault):
                # We'll give server-class unknown SOAP faults the benefit of
                # the doubt as far as temporary failures go.
                if f.value.code.endswith('Server'):
                    raise TemporaryFailure(f)

        yield self.publish_nack(message['message_id'], f.getErrorMessage())
        if f.check(SoapFault):
            # We've ruled out unknown SOAP faults, so this must be a permanent
            # failure.
            raise PermanentFailure(f)
        returnValue(f)

    def handle_raw_inbound_message(self, message_id, inbound_message):
        """
        Handle incoming text messages from `SmsNotificationService` callbacks.
        """
        log.info('Receiving SMS via ParlayX: %r: %r' % (
            message_id, inbound_message,))
        return self.publish_message(
            message_id=message_id,
            content=inbound_message.message,
            to_addr=inbound_message.service_activation_number,
            from_addr=inbound_message.sender_address,
            provider='parlayx',
            transport_type=self.transport_type)
