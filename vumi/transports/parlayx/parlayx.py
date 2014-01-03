# -*- test-case-name: vumi.transports.parlayx.tests.test_parlayx -*-
import uuid

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi import log
from vumi.config import ConfigText, ConfigInt, ConfigBool
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
    start_notifications = ConfigBool(
        'Start (and stop) the ParlayX notification service?', static=True)
    service_provider_service_id = ConfigText(
        'Provisioned service provider service identifier', static=True)
    service_provider_id = ConfigText(
        'Provisioned service provider identifier/username', static=True)
    service_provider_password = ConfigText(
        'Provisioned service provider password', static=True)


class ParlayXTransport(Transport):
    """ParlayX SMS transport.

    ParlayX is a defunkt standard web service API for telephone networks.
    See http://en.wikipedia.org/wiki/Parlay_X for an overview.

    .. warning::

       This transport has not been tested against another ParlayX
       implementation. If you use it, please provide feedback to the
       Vumi development team on your experiences.
    """

    CONFIG_CLASS = ParlayXTransportConfig
    transport_type = 'sms'

    def _create_client(self, config):
        """
        Create a `ParlayXClient` instance.
        """
        return ParlayXClient(
            service_provider_service_id=config.service_provider_service_id,
            service_provider_id=config.service_provider_id,
            service_provider_password=config.service_provider_password,
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
        if config.start_notifications:
            yield self._parlayx_client.start_sms_notification()

    @inlineCallbacks
    def teardown_transport(self):
        config = self.get_static_config()
        log.info('Stopping ParlayX transport: %s' % (self.transport_name,))
        yield self.web_resource.loseConnection()
        if config.start_notifications:
            yield self._parlayx_client.stop_sms_notification()

    def handle_outbound_message(self, message):
        """
        Send a text message via the ParlayX client.
        """
        log.info('Sending SMS via ParlayX: %r' % (message.to_json(),))
        transport_metadata = message.get('transport_metadata', {})
        d = self._parlayx_client.send_sms(
            message['to_addr'],
            message['content'],
            unique_correlator(message['message_id']),
            transport_metadata.get('linkid'))
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

    def handle_raw_inbound_message(self, correlator, linkid, inbound_message):
        """
        Handle incoming text messages from `SmsNotificationService` callbacks.
        """
        log.info('Receiving SMS via ParlayX: %r: %r' % (
            correlator, inbound_message,))
        message_id = extract_message_id(correlator)
        return self.publish_message(
            message_id=message_id,
            content=inbound_message.message,
            to_addr=inbound_message.service_activation_number,
            from_addr=inbound_message.sender_address,
            provider='parlayx',
            transport_type=self.transport_type,
            transport_metadata=dict(linkid=linkid))


def unique_correlator(message_id, _uuid=None):
    """
    Construct a unique message identifier from an existing message
    identifier.

    This is necessary for the cases where a ``TransportMessage`` needs to
    be transmitted, since ParlayX wants unique identifiers for all sent
    messages.
    """
    if _uuid is None:
        _uuid = uuid.uuid4()
    return '%s:%s' % (message_id, _uuid)


def extract_message_id(correlator):
    """
    Extract the Vumi message identifier from a ParlayX correlator.
    """
    return correlator.split(':', 1)[0]
