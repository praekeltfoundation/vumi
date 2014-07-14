# -*- test-case-name: vumi.transports.parlayx.tests.test_client -*-
import hashlib
import uuid
from collections import namedtuple
from datetime import datetime

from vumi.transports.parlayx.soaputil import perform_soap_request, SoapFault
from vumi.transports.parlayx.xmlutil import (
    gettext, gettextall, Namespace, LocalNamespace as L)


PARLAYX_COMMON_NS = Namespace(
    'http://www.csapi.org/schema/parlayx/common/v2_1', 'parlayx_common')
SEND_NS = Namespace(
    'http://www.csapi.org/schema/parlayx/sms/send/v2_2/local', 'send')
NOTIFICATION_MANAGER_NS = Namespace(
    'http://www.csapi.org/schema/parlayx/sms/notification_manager/v2_3/local',
    'nm')
PARLAYX_HEAD_NS = Namespace(
    'http://www.huawei.com.cn/schema/common/v2_1', 'parlayx_head')


def format_address(msisdn):
    """
    Format a normalized MSISDN as a URI that ParlayX will accept.
    """
    if not msisdn.startswith('+'):
        raise ValueError('Only international format addresses are supported')
    return 'tel:' + msisdn[1:]


def format_timestamp(when):
    """
    Format a `datetime` instance timestamp according to ParlayX
    requirements.
    """
    return when.strftime('%Y%m%d%H%M%S')


def make_password(service_provider_id, service_provider_password,
                   timestamp):
    """
    Build a time-sensitive password for a request.
    """
    return hashlib.md5(
        service_provider_id +
        service_provider_password +
        timestamp).hexdigest()


class _ParlayXFaultDetail(namedtuple('_ParlayXFaultDetail',
                                   ['message_id', 'text', 'variables'])):
    """
    Generic ParlayX SOAP fault detail.
    """
    tag = None

    @classmethod
    def from_element(cls, element):
        if element.tag != cls.tag.text:
            return None
        return cls(
            message_id=gettext(element, 'messageId'),
            text=gettext(element, 'text'),
            variables=list(gettextall(element, 'variables')))


class ServiceExceptionDetail(_ParlayXFaultDetail):
    """
    ParlayX service exception detail.
    """
    tag = PARLAYX_COMMON_NS.ServiceExceptionDetail


class ServiceException(SoapFault):
    """
    ParlayX service exception.
    """
    detail_type = ServiceExceptionDetail


class PolicyExceptionDetail(_ParlayXFaultDetail):
    """
    ParlayX policy exception detail.
    """
    tag = PARLAYX_COMMON_NS.PolicyExceptionDetail


class PolicyException(SoapFault):
    """
    ParlayX policy exception.
    """
    detail_type = PolicyExceptionDetail


class ParlayXClient(object):
    """
    ParlayX SOAP client.

    :ivar _service_correlator:
        A unique identifier for this service, used when registering and
        deregistering for SMS notifications.
    """
    def __init__(self, service_provider_service_id, service_provider_id,
                 service_provider_password, short_code, endpoint, send_uri,
                 notification_uri, perform_soap_request=perform_soap_request):
        """
        :param service_provider_service_id:
            Provisioned service provider service identifier.
        :param service_provider_id:
            Provisioned service provider identifier/username.
        :param service_provider_password:
            Provisioned service provider password.
        :param short_code:
            SMS shortcode or service activation number.
        :param endpoint:
            URI to which the remote ParlayX service will deliver notification
            messages.
        :param send_uri:
            URI for the ParlayX ``SendSmsService`` SOAP endpoint.
        :param notification_uri:
            URI for the ParlayX ``SmsNotificationService`` SOAP endpoint.
        """
        self.service_provider_service_id = service_provider_service_id
        self.service_provider_id = service_provider_id
        self.service_provider_password = service_provider_password
        self.short_code = short_code
        self.endpoint = endpoint
        self.send_uri = send_uri
        self.notification_uri = notification_uri
        self.perform_soap_request = perform_soap_request
        self._service_correlator = uuid.uuid4().hex

    def _now(self):
        """
        The current date and time.
        """
        return datetime.now()

    def _make_header(self, service_subscription_address=None, linkid=None):
        """
        Create a ``RequestSOAPHeader`` element.

        :param service_subscription_address:
            Service subscription address for the ``OA`` header field, this
            field is omitted if its value is ``None``.
        """
        NS = PARLAYX_HEAD_NS
        other = []
        timestamp = format_timestamp(self._now())
        if service_subscription_address is not None:
            other.append(NS.OA(format_address(service_subscription_address)))
        if linkid is not None:
            other.append(NS.linkid(linkid))
        return NS.RequestSOAPHeader(
            NS.spId(self.service_provider_id),
            NS.spPassword(
                make_password(
                    self.service_provider_id,
                    self.service_provider_password,
                    timestamp)),
            NS.serviceId(self.service_provider_service_id),
            NS.timeStamp(timestamp),
            *other)

    def start_sms_notification(self):
        """
        Register a notification delivery endpoint with the remote ParlayX
        service.
        """
        body = NOTIFICATION_MANAGER_NS.startSmsNotification(
            NOTIFICATION_MANAGER_NS.reference(
                L.endpoint(self.endpoint),
                L.interfaceName('notifySmsReception'),
                L.correlator(self._service_correlator)),
            NOTIFICATION_MANAGER_NS.smsServiceActivationNumber(
                self.short_code))
        header = self._make_header()
        return self.perform_soap_request(
            uri=self.notification_uri,
            action='',
            body=body,
            header=header,
            expected_faults=[ServiceException])

    def stop_sms_notification(self):
        """
        Deregister notification delivery with the remote ParlayX service.
        """
        body = NOTIFICATION_MANAGER_NS.stopSmsNotification(
            L.correlator(self._service_correlator))
        header = self._make_header()
        return self.perform_soap_request(
            uri=self.notification_uri,
            action='',
            body=body,
            header=header,
            expected_faults=[ServiceException])

    def send_sms(self, to_addr, content, message_id, linkid=None):
        """
        Send an SMS.
        """
        def _extractRequestIdentifier((body, header)):
            return gettext(body, './/' + str(SEND_NS.result), default='')

        body = SEND_NS.sendSms(
            SEND_NS.addresses(format_address(to_addr)),
            SEND_NS.message(content),
            SEND_NS.receiptRequest(
                L.endpoint(self.endpoint),
                L.interfaceName(u'SmsNotification'),
                L.correlator(message_id)))
        header = self._make_header(
            service_subscription_address=to_addr,
            linkid=linkid)
        d = self.perform_soap_request(
            uri=self.send_uri,
            action='',
            body=body,
            header=header,
            expected_faults=[PolicyException, ServiceException])
        d.addCallback(_extractRequestIdentifier)
        return d


__all__ = [
    'format_address', 'ServiceExceptionDetail', 'ServiceException',
    'PolicyExceptionDetail', 'PolicyException', 'ParlayXClient']
