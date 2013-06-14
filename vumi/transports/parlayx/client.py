# -*- test-case-name: vumi.transports.parlayx.tests.test_client -*-
import uuid
from collections import namedtuple

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



def format_address(msisdn):
    """
    Format a normalized MSISDN as a URI that ParlayX will accept.
    """
    if not msisdn.startswith('+'):
        raise ValueError('Only international format addresses are supported')
    return 'tel:' + msisdn[1:]



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
    def __init__(self, short_code, endpoint, send_uri, notification_uri,
                 perform_soap_request=perform_soap_request):
        """
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
        self.short_code = short_code
        self.endpoint = endpoint
        self.send_uri = send_uri
        self.notification_uri = notification_uri
        self.perform_soap_request = perform_soap_request
        self._service_correlator = uuid.uuid4().hex


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
        return self.perform_soap_request(
            uri=self.notification_uri,
            action='',
            body=body,
            expected_faults=[ServiceException])


    def stop_sms_notification(self):
        """
        Deregister notification delivery with the remote ParlayX service.
        """
        body = NOTIFICATION_MANAGER_NS.stopSmsNotification(
            L.correlator(self._service_correlator))
        return self.perform_soap_request(
            uri=self.notification_uri,
            action='',
            body=body,
            expected_faults=[ServiceException])


    def send_sms(self, to_addr, content, message_id):
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
        d = self.perform_soap_request(
            uri=self.send_uri,
            action='',
            body=body,
            expected_faults=[PolicyException, ServiceException])
        d.addCallback(_extractRequestIdentifier)
        return d



__all__ = [
    'format_address', 'ServiceExceptionDetail', 'ServiceException',
    'PolicyExceptionDetail', 'PolicyException', 'ParlayXClient']
