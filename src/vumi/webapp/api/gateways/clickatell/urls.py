from django.conf.urls.defaults import *
from piston.resource import Resource
from piston.authentication import HttpBasicAuthentication
from vumi.webapp.api.gateways.clickatell import handlers

ad = {'authentication': HttpBasicAuthentication(realm="Vumi")}

sms_receipt_resource = Resource(handler=handlers.SMSReceiptHandler, **ad)
sms_send_resource = Resource(handler=handlers.SendSMSHandler, **ad)
sms_template_send_resource = Resource(handler=handlers.SendTemplateSMSHandler, **ad)
sms_receive_resource = Resource(handler=handlers.ReceiveSMSHandler, **ad)

urlpatterns = patterns('',
    (r'^receipt\.json$', sms_receipt_resource, {}, 'sms-receipt'),
    (r'^send\.json$', sms_send_resource, {}, 'sms-send'),
    (r'^status\.json$', sms_send_resource, {}, 'sms-status-list'),
    (r'^status/(?P<sms_id>\d+)\.json$', sms_send_resource, {}, 'sms-status'),
    (r'^template_send\.json$', sms_template_send_resource, {}, 'sms-template-send'),
    (r'^receive\.json$', sms_receive_resource, {}, 'sms-receive'),
)

