from django.conf.urls.defaults import patterns
from piston.resource import Resource
from piston.authentication import HttpBasicAuthentication
from vumi.webapp.api.gateways.opera import handlers

ad = {'authentication': HttpBasicAuthentication(realm="Vumi")}

sms_receipt_resource = Resource(handler=handlers.SMSReceiptHandler, **ad)
sms_receive_resource = Resource(handler=handlers.ReceiveSMSHandler, **ad)

urlpatterns = patterns('',
    (r'^receipt\.json$', sms_receipt_resource, {}, 'sms-receipt'),
    (r'^receive\.xml$', sms_receive_resource, {}, 'sms-receive'),
)
