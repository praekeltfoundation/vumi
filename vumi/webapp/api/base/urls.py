from django.conf.urls.defaults import patterns
from piston.resource import Resource
from piston.authentication import HttpBasicAuthentication
from vumi.webapp.api.base import handlers

ad = {'authentication': HttpBasicAuthentication(realm="Vumi")}

sms_send_resource = Resource(handler=handlers.SendSMSHandler, **ad)
sms_template_send_resource = Resource(
    handler=handlers.SendTemplateSMSHandler, **ad)

urlpatterns = patterns('',
    (r'^send\.json$', sms_send_resource, {}, 'sms-send'),
    (r'^status\.json$', sms_send_resource, {}, 'sms-status-list'),
    (r'^status/(?P<sms_id>\d+)\.json$', sms_send_resource, {}, 'sms-status'),
    (r'^template_send\.json$', sms_template_send_resource, {},
     'sms-template-send'),
)
